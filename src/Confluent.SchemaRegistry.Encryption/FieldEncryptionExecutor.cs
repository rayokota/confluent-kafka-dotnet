using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Linq;
using System.Text;
using Microsoft.Extensions.Caching.Memory;
using System.Runtime.CompilerServices;

namespace Confluent.SchemaRegistry.Encryption
{
    public class FieldEncryptionExecutor : FieldRuleExecutor
    {
        [ModuleInitializer]
        internal static void Register()
        {
            RuleRegistry.RegisterRuleExecutor("ENCRYPT", new FieldEncryptionExecutor());
        }

        public static readonly string HeaderNamePrefix = "encrypt";

        private static readonly string EncryptMap = "encryptMap";
        private static readonly string DecryptMap = "decryptMap";

        private static byte Version = 0;
        private static int LengthVersion = 1;
        private static int LengthEncryptedDek = 4;
        private static int LengthKekId = 4;
        private static int LengthDekFormat = 4;

        protected string kekId;
        private IDictionary<DekFormat, Cryptor> cryptors;
        private int cacheExpirySecs = 300;
        private int cacheSize = 1000;
        private bool keyDeterministic = false;
        private bool valueDeterministic = false;
        private readonly IMemoryCache dekEncryptCache;
        private readonly IMemoryCache dekDecryptCache;


        public FieldEncryptionExecutor()
        {
            dekEncryptCache = new MemoryCache(new MemoryCacheOptions
            {
                ExpirationScanFrequency = TimeSpan.FromSeconds(cacheExpirySecs),
                SizeLimit = cacheSize
            });
            dekDecryptCache = new MemoryCache(new MemoryCacheOptions
            {
                SizeLimit = cacheSize
            });
            cryptors = new Dictionary<DekFormat, Cryptor>();
        }

        public override string Type() => "ENCRYPT";

        private static DekFormat GetKeyFormat(bool isDeterministic)
        {
            // TODO RULES fix gcm in tests
            return isDeterministic ? DekFormat.AES256_SIV : DekFormat.AES256_GCM;
        }

        protected override FieldTransform newTransform(RuleContext ctx)
        {
            return execute;
        }

        private object execute(RuleContext ctx, RuleContext.FieldContext fieldCtx, object obj)
        {
            byte[] plaintext;
            byte[] ciphertext;
            byte[] metadata;
            switch (ctx.RuleMode)
            {
                case RuleMode.Write:
                    plaintext = ToBytes(fieldCtx, obj);
                    if (plaintext == null)
                    {
                        return obj;
                    }

                    Cryptor cryptor = GetCryptor(ctx.IsKey);
                    Dek dek = GetDekForEncrypt(ctx, cryptor);
                    ciphertext = cryptor.Encrypt(dek.RawDek, plaintext);
                    metadata = BuildMetadata(cryptor.DekFormat.ToString(), dek.EncryptedDek);
                    string headerName = GetHeaderName(ctx);
                    if (!ctx.Headers.TryGetLastBytes(headerName, out _))
                    {
                        ctx.Headers.Add(headerName, metadata);
                    }

                    return Convert.ToBase64String(ciphertext);
                case RuleMode.Read:
                    if (!ctx.Headers.TryGetLastBytes(GetHeaderName(ctx), out metadata))
                    {
                        return obj;
                    }

                    ciphertext = Convert.FromBase64String(obj.ToString());
                    plaintext = Decrypt(ctx, metadata, ciphertext);
                    object result = ToObject(fieldCtx, plaintext);
                    return result != null ? result : obj;
                default:
                    throw new ArgumentException("Unsupported rule mode " + ctx.RuleMode);
            }
        }

        private byte[] BuildMetadata(string dekFormat, byte[] encryptedDek)
        {
            byte[] kekBytes = Encoding.UTF8.GetBytes(kekId);
            byte[] dekBytes = Encoding.UTF8.GetBytes(dekFormat);
            byte[] buffer = new byte[LengthVersion
                                     + LengthKekId + kekBytes.Length
                                     + LengthDekFormat + dekBytes.Length
                                     + LengthEncryptedDek + encryptedDek.Length];
            using (MemoryStream stream = new MemoryStream(buffer))
            {
                using (BinaryWriter writer = new BinaryWriter(stream))
                {
                    writer.Write(Version);
                    writer.Write(kekBytes.Length);
                    writer.Write(kekBytes);
                    writer.Write(dekBytes.Length);
                    writer.Write(dekBytes);
                    writer.Write(encryptedDek.Length);
                    writer.Write(encryptedDek);
                    return stream.ToArray();
                }
            }
        }

        private byte[] Decrypt(RuleContext ctx, byte[] metadata, byte[] ciphertext)
        {
            using (MemoryStream stream = new MemoryStream(metadata))
            {
                using (BinaryReader reader = new BinaryReader(stream))
                {
                    int remainingSize = metadata.Length;
                    reader.ReadByte();
                    remainingSize--;
                    int kekIdSize = reader.ReadInt32();
                    remainingSize -= LengthKekId;
                    if (kekIdSize <= 0 || kekIdSize > remainingSize)
                    {
                        throw new ArgumentException("invalid ciphertext");
                    }

                    byte[] kekId = reader.ReadBytes(kekIdSize);
                    remainingSize -= kekIdSize;
                    int dekFormatSize = reader.ReadInt32();
                    remainingSize -= LengthDekFormat;
                    if (dekFormatSize <= 0 || dekFormatSize > remainingSize)
                    {
                        throw new ArgumentException("invalid ciphertext");
                    }

                    byte[] dekFormat = reader.ReadBytes(dekFormatSize);
                    remainingSize -= dekFormatSize;
                    int encryptedDekSize = reader.ReadInt32();
                    remainingSize -= LengthDekFormat;
                    if (encryptedDekSize <= 0 || encryptedDekSize > remainingSize)
                    {
                        throw new ArgumentException("invalid ciphertext");
                    }

                    byte[] encryptedDek = reader.ReadBytes(encryptedDekSize);
                    remainingSize -= encryptedDekSize;
                    if (remainingSize != 0)
                    {
                        throw new ArgumentException("invalid ciphertext");
                    }

                    Cryptor cryptor = GetCryptor(Enum.Parse<DekFormat>(Encoding.UTF8.GetString(dekFormat)));
                    Dek dek = GetDekForDecrypt(
                        ctx, Encoding.UTF8.GetString(kekId), encryptedDek);
                    return cryptor.Decrypt(dek.RawDek, ciphertext);
                }
            }
        }

        private Dek GetDekForEncrypt(RuleContext ctx, Cryptor cryptor)
        {
            string key = cryptor.DekFormat.ToString();
            // Cache on rule context to ensure dek lives during life of entire transformation
            IDictionary<string, Dek> dict = (IDictionary<string, Dek>)
                ComputeIfAbsent(ctx.CustomData, EncryptMap, () => new Dictionary<string, Dek>());
            return ComputeIfAbsent(dict, key, () =>
            {
                if (!dekEncryptCache.TryGetValue(key, out Dek dek))
                {
                    byte[] rawDek = cryptor.GenerateKey();
                    IKmsClient kmsClient = KmsClients.Get(kekId);
                    byte[] encryptedDek = kmsClient.Encrypt(rawDek)
                        .ConfigureAwait(continueOnCapturedContext: false)
                        .GetAwaiter()
                        .GetResult();
                    dek = new Dek(rawDek, encryptedDek);
                    var options = new MemoryCacheEntryOptions().SetSize(1);
                    dekEncryptCache.Set(key, dek, options);
                }

                return dek;
            });
        }

        private Dek GetDekForDecrypt(RuleContext ctx, string kekId, byte[] encryptedDek)
        {
            ByteArrayKey key = new ByteArrayKey(encryptedDek);
            // Cache on rule context to ensure dek lives during life of entire transformation
            IDictionary<ByteArrayKey, Dek> dict = (IDictionary<ByteArrayKey, Dek>)
                ComputeIfAbsent(ctx.CustomData, DecryptMap, () => new Dictionary<ByteArrayKey, Dek>());
            return ComputeIfAbsent(dict, key, () =>
            {
                if (!dekDecryptCache.TryGetValue(key, out Dek dek))
                {
                    IKmsClient kmsClient = KmsClients.Get(kekId);
                    byte[] rawDek = kmsClient.Decrypt(encryptedDek)
                        .ConfigureAwait(continueOnCapturedContext: false)
                        .GetAwaiter()
                        .GetResult();
                    dek = new Dek(rawDek, encryptedDek);
                    var options = new MemoryCacheEntryOptions().SetSize(1);
                    dekDecryptCache.Set(key, dek, options);
                }

                return dek;
            });
        }

        public TValue ComputeIfAbsent<TKey, TValue>(IDictionary<TKey, TValue> dict, TKey key, Func<TValue> func)
        {
            if (!dict.TryGetValue(key, out var val))
            {
                val = func.Invoke();
                dict.Add(key, val);
            }

            return (TValue)val;
        }

        private Cryptor GetCryptor(bool isKey)
        {
            return GetCryptor(GetKeyFormat(isKey ? keyDeterministic : valueDeterministic));
        }

        private Cryptor GetCryptor(DekFormat dekFormat)
        {
            bool exists = cryptors.TryGetValue(dekFormat, out Cryptor value);
            if (exists)
            {
                return value;
            }

            Cryptor cryptor = new Cryptor(dekFormat);
            cryptors.Add(dekFormat, cryptor);
            return cryptor;
        }

        private static byte[] ToBytes(RuleContext.FieldContext fieldCtx, object obj)
        {
            switch (fieldCtx.Type)
            {
                case RuleContext.Type.Bytes:
                    return (byte[])obj;
                case RuleContext.Type.String:
                    return Encoding.UTF8.GetBytes(obj.ToString());
                default:
                    return null;
            }
        }

        private static object ToObject(RuleContext.FieldContext fieldCtx, byte[] bytes)
        {
            switch (fieldCtx.Type)
            {
                case RuleContext.Type.Bytes:
                    return bytes;
                case RuleContext.Type.String:
                    return Encoding.UTF8.GetString(bytes);
                default:
                    return null;
            }
        }

        private static String GetHeaderName(RuleContext ctx)
        {
            return HeaderNamePrefix + (ctx.IsKey ? "-key" : "-value");
        }

        public class ByteArrayKey
        {
            public byte[] Bytes { get; }
            private readonly int _hashCode;

            public ByteArrayKey(byte[] bytes)
            {
                Bytes = bytes;
                _hashCode = GetHashCode(bytes);
            }

            public override bool Equals(object obj)
            {
                if (obj == null)
                {
                    return false;
                }

                var other = (ByteArrayKey)obj;
                return Bytes.SequenceEqual(other.Bytes);
            }

            public override int GetHashCode()
            {
                return _hashCode;
            }

            private static int GetHashCode([NotNull] byte[] bytes)
            {
                return bytes.Sum(b => b);
            }
        }

        public class Dek
        {
            public byte[] RawDek { get; }
            public byte[] EncryptedDek { get; }

            public Dek(byte[] rawDek, byte[] encryptedDek)
            {
                RawDek = rawDek;
                EncryptedDek = encryptedDek;
            }
        }
    }
}