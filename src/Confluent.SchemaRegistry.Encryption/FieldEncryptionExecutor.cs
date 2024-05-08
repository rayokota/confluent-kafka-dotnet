using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;

namespace Confluent.SchemaRegistry.Encryption
{
    public class FieldEncryptionExecutor : FieldRuleExecutor
    {
        public static void Register()
        {
            Register(null);
        }

        public static void Register(IDekRegistryClient client)
        {
            RuleRegistry.RegisterRuleExecutor(RuleType, new FieldEncryptionExecutor(client));
        }

        public static readonly string RuleType = "ENCRYPT";
        
        public static readonly string EncryptKekName = "encrypt.kek.name";
        public static readonly string EncryptKmsKeyid = "encrypt.kms.key.id";
        public static readonly string EncryptKmsType = "encrypt.kms.type";
        public static readonly string EncryptDekAlgorithm = "encrypt.dek.algorithm";
        public static readonly string EncryptDekExpiryDays = "encrypt.dek.expiry.days";
        
        public static readonly string KmsTypeSuffix = "://";

        internal static readonly int LatestVersion = 1;
        internal static readonly byte MagicByte = 0x0;
        internal static readonly int MillisInDay = 24 * 60 * 60 * 1000;
        internal static readonly int VersionSize = 4;

        internal IDictionary<DekFormat, Cryptor> Cryptors;
        internal IEnumerable<KeyValuePair<string, string>> Configs;
        internal IDekRegistryClient Client;

        public FieldEncryptionExecutor()
        {
        }
        
        public FieldEncryptionExecutor(IDekRegistryClient client)
        {
            Client = client;
        }
        
        public override void Configure(IEnumerable<KeyValuePair<string, string>> config)
        {
            Configs = config;
            if (Client == null)
            {
                Client = new CachedDekRegistryClient(Configs);
            }
            Cryptors = new Dictionary<DekFormat, Cryptor>();
        }

        public override string Type() => RuleType;

        protected override IFieldTransform newTransform(RuleContext ctx)
        {
            FieldEncryptionExecutorTransform transform = new FieldEncryptionExecutorTransform(this);
            transform.Init(ctx);
            return transform;
        }

        internal Cryptor GetCryptor(RuleContext ctx)
        {
            string algorithm = ctx.GetParameter(EncryptDekAlgorithm);
            if (!Enum.TryParse<DekFormat>(algorithm, out DekFormat dekFormat))
            {
                dekFormat = DekFormat.AES256_GCM;
            }
            return GetCryptor(dekFormat);
        }

        private Cryptor GetCryptor(DekFormat dekFormat)
        {
            if (Cryptors.TryGetValue(dekFormat, out Cryptor value))
            {
                return value;
            }

            Cryptor cryptor = new Cryptor(dekFormat);
            Cryptors.Add(dekFormat, cryptor);
            return cryptor;
        }

        internal static byte[] ToBytes(RuleContext.Type type, object obj)
        {
            switch (type)
            {
                case RuleContext.Type.Bytes:
                    return (byte[])obj;
                case RuleContext.Type.String:
                    return Encoding.UTF8.GetBytes(obj.ToString());
                default:
                    return null;
            }
        }

        internal static object ToObject(RuleContext.Type type, byte[] bytes)
        {
            switch (type)
            {
                case RuleContext.Type.Bytes:
                    return bytes;
                case RuleContext.Type.String:
                    return Encoding.UTF8.GetString(bytes);
                default:
                    return null;
            }
        }
        
        public override void Dispose()
        {
            if (Client != null)
            {
                Client.Dispose();
            }
        }
    }

    public class FieldEncryptionExecutorTransform : IFieldTransform
    {
        
        public FieldEncryptionExecutor Executor { get; private set; }
        public Cryptor Cryptor { get; private set; }
        public string KekName { get; private set; }
        public RegisteredKek Kek { get; private set; }
        public int DekExpiryDays { get; private set; }

        public FieldEncryptionExecutorTransform(FieldEncryptionExecutor executor)
        {
            Executor = executor;
        }
        
        public void Init(RuleContext ctx)
        {
            Cryptor = Executor.GetCryptor(ctx);
            KekName = GetKekName(ctx);
            Kek = GetOrCreateKek(ctx);
            DekExpiryDays = GetDekExpiryDays(ctx);
        }

        public bool IsDekRotated() => DekExpiryDays > 0;

        private string GetKekName(RuleContext ctx)
        {
            string name = ctx.GetParameter(FieldEncryptionExecutor.EncryptKekName);
            if (String.IsNullOrEmpty(name))
            {
                throw new RuleException("No kek name found");
            }

            return name;
        }
        
        private RegisteredKek GetOrCreateKek(RuleContext ctx)
        {
            bool isRead = ctx.RuleMode == RuleMode.Read;
            KekId kekId = new KekId(KekName, isRead);

            string kmsType = ctx.GetParameter(FieldEncryptionExecutor.EncryptKmsType);
            string kmsKeyId = ctx.GetParameter(FieldEncryptionExecutor.EncryptKmsKeyid);

            RegisteredKek kek = RetrieveKekFromRegistry(kekId);
            if (kek == null)
            {
                if (isRead)
                {
                    throw new RuleException($"No kek found for name {KekName} during consume");
                }
                if (String.IsNullOrEmpty(kmsType))
                {
                    throw new RuleException($"No kms type found for {KekName} during produce");
                }
                if (String.IsNullOrEmpty(kmsKeyId))
                {
                    throw new RuleException($"No kms key id found for {KekName} during produce");
                }

                kek = StoreKekToRegistry(kekId, kmsType, kmsKeyId, false);
                if (kek == null)
                {
                    // Handle conflicts (409)
                    kek = RetrieveKekFromRegistry(kekId);
                }

                if (kek == null)
                {
                    throw new RuleException($"No kek found for {KekName} during produce");
                }
            }
            if (!String.IsNullOrEmpty(kmsType) && !kmsType.Equals(kek.KmsType))
            {
                throw new RuleException($"Found {KekName} with kms type {kek.KmsType} but expected {kmsType}");
            }
            if (!String.IsNullOrEmpty(kmsKeyId) && !kmsKeyId.Equals(kek.KmsKeyId))
            {
                throw new RuleException($"Found {KekName} with kms key id {kek.KmsKeyId} but expected {kmsKeyId}");
            }

            return kek;
        }
        
        private int GetDekExpiryDays(RuleContext ctx)
        {
            string expiryDays = ctx.GetParameter(FieldEncryptionExecutor.EncryptDekExpiryDays);
            if (String.IsNullOrEmpty(expiryDays))
            {
                return 0;
            }
            if (!Int32.TryParse(expiryDays, out int days))
            {
                throw new RuleException($"Invalid expiry days {expiryDays}");
            }
            if (days < 0)
            {
                throw new RuleException($"Invalid expiry days {expiryDays}");
            }
            return days;
        }
        
        private RegisteredKek RetrieveKekFromRegistry(KekId key)
        {
            try
            {
                return Executor.Client.GetKekAsync(key.Name, !key.LookupDeletedKeks)
                    .ConfigureAwait(continueOnCapturedContext: false)
                    .GetAwaiter()
                    .GetResult();
            }
            catch (SchemaRegistryException e)
            {
                if (e.Status == HttpStatusCode.NotFound)
                {
                    return null;
                }

                throw new RuleException($"Failed to retrieve kek {key.Name}", e);
            }
        }
        
        private RegisteredKek StoreKekToRegistry(KekId key, string kmsType, string kmsKeyId, bool shared)
        {
            Kek kek = new Kek
            {
                Name = key.Name,
                KmsType = kmsType,
                KmsKeyId = kmsKeyId,
                Shared = shared
            };
            try
            {
                return Executor.Client.CreateKekAsync(kek)
                    .ConfigureAwait(continueOnCapturedContext: false)
                    .GetAwaiter()
                    .GetResult();
            }
            catch (SchemaRegistryException e)
            {
                if (e.Status == HttpStatusCode.Conflict)
                {
                    return null;
                }

                throw new RuleException($"Failed to create kek {key.Name}", e);
            }
        }
        
        private RegisteredDek GetOrCreateDek(RuleContext ctx, int? version)
        {
            bool isRead = ctx.RuleMode == RuleMode.Read;
            DekId dekId = new DekId(KekName, ctx.Subject, version, Cryptor.DekFormat, isRead);

            IKmsClient kmsClient = null;
            RegisteredDek dek = RetrieveDekFromRegistry(dekId);
            bool isExpired = IsExpired(ctx, dek);
            if (dek == null || isExpired)
            {
                if (isRead)
                {
                    throw new RuleException($"No dek found for {KekName} during consume");
                }

                byte[] encryptedDek = null;
                if (!Kek.Shared)
                {
                    kmsClient = GetKmsClient(Executor.Configs, Kek);
                    // Generate new dek
                    byte[] rawDek = Cryptor.GenerateKey();
                    encryptedDek = kmsClient.Encrypt(rawDek)
                        .ConfigureAwait(continueOnCapturedContext: false)
                        .GetAwaiter()
                        .GetResult();
                }

                int? newVersion = isExpired ? dek.Version : null;
                DekId newDekId = new DekId(KekName, ctx.Subject, newVersion, Cryptor.DekFormat, isRead);
                dek = StoreDekToRegistry(newDekId, encryptedDek);
                if (dek == null)
                {
                    // Handle conflicts (409)
                    dek = RetrieveDekFromRegistry(dekId);
                }

                if (dek == null)
                {
                    throw new RuleException($"No dek found for {KekName} during produce");
                }
            }

            if (dek.KeyMaterialBytes == null)
            {
                if (kmsClient == null)
                {
                    kmsClient = GetKmsClient(Executor.Configs, Kek);
                }

                byte[] rawDek = kmsClient.Decrypt(dek.EncryptedKeyMaterialBytes)
                    .ConfigureAwait(continueOnCapturedContext: false)
                    .GetAwaiter()
                    .GetResult();
                dek.SetKeyMaterial(rawDek);

            }

            return dek;
        }

        private bool IsExpired(RuleContext ctx, RegisteredDek dek)
        {
            return ctx.RuleMode != RuleMode.Read
                && DekExpiryDays > 0
                && dek != null
                && (DateTimeOffset.Now.ToUnixTimeMilliseconds() - dek.Timestamp) / FieldEncryptionExecutor.MillisInDay > DekExpiryDays;
        }
        
        private RegisteredDek RetrieveDekFromRegistry(DekId key)
        {
            try
            {
                RegisteredDek dek;
                if (key.Version != null)
                {
                    dek = Executor.Client.GetDekVersionAsync(key.KekName, key.Subject, key.Version.Value, key.DekFormat, !key.LookupDeletedDeks)
                        .ConfigureAwait(continueOnCapturedContext: false)
                        .GetAwaiter()
                        .GetResult();
                    
                }
                else
                {
                    dek = Executor.Client.GetDekAsync(key.KekName, key.Subject, key.DekFormat, !key.LookupDeletedDeks)
                        .ConfigureAwait(continueOnCapturedContext: false)
                        .GetAwaiter()
                        .GetResult();
                }

                return dek?.EncryptedKeyMaterial != null ? dek : null;
            }
            catch (SchemaRegistryException e)
            {
                if (e.Status == HttpStatusCode.NotFound)
                {
                    return null;
                }

                throw new RuleException($"Failed to retrieve dek for kek {key.KekName}, subject {key.Subject}", e);
            }
        }
        
        private RegisteredDek StoreDekToRegistry(DekId key, byte[] encryptedDek)
        {

            string encryptedDekStr = encryptedDek != null ? Convert.ToBase64String(encryptedDek) : null;
            Dek dek = new Dek
            {
                Subject = key.Subject,
                Version = key.Version,
                Algorithm = key.DekFormat ?? DekFormat.AES256_GCM,
                EncryptedKeyMaterial = encryptedDekStr
            };
            try
            {
                return Executor.Client.CreateDekAsync(key.KekName, dek)
                    .ConfigureAwait(continueOnCapturedContext: false)
                    .GetAwaiter()
                    .GetResult();
            }
            catch (SchemaRegistryException e)
            {
                if (e.Status == HttpStatusCode.Conflict)
                {
                    return null;
                }

                throw new RuleException($"Failed to create dek for kek {key.KekName}, subject {key.Subject}", e);
            }
        }

        public object Transform(RuleContext ctx, RuleContext.FieldContext fieldCtx, object fieldValue)
        {
            if (fieldValue == null)
            {
                return null;
            }

            RegisteredDek dek;
            byte[] plaintext;
            byte[] ciphertext;
            switch (ctx.RuleMode)
            {
                case RuleMode.Write:
                    plaintext = FieldEncryptionExecutor.ToBytes(fieldCtx.Type, fieldValue);
                    if (plaintext == null)
                    {
                        throw new RuleException($"Type {fieldCtx.Type} not supported for encryption");
                    }


                    dek = GetOrCreateDek(ctx, IsDekRotated() ? FieldEncryptionExecutor.LatestVersion : null);
                    ciphertext = Cryptor.Encrypt(dek.KeyMaterialBytes, plaintext);
                    if (IsDekRotated())
                    {
                        ciphertext = PrefixVersion(dek.Version.Value, ciphertext);
                    }

                    if (fieldCtx.Type == RuleContext.Type.String)
                    {
                        return Convert.ToBase64String(ciphertext);
                    }
                    else
                    {
                        return FieldEncryptionExecutor.ToObject(fieldCtx.Type, ciphertext);
                    }
                case RuleMode.Read:
                    if (fieldCtx.Type == RuleContext.Type.String)
                    {
                        ciphertext = Convert.FromBase64String((string)fieldValue);
                    }
                    else
                    {
                        ciphertext = FieldEncryptionExecutor.ToBytes(fieldCtx.Type, fieldValue);
                    }

                    if (ciphertext == null)
                    {
                        return fieldValue;
                    }

                    int? version = null;
                    if (IsDekRotated())
                    {
                        (int, byte[]) kv = ExtractVersion(ciphertext);
                        version = kv.Item1;
                        ciphertext = kv.Item2;
                    }

                    dek = GetOrCreateDek(ctx, version);
                    plaintext = Cryptor.Decrypt(dek.KeyMaterialBytes, ciphertext);
                    return FieldEncryptionExecutor.ToObject(fieldCtx.Type, plaintext);
                default:
                    throw new ArgumentException("Unsupported rule mode " + ctx.RuleMode);
            }
        }

        private byte[] PrefixVersion(int version, byte[] ciphertext)
        {
            byte[] buffer = new byte[1 + FieldEncryptionExecutor.VersionSize + ciphertext.Length];
            using (MemoryStream stream = new MemoryStream(buffer))
            {
                using (BinaryWriter writer = new BinaryWriter(stream))
                {
                    writer.Write(FieldEncryptionExecutor.MagicByte);
                    writer.Write(version);
                    writer.Write(ciphertext);
                    return stream.ToArray();
                }
            }
        }

        private (int, byte[]) ExtractVersion(byte[] ciphertext)
        {
            using (MemoryStream stream = new MemoryStream(ciphertext))
            {
                using (BinaryReader reader = new BinaryReader(stream))
                {
                    int remainingSize = ciphertext.Length;
                    reader.ReadByte();
                    remainingSize--;
                    int version = reader.ReadInt32();
                    remainingSize -= FieldEncryptionExecutor.VersionSize;
                    byte[] remaining = reader.ReadBytes(remainingSize);
                    return (version, remaining);
                }
            }
        }
        
        private static IKmsClient GetKmsClient(IEnumerable<KeyValuePair<string, string>> configs, RegisteredKek kek)
        {
            string keyUrl = kek.KmsType + FieldEncryptionExecutor.KmsTypeSuffix + kek.KmsKeyId;
            IKmsClient kmsClient = KmsRegistry.GetKmsClient(keyUrl);
            if (kmsClient == null)
            {
                IKmsDriver kmsDriver = KmsRegistry.GetKmsDriver(keyUrl);
                kmsClient = kmsDriver.NewKmsClient(
                    configs.ToDictionary(it => it.Key, it => it.Value), keyUrl);
                KmsRegistry.RegisterKmsClient(kmsClient);
            }

            return kmsClient;
        }

        public void Dispose()
        {
        }
    }
}