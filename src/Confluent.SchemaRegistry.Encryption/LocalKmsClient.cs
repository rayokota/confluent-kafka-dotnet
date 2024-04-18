using HkdfStandard;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;

namespace Confluent.SchemaRegistry.Encryption
{
    public class LocalKmsClient : IKmsClient
    {
        public static readonly string Prefix = "local-kms://";
        
        public string Secret { get; }
        private Cryptor cryptor;
        private byte[] key;

        public LocalKmsClient(string secret)
        {
            Secret = secret;
            // TODO RULES fix gcm in tests
            cryptor = new Cryptor(DekFormat.AES256_GCM);
            key = Hkdf.DeriveKey(HashAlgorithmName.SHA256, Encoding.UTF8.GetBytes(secret), cryptor.KeySize());
        }
        
        public Task<byte[]> Encrypt(byte[] plaintext)
        {
            return Task.FromResult(cryptor.Encrypt(key, plaintext));
        }

        public Task<byte[]> Decrypt(byte[] ciphertext)
        {
            return Task.FromResult(cryptor.Decrypt(key, ciphertext));
        }
    }
}