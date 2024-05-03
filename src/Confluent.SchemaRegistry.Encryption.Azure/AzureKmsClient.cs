using System;
using System.Threading.Tasks;
using Azure.Identity;
using Azure.Security.KeyVault.Keys.Cryptography;

namespace Confluent.SchemaRegistry.Encryption.Azure
{
    public class AzureKmsClient : IKmsClient
    {
        private CryptographyClient kmsClient;
        private ClientSecretCredential credential;
        private string keyId;
        
        public string KekId { get; }
        public string TenantId { get; }
        public string ClientId { get; }
        public string ClientSecret { get; }
        
        public AzureKmsClient(string kekId, string tenantId, string clientId, string clientSecret)
        {
            KekId = kekId;
            TenantId = tenantId;
            ClientId = clientId;
            ClientSecret = clientSecret;
            
            if (!kekId.StartsWith(AzureKmsDriver.Prefix)) {
              throw new ArgumentException(string.Format($"key URI must start with {AzureKmsDriver.Prefix}"));
            }
            keyId = KekId.Substring(AzureKmsDriver.Prefix.Length);
            credential = new ClientSecretCredential(tenantId, clientId, clientSecret);
        }
        
        public bool DoesSupport(string uri)
        {
            return uri.StartsWith(AzureKmsDriver.Prefix); 
        }
        
        public async Task<byte[]> Encrypt(byte[] plaintext)
        {
            var client = await GetCryptographyClientAsync();
            var result = await client.EncryptAsync(EncryptionAlgorithm.RsaOaep, plaintext);
            return result.Ciphertext;
        }

        public async Task<byte[]> Decrypt(byte[] ciphertext)
        {
            var client = await GetCryptographyClientAsync();
            var result = await client.DecryptAsync(EncryptionAlgorithm.RsaOaep, ciphertext);
            return result.Plaintext;
        }
        
        // TODO fix async
        private async Task<CryptographyClient> GetCryptographyClientAsync()
        {
            if (kmsClient == null)
            {
                kmsClient = new CryptographyClient(new Uri(keyId), credential);
            }
            return kmsClient;
        }
    }
}