using System;
using System.Collections.Generic;

namespace Confluent.SchemaRegistry.Encryption.Gcp
{
    public class GcpKmsDriver : IKmsDriver
    {
        public static void Register()
        {
            KmsRegistry.RegisterKmsDriver(new GcpKmsDriver());
        }
    
        public static readonly string Prefix = "gcp-kms://";
        public static readonly string AccountType = "account.type";
        public static readonly string ClientId = "client.id";
        public static readonly string ClientEmail = "client.email";
        public static readonly string PrivateKeyId = "private.key.id";
        public static readonly string PrivateKey = "private.key";
        
        public string GetKeyUrlPrefix()
        {
            return Prefix;
        }

        public IKmsClient NewKmsClient(IDictionary<string, string> config, string keyUrl)
        {
            // TODO env vars
                // TODO params
            if (config.TryGetValue(AccountType, out string accountType) 
                && config.TryGetValue(ClientId, out string clientId)
                && config.TryGetValue(ClientEmail, out string clientEmail)
                && config.TryGetValue(PrivateKeyId, out string privateKeyId)
                && config.TryGetValue(PrivateKey, out string privateKey))
            {
                return new GcpKmsClient(keyUrl);
            }

            throw new ArgumentException("Cannot load credentials");
        }
    }
}