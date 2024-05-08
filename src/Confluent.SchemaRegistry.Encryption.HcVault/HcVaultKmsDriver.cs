using System;
using System.Collections.Generic;

namespace Confluent.SchemaRegistry.Encryption.HcVault
{
    public class HcVaultKmsDriver : IKmsDriver
    {
        public static void Register()
        {
            KmsRegistry.RegisterKmsDriver(new HcVaultKmsDriver());
        }
    
        public static readonly string Prefix = "hcvault://";
        public static readonly string TokenId = "token.id";
        public static readonly string Namespace = "namespace";
        
        public string GetKeyUrlPrefix()
        {
            return Prefix;
        }

        public IKmsClient NewKmsClient(IDictionary<string, string> config, string keyUrl)
        {
            // TODO env vars
            // TODO namespace
            if (config.TryGetValue(TokenId, out string tokenId)) 
            {
                return new HcVaultKmsClient(keyUrl, tokenId);
            }

            throw new ArgumentException("Cannot load credentials");
        }
    }
}