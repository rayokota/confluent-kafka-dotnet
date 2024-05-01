using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;

namespace Confluent.SchemaRegistry.Encryption.Azure
{
    public class AzureKmsDriver : IKmsDriver
    {
        [ModuleInitializer]
        internal static void Register()
        {
            KmsRegistry.RegisterKmsDriver(new AzureKmsDriver());
        }
    
        public static readonly string Prefix = "azure-kms://";
        public static readonly string TenantId = "tenant.id";
        public static readonly string ClientId = "client.id";
        public static readonly string ClientSecret = "client.secret";
        
        public string GetKeyUrlPrefix()
        {
            return Prefix;
        }

        public IKmsClient NewKmsClient(IDictionary<string, string> config, string keyUrl)
        {
            // TODO env vars
            if (config.TryGetValue(TenantId, out string tenantId) 
                && config.TryGetValue(ClientId, out string clientId)
                && config.TryGetValue(ClientSecret, out string clientSecret))
            {
                return new AzureKmsClient(keyUrl, tenantId, clientId, clientSecret);
            }

            throw new ArgumentException("Cannot load credentials");
        }
    }
}