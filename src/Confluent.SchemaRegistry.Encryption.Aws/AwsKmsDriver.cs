using System;
using System.Collections.Generic;

namespace Confluent.SchemaRegistry.Encryption.Aws
{
    public class AwsKmsDriver : IKmsDriver
    {
        public static void Register()
        {
            KmsRegistry.RegisterKmsDriver(new AwsKmsDriver());
        }

        public static readonly string Prefix = "aws-kms://";
        public static readonly string AccessKeyId = "access.key.id";
        public static readonly string SecretAccessKey = "secret.access.key";
        
        public string GetKeyUrlPrefix()
        {
            return Prefix;
        }

        public IKmsClient NewKmsClient(IDictionary<string, string> config, string keyUrl)
        {
            // TODO env vars
            if (config.TryGetValue(AccessKeyId, out string accessKeyId) 
                && config.TryGetValue(SecretAccessKey, out string secretAccessKey))
            {
                return new AwsKmsClient(keyUrl, accessKeyId, secretAccessKey);
            }
            
            throw new ArgumentException("Cannot load credentials");
        }
    }
}