﻿using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;

namespace Confluent.SchemaRegistry.Encryption
{
    
    public class LocalKmsDriver : IKmsDriver
    {
        [ModuleInitializer]
        internal static void Register()
        {
            KmsRegistry.RegisterKmsDriver(new LocalKmsDriver());
        }
    
        public static readonly string Prefix = "local-kms://";
        public static readonly string Secret = "secret";
        
        public string GetKeyUrlPrefix()
        {
            return Prefix;
        }

        public IKmsClient NewKmsClient(IDictionary<string, string> config, string keyUrl)
        {
            // TODO env vars
            if (config.TryGetValue(Secret, out string secret))
            {
                return new LocalKmsClient(secret);
            }
            
            throw new ArgumentException("Cannot load credentials");
        }
    }
}