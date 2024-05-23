// Copyright 2020 Confluent Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// Refer to LICENSE for more information.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;


namespace Confluent.SchemaRegistry
{
    public abstract class AsyncDeserializer<T, TParsedSchema> : IAsyncDeserializer<T>
    {
        protected bool useLatestVersion = false;
        protected IDictionary<string, string> useLatestWithMetadata = null;
        protected SubjectNameStrategyDelegate subjectNameStrategy = null;
        
        protected readonly int headerSize =  sizeof(int) + sizeof(byte);
        
        protected readonly IDictionary<int, (Schema, TParsedSchema)> schemaCache = new Dictionary<int, (Schema, TParsedSchema)>();
        
        protected SemaphoreSlim deserializeMutex = new SemaphoreSlim(1);

        protected ISchemaRegistryClient schemaRegistryClient;
        protected IList<IRuleExecutor> ruleExecutors;
        
        protected AsyncDeserializer(ISchemaRegistryClient schemaRegistryClient, SerdeConfig config, 
            IList<IRuleExecutor> ruleExecutors = null)
        {
            this.schemaRegistryClient = schemaRegistryClient;
            this.ruleExecutors = ruleExecutors ?? new List<IRuleExecutor>();

            if (config == null) { return; }

            foreach (IRuleExecutor executor in this.ruleExecutors.Concat(RuleRegistry.GetRuleExecutors()))
            {
                IEnumerable<KeyValuePair<string, string>> ruleConfigs = config
                    .Select(kv => new KeyValuePair<string, string>(
                        kv.Key.StartsWith("rules.") ? kv.Key.Substring("rules.".Length) : kv.Key, kv.Value));
                executor.Configure(ruleConfigs); 
            }
        }
        public abstract Task<T> DeserializeAsync(ReadOnlyMemory<byte> data, bool isNull, SerializationContext context);

        protected abstract Task<TParsedSchema> ParseSchema(Schema schema);

        protected async Task<(Schema, TParsedSchema)> GetSchema(int writerId)
        {
            Schema writerSchema;
            TParsedSchema parsedSchema;
            await deserializeMutex.WaitAsync().ConfigureAwait(continueOnCapturedContext: false);
            try
            {
                if (schemaCache.TryGetValue(writerId, out var tuple))
                {
                    (writerSchema, parsedSchema) = tuple;
                }
                else
                {
                    if (schemaCache.Count > schemaRegistryClient.MaxCachedSchemas)
                    {
                        schemaCache.Clear();
                    }

                    writerSchema = await schemaRegistryClient.GetSchemaAsync(writerId).ConfigureAwait(continueOnCapturedContext: false);
                    parsedSchema = await ParseSchema(writerSchema).ConfigureAwait(continueOnCapturedContext: false);
                    schemaCache[writerId] = (writerSchema, parsedSchema);
                }
            }
            finally
            {
                deserializeMutex.Release();
            }

            return (writerSchema, parsedSchema);
        }
    }
}
