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
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using NJsonSchema;
using NJsonSchema.Generation;


namespace Confluent.SchemaRegistry.Serdes
{
    /// <summary>
    ///     (async) JSON deserializer.
    /// </summary>
    /// <remarks>
    ///     Serialization format:
    ///       byte 0:           A magic byte that identifies this as a message with
    ///                         Confluent Platform framing.
    ///       bytes 1-4:        Unique global id of the JSON schema associated with
    ///                         the data (as registered in Confluent Schema Registry),
    ///                         big endian.
    ///       following bytes:  The JSON data (utf8)
    ///
    ///     Internally, uses Newtonsoft.Json for deserialization. Currently,
    ///     no explicit validation of the data is done against the
    ///     schema stored in Schema Registry.
    ///
    ///     Note: Off-the-shelf libraries do not yet exist to enable
    ///     integration of System.Text.Json and JSON Schema, so this
    ///     is not yet supported by the deserializer.
    /// </remarks>
    public class JsonDeserializer<T> : IAsyncDeserializer<T> where T : class
    {
        private bool useLatestVersion = false;
        private IDictionary<string, string> useLatestWithMetadata = null;
        private SubjectNameStrategyDelegate subjectNameStrategy = null;
        
        private readonly int headerSize =  sizeof(int) + sizeof(byte);
        
        private readonly IDictionary<int, (Schema, JsonSchema)> schemaCache = new Dictionary<int, (Schema, JsonSchema)>();
        
        private SemaphoreSlim deserializeMutex = new SemaphoreSlim(1);

        private ISchemaRegistryClient schemaRegistryClient;
        private IList<IRuleExecutor> ruleExecutors;
        
        private readonly JsonSchemaGeneratorSettings jsonSchemaGeneratorSettings;

        /// <summary>
        ///     Initialize a new JsonDeserializer instance.
        /// </summary>
        /// <param name="config">
        ///     Deserializer configuration properties (refer to
        ///     <see cref="JsonDeserializerConfig" />).
        /// </param>
        /// <param name="jsonSchemaGeneratorSettings">
        ///     JSON schema generator settings.
        /// </param>
        public JsonDeserializer(IEnumerable<KeyValuePair<string, string>> config = null, JsonSchemaGeneratorSettings jsonSchemaGeneratorSettings = null) :
            this(null, config, jsonSchemaGeneratorSettings)
        {
        }

        public JsonDeserializer(ISchemaRegistryClient schemaRegistryClient, IEnumerable<KeyValuePair<string, string>> config = null, JsonSchemaGeneratorSettings jsonSchemaGeneratorSettings = null) 
            : this(schemaRegistryClient, config != null ? new JsonDeserializerConfig(config) : null, jsonSchemaGeneratorSettings)
        {
        }

        public JsonDeserializer(ISchemaRegistryClient schemaRegistryClient, JsonDeserializerConfig config, JsonSchemaGeneratorSettings jsonSchemaGeneratorSettings = null, IList<IRuleExecutor> ruleExecutors = null)
        {
            this.schemaRegistryClient = schemaRegistryClient;
            this.jsonSchemaGeneratorSettings = jsonSchemaGeneratorSettings;
            this.ruleExecutors = ruleExecutors ?? new List<IRuleExecutor>();

            if (config == null) { return; }

            var nonJsonConfig = config
                .Where(item => !item.Key.StartsWith("json.") && !item.Key.StartsWith("rules."));
            if (nonJsonConfig.Count() > 0)
            {
                throw new ArgumentException($"JsonDeserializer: unknown configuration parameter {nonJsonConfig.First().Key}.");
            }

            if (config.UseLatestVersion != null) { this.useLatestVersion = config.UseLatestVersion.Value; }
            if (config.UseLatestWithMetadata != null) { this.useLatestWithMetadata = config.UseLatestWithMetadata; }
            if (config.SubjectNameStrategy != null) { this.subjectNameStrategy = config.SubjectNameStrategy.Value.ToDelegate(); }
            
            foreach (IRuleExecutor executor in this.ruleExecutors.Concat(RuleRegistry.GetRuleExecutors()))
            {
                IEnumerable<KeyValuePair<string, string>> ruleConfigs = config
                    .Select(kv => new KeyValuePair<string, string>(
                        kv.Key.StartsWith("rules.") ? kv.Key.Substring("rules.".Length) : kv.Key, kv.Value));
                executor.Configure(ruleConfigs); 
            }
        }

        /// <summary>
        ///     Deserialize an object of type <typeparamref name="T"/>
        ///     from a byte array.
        /// </summary>
        /// <param name="data">
        ///     The raw byte data to deserialize.
        /// </param>
        /// <param name="isNull">
        ///     True if this is a null value.
        /// </param>
        /// <param name="context">
        ///     Context relevant to the deserialize operation.
        /// </param>
        /// <returns>
        ///     A <see cref="System.Threading.Tasks.Task" /> that completes
        ///     with the deserialized value.
        /// </returns>
        public async Task<T> DeserializeAsync(ReadOnlyMemory<byte> data, bool isNull, SerializationContext context)
        {
            if (isNull) { return null; }

            var array = data.ToArray();
            if (array.Length < 6)
            {
                throw new InvalidDataException($"Expecting data framing of length 6 bytes or more but total data size is {array.Length} bytes");
            }
            
            bool isKey = context.Component == MessageComponentType.Key;
            string topic = context.Topic;
            string subject = this.subjectNameStrategy != null
                // use the subject name strategy specified in the serializer config if available.
                ? this.subjectNameStrategy(
                    new SerializationContext(isKey ? MessageComponentType.Key : MessageComponentType.Value, topic),
                    null)
                // else fall back to the deprecated config from (or default as currently supplied by) SchemaRegistry.
                : schemaRegistryClient == null 
                    ? null
                    : isKey 
                        ? schemaRegistryClient.ConstructKeySubjectName(topic)
                        : schemaRegistryClient.ConstructValueSubjectName(topic);
            
            Schema latestSchema = await SerdeUtils.GetReaderSchema(schemaRegistryClient, subject, useLatestWithMetadata, useLatestVersion)
                .ConfigureAwait(continueOnCapturedContext: false);
                
            try
            {
                Schema writerSchema = null;
                JsonSchema writerSchemaJson = null;
                T value;
                using (var stream = new MemoryStream(array))
                using (var reader = new BinaryReader(stream))
                {
                    var magicByte = reader.ReadByte();
                    if (magicByte != Constants.MagicByte)
                    {
                        throw new InvalidDataException($"Expecting message {context.Component.ToString()} with Confluent Schema Registry framing. Magic byte was {array[0]}, expecting {Constants.MagicByte}");
                    }

                    // A schema is not required to deserialize protobuf messages since the
                    // serialized data includes tag and type information, which is enough for
                    // the IMessage<T> implementation to deserialize the data (even if the
                    // schema has evolved). _schemaId is thus unused.
                    var writerId = IPAddress.NetworkToHostOrder(reader.ReadInt32());

                    if (schemaRegistryClient != null)
                    {
                        (writerSchema, writerSchemaJson) = await GetSchema(writerId);
                    }
                    // A schema is not required to deserialize json messages.
                    // TODO: add validation capability.

                    using (var jsonStream = new MemoryStream(array, headerSize, array.Length - headerSize))
                    using (var jsonReader = new StreamReader(jsonStream, Encoding.UTF8))
                    {
                        value = Newtonsoft.Json.JsonConvert.DeserializeObject<T>(jsonReader.ReadToEnd(), this.jsonSchemaGeneratorSettings?.ActualSerializerSettings);
                    }
                    
                }
                if (writerSchema != null)
                {
                    FieldTransformer fieldTransformer = async (ctx, transform, message) =>
                    {
                        return await JsonUtils.Transform(ctx, writerSchemaJson, "$", message, transform).ConfigureAwait(false);
                    };
                    value = await SerdeUtils.ExecuteRules(context.Component == MessageComponentType.Key, subject,
                            context.Topic, context.Headers, RuleMode.Read, null,
                            writerSchema, value, fieldTransformer, ruleExecutors)
                        .ContinueWith(t => (T)t.Result)
                        .ConfigureAwait(continueOnCapturedContext: false);
                } 
                    
                return value;
            }
            catch (AggregateException e)
            {
                throw e.InnerException;
            }
        }

        private async Task<(Schema, JsonSchema)> GetSchema(int writerId)
        {
            Schema writerSchema;
            JsonSchema writerSchemaJson;
            await deserializeMutex.WaitAsync().ConfigureAwait(continueOnCapturedContext: false);
            try
            {
                if (schemaCache.TryGetValue(writerId, out var tuple))
                {
                    (writerSchema, writerSchemaJson) = tuple;
                }
                else
                {
                    if (schemaCache.Count > schemaRegistryClient.MaxCachedSchemas)
                    {
                        schemaCache.Clear();
                    }

                    writerSchema = await schemaRegistryClient.GetSchemaAsync(writerId).ConfigureAwait(continueOnCapturedContext: false);
                    writerSchemaJson = await JsonSchema.FromJsonAsync(writerSchema).ConfigureAwait(false);
                    schemaCache[writerId] = (writerSchema, writerSchemaJson);
                }
            }
            finally
            {
                deserializeMutex.Release();
            }

            return (writerSchema, writerSchemaJson);
        }
    }
}
