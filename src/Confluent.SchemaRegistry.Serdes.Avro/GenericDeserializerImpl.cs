﻿// Copyright 2018 Confluent Inc.
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
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Avro.IO;
using Avro.Generic;
using Confluent.Kafka;


namespace Confluent.SchemaRegistry.Serdes
{
    internal class GenericDeserializerImpl : IAvroDeserializerImpl<GenericRecord>
    {
        /// <remarks>
        ///     A datum reader cache (one corresponding to each write schema that's been seen)
        ///     is maintained so that they only need to be constructed once.
        /// </remarks>
        private readonly Dictionary<int, DatumReader<GenericRecord>> datumReaderBySchemaId 
            = new Dictionary<int, DatumReader<GenericRecord>>();
       
        private SemaphoreSlim deserializeMutex = new SemaphoreSlim(1);

        private ISchemaRegistryClient schemaRegistryClient;
        private bool useLatestVersion;
        private IDictionary<string, string> useLatestWithMetadata;
        private SubjectNameStrategyDelegate subjectNameStrategy;
        private IList<IRuleExecutor> ruleExecutors;

        public GenericDeserializerImpl(
            ISchemaRegistryClient schemaRegistryClient, 
            bool useLatestVersion, 
            IDictionary<string, string> useLatestWithMetadata, 
            SubjectNameStrategyDelegate subjectNameStrategy,
            IList<IRuleExecutor> ruleExecutors)
        {
            this.schemaRegistryClient = schemaRegistryClient;
            this.useLatestVersion = useLatestVersion;
            this.useLatestWithMetadata = useLatestWithMetadata;
            this.subjectNameStrategy = subjectNameStrategy;
            this.ruleExecutors = ruleExecutors;
        }

        public async Task<GenericRecord> Deserialize(string topic, Headers headers, byte[] array, bool isKey)
        {
            try
            {
                // Note: topic is not necessary for deserialization (or knowing if it's a key 
                // or value) only the schema id is needed.

                if (array.Length < 5)
                {
                    throw new InvalidDataException($"Expecting data framing of length 5 bytes or more but total data size is {array.Length} bytes");
                }

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
                
                Schema writerSchemaResult = null;
                GenericRecord data;
                using (var stream = new MemoryStream(array))
                using (var reader = new BinaryReader(stream))
                {
                    var magicByte = reader.ReadByte();
                    if (magicByte != Constants.MagicByte)
                    {
                        throw new InvalidDataException($"Expecting data with Confluent Schema Registry framing. Magic byte was {array[0]}, expecting {Constants.MagicByte}");
                    }
                    var writerId = IPAddress.NetworkToHostOrder(reader.ReadInt32());

                    DatumReader<GenericRecord> datumReader;
                    await deserializeMutex.WaitAsync().ConfigureAwait(continueOnCapturedContext: false);
                    try
                    {
                        datumReaderBySchemaId.TryGetValue(writerId, out datumReader);
                        if (datumReader == null)
                        {
                            // TODO: If any of this cache fills up, this is probably an
                            // indication of misuse of the deserializer. Ideally we would do 
                            // something more sophisticated than the below + not allow 
                            // the misuse to keep happening without warning.
                            if (datumReaderBySchemaId.Count > schemaRegistryClient.MaxCachedSchemas)
                            {
                                datumReaderBySchemaId.Clear();
                            }

                            writerSchemaResult = await schemaRegistryClient.GetSchemaAsync(writerId)
                                .ConfigureAwait(continueOnCapturedContext: false);
                            if (writerSchemaResult.SchemaType != SchemaType.Avro)
                            {
                                throw new InvalidOperationException("Expecting writer schema to have type Avro, not {writerSchemaResult.SchemaType}");
                            }
                            var writerSchema = global::Avro.Schema.Parse(writerSchemaResult.SchemaString);

                            datumReader = new GenericReader<GenericRecord>(writerSchema, writerSchema);
                            datumReaderBySchemaId[writerId] = datumReader;
                        }
                    }
                    finally
                    {
                        deserializeMutex.Release();
                    }
                    
                    data = datumReader.Read(default(GenericRecord), new BinaryDecoder(stream));

                }
                
                var schema = Avro.Schema.Parse(writerSchemaResult.SchemaString);
                FieldTransformer fieldTransformer = async (ctx, transform, message) => 
                {
                    return await AvroUtils.Transform(ctx, schema, message, transform).ConfigureAwait(false);
                };
                data = await SerdeUtils.ExecuteRules(isKey, subject, topic, headers, RuleMode.Read, null,
                    writerSchemaResult, data, fieldTransformer, ruleExecutors)
                    .ContinueWith(t => (GenericRecord)t.Result)
                    .ConfigureAwait(continueOnCapturedContext: false);

                return data;
            }
            catch (AggregateException e)
            {
                throw e.InnerException;
            }
        }

    }
}
