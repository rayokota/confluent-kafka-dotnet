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
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Avro.Specific;
using Avro.IO;
using Avro.Generic;
using Confluent.Kafka;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;


namespace Confluent.SchemaRegistry.Serdes
{
    internal class SpecificDeserializerImpl<T> : IAvroDeserializerImpl<T>
    {
        private readonly IDictionary<int, (Schema, Avro.Schema)> schemaCache = new Dictionary<int, (Schema, Avro.Schema)>();
        
        /// <remarks>
        ///     A datum reader cache (one corresponding to each write schema that's been seen) 
        ///     is maintained so that they only need to be constructed once.
        /// </remarks>
        private readonly Dictionary<(Avro.Schema, Avro.Schema), DatumReader<T>> datumReaderBySchema 
            = new Dictionary<(Avro.Schema, Avro.Schema), DatumReader<T>>();

        private SemaphoreSlim deserializeMutex = new SemaphoreSlim(1);

        /// <summary>
        ///     The Avro schema used to read values of type <typeparamref name="T"/>
        /// </summary>
        public global::Avro.Schema ReaderSchema { get; private set; }

        private ISchemaRegistryClient schemaRegistryClient;
        private bool useLatestVersion;
        private IDictionary<string, string> useLatestWithMetadata;
        private SubjectNameStrategyDelegate subjectNameStrategy;
        private IList<IRuleExecutor> ruleExecutors;

        public SpecificDeserializerImpl(
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

            if (typeof(ISpecificRecord).IsAssignableFrom(typeof(T)))
            {
                ReaderSchema = (global::Avro.Schema)typeof(T).GetField("_SCHEMA", BindingFlags.Public | BindingFlags.Static).GetValue(null);
            }
            else if (typeof(T).Equals(typeof(int)))
            {
                ReaderSchema = global::Avro.Schema.Parse("int");
            }
            else if (typeof(T).Equals(typeof(bool)))
            {
                ReaderSchema = global::Avro.Schema.Parse("boolean");
            }
            else if (typeof(T).Equals(typeof(double)))
            {
                ReaderSchema = global::Avro.Schema.Parse("double");
            }
            else if (typeof(T).Equals(typeof(string)))
            {
                ReaderSchema = global::Avro.Schema.Parse("string");
            }
            else if (typeof(T).Equals(typeof(float)))
            {
                ReaderSchema = global::Avro.Schema.Parse("float");
            }
            else if (typeof(T).Equals(typeof(long)))
            {
                ReaderSchema = global::Avro.Schema.Parse("long");
            }
            else if (typeof(T).Equals(typeof(byte[])))
            {
                ReaderSchema = global::Avro.Schema.Parse("bytes");
            }
            else if (typeof(T).Equals(typeof(Null)))
            {
                ReaderSchema = global::Avro.Schema.Parse("null");
            }
            else
            {
                throw new InvalidOperationException(
                    $"{nameof(AvroDeserializer<T>)} " +
                    "only accepts type parameters of int, bool, double, string, float, " +
                    "long, byte[], instances of ISpecificRecord and subclasses of SpecificFixed."
                );
            }
        }

        public async Task<T> Deserialize(string topic, Headers headers, byte[] array, bool isKey)
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

                Schema writerSchemaJson = null;
                Avro.Schema writerSchema = null;
                object data;
                IList<Migration> migrations = new List<Migration>();
                using (var stream = new MemoryStream(array))
                using (var reader = new BinaryReader(stream))
                {
                    var magicByte = reader.ReadByte();
                    if (magicByte != Constants.MagicByte)
                    {
                        throw new InvalidDataException($"Expecting data with Confluent Schema Registry framing. Magic byte was {array[0]}, expecting {Constants.MagicByte}");
                    }
                    var writerId = IPAddress.NetworkToHostOrder(reader.ReadInt32());

                    (writerSchemaJson, writerSchema) = await GetSchema(writerId);
                    
                    if (latestSchema != null)
                    {
                        migrations = await SerdeUtils.GetMigrations(schemaRegistryClient, subject, writerSchemaJson, latestSchema)
                            .ConfigureAwait(continueOnCapturedContext: false);
                    }

                    DatumReader<T> datumReader = null;
                    if (migrations.Count > 0)
                    {
                        data = new GenericReader<GenericRecord>(writerSchema, writerSchema)
                            .Read(default(GenericRecord), new BinaryDecoder(stream));
                        
                        string jsonString = null;
                        using (var jsonStream = new MemoryStream())
                        {
                            GenericRecord record = (GenericRecord)data;
                            DatumWriter<object> datumWriter = new GenericDatumWriter<object>(writerSchema);

                            JsonEncoder encoder = new JsonEncoder(writerSchema, jsonStream);
                            datumWriter.Write(record, encoder);
                            encoder.Flush();

                            jsonString = Encoding.UTF8.GetString(jsonStream.ToArray());
                        }
                        
                        JToken json = JToken.Parse(jsonString);
                        json = await SerdeUtils.ExecuteMigrations(migrations, isKey, subject, topic, headers, json)
                            .ContinueWith(t => (JToken)t.Result)
                            .ConfigureAwait(continueOnCapturedContext: false);
                        Avro.IO.Decoder decoder = new JsonDecoder(ReaderSchema, json.ToString(Formatting.None));
                        
                        datumReader = new SpecificReader<T>(ReaderSchema, ReaderSchema);
                        data = Read(datumReader, decoder);
                    }
                    else
                    {
                        datumReader = await GetDatumReader(writerSchema, ReaderSchema);
                        data = Read(datumReader, new BinaryDecoder(stream));
                    }
                }

                FieldTransformer fieldTransformer = async (ctx, transform, message) => 
                {
                    return await AvroUtils.Transform(ctx, writerSchema, message, transform).ConfigureAwait(false);
                };
                data = await SerdeUtils.ExecuteRules(isKey, subject, topic, headers, RuleMode.Read, null,
                    writerSchemaJson, data, fieldTransformer, ruleExecutors)
                    .ConfigureAwait(continueOnCapturedContext: false);

                return (T) data;
            }
            catch (AggregateException e)
            {
                throw e.InnerException;
            }
        }

        private async Task<(Schema, Avro.Schema)> GetSchema(int writerId)
        {
            Schema writerSchemaJson;
            Avro.Schema writerSchema;
            await deserializeMutex.WaitAsync().ConfigureAwait(continueOnCapturedContext: false);
            try
            {
                if (schemaCache.TryGetValue(writerId, out var tuple))
                {
                    (writerSchemaJson, writerSchema) = tuple;
                }
                else
                {
                    if (schemaCache.Count > schemaRegistryClient.MaxCachedSchemas)
                    {
                        schemaCache.Clear();
                    }

                    writerSchemaJson = await schemaRegistryClient.GetSchemaAsync(writerId).ConfigureAwait(continueOnCapturedContext: false);
                    writerSchema = global::Avro.Schema.Parse(writerSchemaJson.SchemaString);
                    schemaCache[writerId] = (writerSchemaJson, writerSchema);
                }
            }
            finally
            {
                deserializeMutex.Release();
            }

            return (writerSchemaJson, writerSchema);
        }
        
        private async Task<DatumReader<T>> GetDatumReader(Avro.Schema writerSchema, Avro.Schema readerSchema)
        {
            DatumReader<T> datumReader = null;
            await deserializeMutex.WaitAsync().ConfigureAwait(continueOnCapturedContext: false);
            try
            {
                if (datumReaderBySchema.TryGetValue((writerSchema, readerSchema), out datumReader))
                {
                    return datumReader;

                }
                else
                {
                    if (datumReaderBySchema.Count > schemaRegistryClient.MaxCachedSchemas)
                    {
                        datumReaderBySchema.Clear();
                    }

                    if (readerSchema == null)
                    {
                        readerSchema = writerSchema;
                    }
                    datumReader = new SpecificReader<T>(writerSchema, readerSchema);
                    datumReaderBySchema[(writerSchema, readerSchema)] = datumReader;
                    return datumReader;
                }
            }
            finally
            {
                deserializeMutex.Release();
            }
        }

        private static object Read(DatumReader<T> datumReader, Avro.IO.Decoder decoder)
        {
            object data;
            if (typeof(ISpecificRecord).IsAssignableFrom(typeof(T)))
            {
                // This is a generic deserializer and it knows the type that needs to be serialized into. 
                // Passing default(T) will result in null value and that will force the datumRead to
                // use the schema namespace and name provided in the schema, which may not match (T).
                var reuse = Activator.CreateInstance<T>();
                data = datumReader.Read(reuse, decoder);
            }
            else
            {
                data = datumReader.Read(default(T), decoder);
            }

            return data;
        }
    }
}
