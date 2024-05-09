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

// ConstructValueSubjectName is still used a an internal implementation detail.
#pragma warning disable CS0618

using Confluent.Kafka;
using Confluent.SchemaRegistry.Encryption;
using Moq;
using Newtonsoft.Json;
using Newtonsoft.Json.Serialization;
using NJsonSchema.Generation;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;


namespace Confluent.SchemaRegistry.Serdes.UnitTests
{
    public class JsonSerializeDeserialzeTests
    {
        public class UInt32Value
        {
            public int Value { get; set; }
        }

#nullable enable
        public class NonNullStringValue
        {
            public string Value { get; set; } = "";

            public NestedNonNullStringValue Nested { get; set; } = new();
        }

        public class NestedNonNullStringValue
        {
            public string Value { get; set; } = "";
        }
#nullable disable

        private class UInt32ValueMultiplyConverter : JsonConverter
        {
            public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
            {
                var newValue = ((UInt32Value) value).Value * 2;
                writer.WriteStartObject();
                writer.WritePropertyName("Value");
                writer.WriteValue(newValue);
                writer.WriteEndObject();
            }

            public override object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer)
            {
                if (reader.TokenType == JsonToken.StartObject)
                {
                    reader.Read();
                }

                var value = reader.ReadAsInt32() ?? 0;
                reader.Read();
                return new UInt32Value
                {
                    Value = value / 2
                };
            }

            public override bool CanConvert(Type objectType) => objectType == typeof(UInt32Value);
        }

        public enum EnumType
        {
            None,
            EnumValue = 1234,
            OtherValue = 5678
        }

        public class EnumObject
        {
            public EnumType Value { get; set; }
        }

        private ISchemaRegistryClient schemaRegistryClient;
        private IDekRegistryClient dekRegistryClient;
        private string testTopic;
        private Dictionary<string, int> store = new Dictionary<string, int>();
        private Dictionary<string, RegisteredSchema> subjectStore = new Dictionary<string, RegisteredSchema>();
        private IDictionary<KekId, RegisteredKek> kekStore = new Dictionary<KekId, RegisteredKek>();
        private IDictionary<DekId, RegisteredDek> dekStore = new Dictionary<DekId, RegisteredDek>();

        public JsonSerializeDeserialzeTests()
        {
            testTopic = "topic";
            var schemaRegistryMock = new Mock<ISchemaRegistryClient>();
            schemaRegistryMock.Setup(x => x.ConstructValueSubjectName(testTopic, It.IsAny<string>())).Returns($"{testTopic}-value");
            schemaRegistryMock.Setup(x => x.RegisterSchemaAsync("topic-value", It.IsAny<string>(), It.IsAny<bool>())).ReturnsAsync(
                (string topic, string schema, bool normalize) => store.TryGetValue(schema, out int id) ? id : store[schema] = store.Count + 1
            );
            schemaRegistryMock.Setup(x => x.GetSchemaAsync(It.IsAny<int>(), It.IsAny<string>())).ReturnsAsync(
                (int id, string format) =>
                {
                    try
                    {
                        // First try subjectStore
                        return subjectStore.Where(x => x.Value.Id == id).First().Value;
                    }
                    catch (InvalidOperationException e)
                    {
                        // Next try store
                        return new Schema(store.Where(x => x.Value == id).First().Key, null, SchemaType.Protobuf);
                    }
                });
            schemaRegistryMock.Setup(x => x.GetLatestSchemaAsync(It.IsAny<string>())).ReturnsAsync(
                (string subject) => subjectStore[subject]
            );
            schemaRegistryClient = schemaRegistryMock.Object;   
            
            var dekRegistryMock = new Mock<IDekRegistryClient>();
            dekRegistryMock.Setup(x => x.CreateKekAsync(It.IsAny<Kek>())).ReturnsAsync(
                (Kek kek) =>
                {
                    var kekId = new KekId(kek.Name, false);
                    return kekStore.TryGetValue(kekId, out RegisteredKek registeredKek)
                        ? registeredKek
                        : kekStore[kekId] = new RegisteredKek
                        {
                            Name = kek.Name,
                            KmsType = kek.KmsType,
                            KmsKeyId = kek.KmsKeyId,
                            KmsProps = kek.KmsProps,
                            Doc = kek.Doc,
                            Shared = kek.Shared,
                            Deleted = false,
                            Timestamp = DateTimeOffset.Now.ToUnixTimeMilliseconds()
                        };
                });
            dekRegistryMock.Setup(x => x.GetKekAsync(It.IsAny<string>(), It.IsAny<bool>())).ReturnsAsync(
                (string name, bool ignoreDeletedKeks) =>
                {
                    var kekId = new KekId(name, false);
                    return kekStore.TryGetValue(kekId, out RegisteredKek registeredKek) ? registeredKek : null;
                });
            dekRegistryMock.Setup(x => x.CreateDekAsync(It.IsAny<string>(), It.IsAny<Dek>())).ReturnsAsync(
                (string kekName, Dek dek) =>
                {
                    int version = dek.Version ?? 1;
                    var dekId = new DekId(kekName, dek.Subject, version, dek.Algorithm, false);
                    return dekStore.TryGetValue(dekId, out RegisteredDek registeredDek)
                        ? registeredDek
                        : dekStore[dekId] = new RegisteredDek
                        {
                            KekName = kekName,
                            Subject = dek.Subject,
                            Version = version,
                            Algorithm = dek.Algorithm,
                            EncryptedKeyMaterial = dek.EncryptedKeyMaterial,
                            Deleted = false,
                            Timestamp = DateTimeOffset.Now.ToUnixTimeMilliseconds()
                        };
                });
            dekRegistryMock.Setup(x => x.GetDekAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<DekFormat>(), It.IsAny<bool>())).ReturnsAsync(
                (string kekName, string subject, DekFormat? algorithm, bool ignoreDeletedKeks) =>
                {
                    var dekId = new DekId(kekName, subject, 1, algorithm, false);
                    return dekStore.TryGetValue(dekId, out RegisteredDek registeredDek) ? registeredDek : null;
                });
            dekRegistryMock.Setup(x => x.GetDekVersionAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<int>(), It.IsAny<DekFormat>(), It.IsAny<bool>())).ReturnsAsync(
                (string kekName, string subject, int version, DekFormat? algorithm, bool ignoreDeletedKeks) =>
                {
                    var dekId = new DekId(kekName, subject, version, algorithm, false);
                    return dekStore.TryGetValue(dekId, out RegisteredDek registeredDek) ? registeredDek : null;
                });
            dekRegistryClient = dekRegistryMock.Object;
            
            // Register rule executors and kms drivers
            FieldEncryptionExecutor.Register(dekRegistryClient);
            LocalKmsDriver.Register();
        }

        [Fact]
        public void Null()
        {
            var jsonSerializer = new JsonSerializer<UInt32Value>(schemaRegistryClient);
            var jsonDeserializer = new JsonDeserializer<UInt32Value>(schemaRegistryClient);

            var bytes = jsonSerializer.SerializeAsync(null, new SerializationContext(MessageComponentType.Value, testTopic)).Result;
            Assert.Null(bytes);
            Assert.Null(jsonDeserializer.DeserializeAsync(bytes, true, new SerializationContext(MessageComponentType.Value, testTopic)).Result);
        }


        [Fact]
        public void UInt32SerDe()
        {
            var jsonSerializer = new JsonSerializer<UInt32Value>(schemaRegistryClient);
            var jsonDeserializer = new JsonDeserializer<UInt32Value>();

            var v = new UInt32Value { Value = 1234 };
            var bytes = jsonSerializer.SerializeAsync(v, new SerializationContext(MessageComponentType.Value, testTopic)).Result;
            Assert.Equal(v.Value, jsonDeserializer.DeserializeAsync(bytes, false, new SerializationContext(MessageComponentType.Value, testTopic)).Result.Value);
        }

        [Fact]
        public async Task WithJsonSerializerSettingsSerDe()
        {
            const int value = 1234;
            var expectedJson = $"{{\"Value\":{value * 2}}}";
            var jsonSchemaGeneratorSettings = new JsonSchemaGeneratorSettings
            {
                SerializerSettings = new JsonSerializerSettings
                {
                    Converters = new List<JsonConverter>
                    {
                        new UInt32ValueMultiplyConverter()
                    },
                    ContractResolver = new DefaultContractResolver()
                }
            };

            var jsonSerializer = new JsonSerializer<UInt32Value>(schemaRegistryClient, jsonSchemaGeneratorSettings: jsonSchemaGeneratorSettings);
            var jsonDeserializer = new JsonDeserializer<UInt32Value>(jsonSchemaGeneratorSettings: jsonSchemaGeneratorSettings);

            var v = new UInt32Value { Value = value };
            var bytes = await jsonSerializer.SerializeAsync(v, new SerializationContext(MessageComponentType.Value, testTopic));
            Assert.NotNull(bytes);
            Assert.Equal(expectedJson, Encoding.UTF8.GetString(bytes.AsSpan().Slice(5)));

            var actual = await jsonDeserializer.DeserializeAsync(bytes, false, new SerializationContext(MessageComponentType.Value, testTopic));
            Assert.NotNull(actual);
            Assert.Equal(v.Value, actual.Value);
        }

        [Theory]
        [InlineData(EnumHandling.CamelCaseString, EnumType.EnumValue, "{\"Value\":\"enumValue\"}")]
        [InlineData(EnumHandling.String, EnumType.None, "{\"Value\":\"None\"}")]
        [InlineData(EnumHandling.Integer, EnumType.OtherValue, "{\"Value\":5678}")]
        public async Task WithJsonSchemaGeneratorSettingsSerDe(EnumHandling enumHandling, EnumType value, string expectedJson)
        {
            var jsonSchemaGeneratorSettings = new JsonSchemaGeneratorSettings
            {
                DefaultEnumHandling = enumHandling
            };

            var jsonSerializer = new JsonSerializer<EnumObject>(schemaRegistryClient, jsonSchemaGeneratorSettings: jsonSchemaGeneratorSettings);
            var jsonDeserializer = new JsonDeserializer<EnumObject>(jsonSchemaGeneratorSettings: jsonSchemaGeneratorSettings);

            var v = new EnumObject { Value = value };
            var bytes = await jsonSerializer.SerializeAsync(v, new SerializationContext(MessageComponentType.Value, testTopic));
            Assert.NotNull(bytes);
            Assert.Equal(expectedJson, Encoding.UTF8.GetString(bytes.AsSpan().Slice(5)));

            var actual = await jsonDeserializer.DeserializeAsync(bytes, false, new SerializationContext(MessageComponentType.Value, testTopic));
            Assert.NotNull(actual);
            Assert.Equal(actual.Value, value);
        }

        [Fact]
        public async Task ValidationFailureReturnsPath()
        {
            var jsonSerializer = new JsonSerializer<NonNullStringValue>(schemaRegistryClient);

            var v = new NonNullStringValue { Value = null };

            try
            {
                await jsonSerializer.SerializeAsync(v, new SerializationContext(MessageComponentType.Value, testTopic));
                Assert.True(false, "Serialization did not throw an expected exception");
            }
            catch (InvalidDataException ex)
            {
                Assert.Equal("Schema validation failed for properties: [#/Value]", ex.Message);
            }
            catch (Exception ex)
            {
                Assert.True(false, $"Serialization threw exception of type {ex.GetType().FullName} instead of the expected {typeof(InvalidDataException).FullName}");
            }
        }

        [Fact]
        public async Task NestedValidationFailureReturnsPath()
        {
            var jsonSerializer = new JsonSerializer<NonNullStringValue>(schemaRegistryClient);

            var v = new NonNullStringValue 
            { 
                Nested = new()
                {
                    Value = null
                }
            };

            try
            {
                await jsonSerializer.SerializeAsync(v, new SerializationContext(MessageComponentType.Value, testTopic));
                Assert.True(false, "Serialization did not throw an expected exception");
            }
            catch (InvalidDataException ex)
            {
                Assert.Equal("Schema validation failed for properties: [#/Nested.Value]", ex.Message);
            }
            catch (Exception ex)
            {
                Assert.True(false, $"Serialization threw exception of type {ex.GetType().FullName} instead of the expected {typeof(InvalidDataException).FullName}");
            }
        }
        
        [Fact]
        public void FieldEncryption()
        {
            var schemaStr = @"{
              ""type"": ""object"",
              ""properties"": {
                ""favorite_color"": {
                  ""type"": ""string""
                },
                ""favorite_number"": {
                  ""type"": ""number""
                },
                ""name"": {
                  ""type"": ""string"",
                  ""confluent:tags"": [ ""PII"" ]
                }
              }
            }";
            
            var schema = new RegisteredSchema("topic-value", 1, 1, schemaStr, SchemaType.Json, null);
            schema.Metadata = new Metadata(new Dictionary<string, ISet<string>>
                {
                    ["$.name"] = new HashSet<string> { "PII" }

                }, new Dictionary<string, string>(), new HashSet<string>()
            );
            schema.RuleSet = new RuleSet(new List<Rule>(),
                new List<Rule>
                {
                    new Rule("encryptPII", RuleKind.Transform, RuleMode.WriteRead, "ENCRYPT", new HashSet<string>
                    {
                        "PII"
                    }, new Dictionary<string, string>
                    {
                        ["encrypt.kek.name"] = "kek1",
                        ["encrypt.kms.type"] = "local-kms",
                        ["encrypt.kms.key.id"] = "mykey"
                    })
                }
            );
            store[schemaStr] = 1;
            subjectStore["topic-value"] = schema; 
            var config = new JsonSerializerConfig
            {
                AutoRegisterSchemas = false,
                UseLatestVersion = true
            };
            config.Set("rules.secret", "mysecret");
            var serializer = new JsonSerializer<Customer>(schemaRegistryClient, config, null);
            var deserializer = new JsonDeserializer<Customer>(schemaRegistryClient, null, null);

            var user = new Customer
            {
                FavoriteColor = "blue",
                FavoriteNumber = 100,
                Name = "awesome"
            };

            Headers headers = new Headers();
            var bytes = serializer.SerializeAsync(user, new SerializationContext(MessageComponentType.Value, testTopic, headers)).Result;
            var result = deserializer.DeserializeAsync(bytes, false, new SerializationContext(MessageComponentType.Value, testTopic, headers)).Result;

            // The user name has been modified
            Assert.Equal("awesome", result.Name);
            Assert.Equal(user.FavoriteColor, result.FavoriteColor);
            Assert.Equal(user.FavoriteNumber, result.FavoriteNumber);
        }
    }

    class Customer
    {
        [JsonProperty("favorite_color")]
        public string FavoriteColor { get; set; }
        [JsonProperty("favorite_number")]
        public int FavoriteNumber { get; set; }
        [JsonProperty("name")]
        public string Name { get; set; }
    }
}
