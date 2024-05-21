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
using Newtonsoft.Json;
using Newtonsoft.Json.Serialization;
using NJsonSchema.Generation;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using Xunit;


namespace Confluent.SchemaRegistry.Serdes.UnitTests
{
    public class JsonSerializeDeserializeTests : BaseSerializeDeserializeTests
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
                var newValue = ((UInt32Value)value).Value * 2;
                writer.WriteStartObject();
                writer.WritePropertyName("Value");
                writer.WriteValue(newValue);
                writer.WriteEndObject();
            }

            public override object ReadJson(JsonReader reader, Type objectType, object existingValue,
                JsonSerializer serializer)
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

        public JsonSerializeDeserializeTests() : base()
        {
        }

        [Fact]
        public void Null()
        {
            var jsonSerializer = new JsonSerializer<UInt32Value>(schemaRegistryClient);
            var jsonDeserializer = new JsonDeserializer<UInt32Value>(schemaRegistryClient);

            var bytes = jsonSerializer
                .SerializeAsync(null, new SerializationContext(MessageComponentType.Value, testTopic)).Result;
            Assert.Null(bytes);
            Assert.Null(jsonDeserializer
                .DeserializeAsync(bytes, true, new SerializationContext(MessageComponentType.Value, testTopic)).Result);
        }


        [Fact]
        public void UInt32SerDe()
        {
            var jsonSerializer = new JsonSerializer<UInt32Value>(schemaRegistryClient);
            var jsonDeserializer = new JsonDeserializer<UInt32Value>();

            var v = new UInt32Value { Value = 1234 };
            var bytes = jsonSerializer
                .SerializeAsync(v, new SerializationContext(MessageComponentType.Value, testTopic)).Result;
            Assert.Equal(v.Value,
                jsonDeserializer
                    .DeserializeAsync(bytes, false, new SerializationContext(MessageComponentType.Value, testTopic))
                    .Result.Value);
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

            var jsonSerializer = new JsonSerializer<UInt32Value>(schemaRegistryClient,
                jsonSchemaGeneratorSettings: jsonSchemaGeneratorSettings);
            var jsonDeserializer =
                new JsonDeserializer<UInt32Value>(jsonSchemaGeneratorSettings: jsonSchemaGeneratorSettings);

            var v = new UInt32Value { Value = value };
            var bytes = await jsonSerializer.SerializeAsync(v,
                new SerializationContext(MessageComponentType.Value, testTopic));
            Assert.NotNull(bytes);
            Assert.Equal(expectedJson, Encoding.UTF8.GetString(bytes.AsSpan().Slice(5)));

            var actual = await jsonDeserializer.DeserializeAsync(bytes, false,
                new SerializationContext(MessageComponentType.Value, testTopic));
            Assert.NotNull(actual);
            Assert.Equal(v.Value, actual.Value);
        }

        [Theory]
        [InlineData(EnumHandling.CamelCaseString, EnumType.EnumValue, "{\"Value\":\"enumValue\"}")]
        [InlineData(EnumHandling.String, EnumType.None, "{\"Value\":\"None\"}")]
        [InlineData(EnumHandling.Integer, EnumType.OtherValue, "{\"Value\":5678}")]
        public async Task WithJsonSchemaGeneratorSettingsSerDe(EnumHandling enumHandling, EnumType value,
            string expectedJson)
        {
            var jsonSchemaGeneratorSettings = new JsonSchemaGeneratorSettings
            {
                DefaultEnumHandling = enumHandling
            };

            var jsonSerializer = new JsonSerializer<EnumObject>(schemaRegistryClient,
                jsonSchemaGeneratorSettings: jsonSchemaGeneratorSettings);
            var jsonDeserializer =
                new JsonDeserializer<EnumObject>(jsonSchemaGeneratorSettings: jsonSchemaGeneratorSettings);

            var v = new EnumObject { Value = value };
            var bytes = await jsonSerializer.SerializeAsync(v,
                new SerializationContext(MessageComponentType.Value, testTopic));
            Assert.NotNull(bytes);
            Assert.Equal(expectedJson, Encoding.UTF8.GetString(bytes.AsSpan().Slice(5)));

            var actual = await jsonDeserializer.DeserializeAsync(bytes, false,
                new SerializationContext(MessageComponentType.Value, testTopic));
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
                Assert.True(false,
                    $"Serialization threw exception of type {ex.GetType().FullName} instead of the expected {typeof(InvalidDataException).FullName}");
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
                Assert.True(false,
                    $"Serialization threw exception of type {ex.GetType().FullName} instead of the expected {typeof(InvalidDataException).FullName}");
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
            subjectStore["topic-value"] = new List<RegisteredSchema> { schema };
            var config = new JsonSerializerConfig
            {
                AutoRegisterSchemas = false,
                UseLatestVersion = true
            };
            config.Set("rules.secret", "mysecret");
            IRuleExecutor ruleExecutor = new FieldEncryptionExecutor(dekRegistryClient);
            var serializer = new JsonSerializer<Customer>(schemaRegistryClient, config, null,
                new List<IRuleExecutor> { ruleExecutor });
            var deserializer = new JsonDeserializer<Customer>(schemaRegistryClient, null, null,
                new List<IRuleExecutor> { ruleExecutor });

            var user = new Customer
            {
                FavoriteColor = "blue",
                FavoriteNumber = 100,
                Name = "awesome"
            };

            Headers headers = new Headers();
            var bytes = serializer
                .SerializeAsync(user, new SerializationContext(MessageComponentType.Value, testTopic, headers)).Result;
            var result = deserializer.DeserializeAsync(bytes, false,
                new SerializationContext(MessageComponentType.Value, testTopic, headers)).Result;

            // The user name has been modified
            Assert.Equal("awesome", result.Name);
            Assert.Equal(user.FavoriteColor, result.FavoriteColor);
            Assert.Equal(user.FavoriteNumber, result.FavoriteNumber);
        }

        [Fact]
        public void JSONataFullyCompatible()
        {
            var rule1To2 = "$merge([$sift($, function($v, $k) {$k != 'name'}), {'full_name': $.'name'}])";
            var rule2To1 = "$merge([$sift($, function($v, $k) {$k != 'full_name'}), {'name': $.'full_name'}])";
            var rule2To3 = "$merge([$sift($, function($v, $k) {$k != 'full_name'}), {'title': $.'full_name'}])";
            var rule3To2 = "$merge([$sift($, function($v, $k) {$k != 'title'}), {'full_name': $.'title'}])";

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
                  ""type"": ""string""
                }
              }
            }";
            var schema = new RegisteredSchema("topic-value", 1, 1, schemaStr, SchemaType.Json, null);
            schema.Metadata = new Metadata(null, new Dictionary<string, string>
                {
                    { "application.version", "1" }

                }, new HashSet<string>()
            );
            store[schemaStr] = 1;
            var config1 = new JsonSerializerConfig
            {
                AutoRegisterSchemas = false,
                UseLatestVersion = false,
                UseLatestWithMetadata = new Dictionary<string, string> { { "application.version", "1" } }
            };
            var deserConfig1 = new JsonDeserializerConfig
            {
                UseLatestVersion = false,
                UseLatestWithMetadata = new Dictionary<string, string> { { "application.version", "1" } }
            };
            var serializer1 = new JsonSerializer<Customer>(schemaRegistryClient, config1);
            var deserializer1 = new JsonDeserializer<Customer>(schemaRegistryClient, deserConfig1);

            var user = new Customer
            {
                FavoriteColor = "blue",
                FavoriteNumber = 100,
                Name = "awesome"
            };

            var newSchemaStr = @"{
              ""type"": ""object"",
              ""properties"": {
                ""favorite_color"": {
                  ""type"": ""string""
                },
                ""favorite_number"": {
                  ""type"": ""number""
                },
                ""full_name"": {
                  ""type"": ""string""
                }
              }
            }";
            var newSchema = new RegisteredSchema("topic-value", 2, 2, newSchemaStr, SchemaType.Json, null);
            newSchema.Metadata = new Metadata(null, new Dictionary<string, string>
                {
                    { "application.version", "2" }

                }, new HashSet<string>()
            );
            newSchema.RuleSet = new RuleSet(
                new List<Rule>
                {
                    new Rule("myRule1", RuleKind.Transform, RuleMode.Upgrade, "JSONATA", null,
                        null, rule1To2, null, null, false),
                    new Rule("myRule2", RuleKind.Transform, RuleMode.Downgrade, "JSONATA", null,
                        null, rule2To1, null, null, false)
                }, new List<Rule>()
            );
            var config2 = new JsonSerializerConfig
            {
                AutoRegisterSchemas = false,
                UseLatestVersion = false,
                UseLatestWithMetadata = new Dictionary<string, string> { { "application.version", "2" } }
            };
            var deserConfig2 = new JsonDeserializerConfig
            {
                UseLatestVersion = false,
                UseLatestWithMetadata = new Dictionary<string, string> { { "application.version", "2" } }
            };
            var serializer2 = new JsonSerializer<NewCustomer>(schemaRegistryClient, config2);
            var deserializer2 = new JsonDeserializer<NewCustomer>(schemaRegistryClient, deserConfig2);

            var newUser = new NewCustomer
            {
                FavoriteColor = "blue",
                FavoriteNumber = 100,
                FullName = "awesome"
            };

            var newerSchemaStr = @"{
              ""type"": ""object"",
              ""properties"": {
                ""favorite_color"": {
                  ""type"": ""string""
                },
                ""favorite_number"": {
                  ""type"": ""number""
                },
                ""title"": {
                  ""type"": ""string""
                }
              }
            }";
            var newerSchema = new RegisteredSchema("topic-value", 3, 3, newerSchemaStr, SchemaType.Json, null);
            newerSchema.Metadata = new Metadata(null, new Dictionary<string, string>
                {
                    { "application.version", "3" }

                }, new HashSet<string>()
            );
            newerSchema.RuleSet = new RuleSet(
                new List<Rule>
                {
                    new Rule("myRule1", RuleKind.Transform, RuleMode.Upgrade, "JSONATA", null,
                        null, rule2To3, null, null, false),
                    new Rule("myRule2", RuleKind.Transform, RuleMode.Downgrade, "JSONATA", null,
                        null, rule3To2, null, null, false)
                }, new List<Rule>()
            );
            var config3 = new JsonSerializerConfig
            {
                AutoRegisterSchemas = false,
                UseLatestVersion = false,
                UseLatestWithMetadata = new Dictionary<string, string> { { "application.version", "3" } }
            };
            var deserConfig3 = new JsonDeserializerConfig
            {
                UseLatestVersion = false,
                UseLatestWithMetadata = new Dictionary<string, string> { { "application.version", "3" } }
            };
            var serializer3 = new JsonSerializer<NewerCustomer>(schemaRegistryClient, config3);
            var deserializer3 = new JsonDeserializer<NewerCustomer>(schemaRegistryClient, deserConfig3);

            var newerUser = new NewerCustomer
            {
                FavoriteColor = "blue",
                FavoriteNumber = 100,
                Title = "awesome"
            };

            store[schemaStr] = 1;
            store[newSchemaStr] = 2;
            store[newerSchemaStr] = 3;
            subjectStore["topic-value"] = new List<RegisteredSchema> { schema, newSchema, newerSchema };

            Headers headers = new Headers();
            var bytes = serializer1
                .SerializeAsync(user, new SerializationContext(MessageComponentType.Value, testTopic, headers)).Result;
            DeserializeAllVersions(deserializer1, deserializer2, deserializer3, bytes, headers, user);

            bytes = serializer2.SerializeAsync(newUser,
                new SerializationContext(MessageComponentType.Value, testTopic, headers)).Result;
            DeserializeAllVersions(deserializer1, deserializer2, deserializer3, bytes, headers, user);

            bytes = serializer3.SerializeAsync(newerUser,
                new SerializationContext(MessageComponentType.Value, testTopic, headers)).Result;
            DeserializeAllVersions(deserializer1, deserializer2, deserializer3, bytes, headers, user);
        }

        private void DeserializeAllVersions(JsonDeserializer<Customer> deserializer1,
            JsonDeserializer<NewCustomer> deserializer2, JsonDeserializer<NewerCustomer> deserializer3,
            byte[] bytes, Headers headers, Customer user)
        {
            var result1 = deserializer1.DeserializeAsync(bytes, false,
                new SerializationContext(MessageComponentType.Value, testTopic, headers)).Result;
            var result2 = deserializer2.DeserializeAsync(bytes, false,
                new SerializationContext(MessageComponentType.Value, testTopic, headers)).Result;
            var result3 = deserializer3.DeserializeAsync(bytes, false,
                new SerializationContext(MessageComponentType.Value, testTopic, headers)).Result;

            Assert.Equal("awesome", result1.Name);
            Assert.Equal(user.FavoriteColor, result1.FavoriteColor);
            Assert.Equal(user.FavoriteNumber, result1.FavoriteNumber);

            Assert.Equal("awesome", result2.FullName);
            Assert.Equal(user.FavoriteColor, result2.FavoriteColor);
            Assert.Equal(user.FavoriteNumber, result2.FavoriteNumber);

            Assert.Equal("awesome", result3.Title);
            Assert.Equal(user.FavoriteColor, result3.FavoriteColor);
            Assert.Equal(user.FavoriteNumber, result3.FavoriteNumber);
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
    
    class NewCustomer
    {
        [JsonProperty("favorite_color")]
        public string FavoriteColor { get; set; }
        [JsonProperty("favorite_number")]
        public int FavoriteNumber { get; set; }
        [JsonProperty("full_name")]
        public string FullName { get; set; }
    }
    
    class NewerCustomer
    {
        [JsonProperty("favorite_color")]
        public string FavoriteColor { get; set; }
        [JsonProperty("favorite_number")]
        public int FavoriteNumber { get; set; }
        [JsonProperty("title")]
        public string Title { get; set; }
    }
}
