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

using System;
using Moq;
using Xunit;
using System.Collections.Generic;
using System.Linq;
using Confluent.Kafka;
using Confluent.SchemaRegistry.Encryption;
using Example;


namespace Confluent.SchemaRegistry.Serdes.UnitTests
{
    public class ProtobufSerializeDeserialzeTests
    {
        private ISchemaRegistryClient schemaRegistryClient;
        private IDekRegistryClient dekRegistryClient;
        private string testTopic;
        private Dictionary<string, int> store = new Dictionary<string, int>();
        private Dictionary<string, RegisteredSchema> subjectStore = new Dictionary<string, RegisteredSchema>();
        private IDictionary<KekId, RegisteredKek> kekStore = new Dictionary<KekId, RegisteredKek>();
        private IDictionary<DekId, RegisteredDek> dekStore = new Dictionary<DekId, RegisteredDek>();

        public ProtobufSerializeDeserialzeTests()
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
        public void ParseSchema()
        {
            string schema = @"syntax = ""proto3"";
            package io.confluent.kafka.serializers.protobuf.test;

            import ""ref.proto"";
            import ""confluent/meta.proto"";

            message ReferrerMessage {

                string root_id = 1 [(.confluent.field_meta) = { annotation: ""PII"" }];
                ReferencedMessage ref = 2 [(.confluent.field_meta).annotation = ""PII""];

            }";
            
            string import = @"syntax = ""proto3"";
            package io.confluent.kafka.serializers.protobuf.test;

            message ReferencedMessage {
                string ref_id = 1;
                bool is_active = 2;
            }
            ";

            IDictionary<string, string> imports = new Dictionary<string, string>();
            imports["ref.proto"] = import;

            var fds = ProtobufUtils.Parse(schema, imports);
            foreach (var file in fds.Files)
            {
                foreach (var messageType in file.MessageTypes)
                {
                    Assert.Equal("ReferrerMessage", messageType.Name);
                }
            }
        }

        [Fact]
        public void Null()
        {
            var protoSerializer = new ProtobufSerializer<UInt32Value>(schemaRegistryClient);
            var protoDeserializer = new ProtobufDeserializer<UInt32Value>(schemaRegistryClient);

            var bytes = protoSerializer.SerializeAsync(null, new SerializationContext(MessageComponentType.Value, testTopic)).Result;
            Assert.Null(bytes);
            Assert.Null(protoDeserializer.DeserializeAsync(bytes, true, new SerializationContext(MessageComponentType.Value, testTopic)).Result);
        }

        [Fact]
        public void UInt32SerDe()
        {
            var protoSerializer = new ProtobufSerializer<UInt32Value>(schemaRegistryClient);
            var protoDeserializer = new ProtobufDeserializer<UInt32Value>();

            var v = new UInt32Value { Value = 1234 };
            var bytes = protoSerializer.SerializeAsync(v, new SerializationContext(MessageComponentType.Value, testTopic)).Result;
            Assert.Equal(v.Value, protoDeserializer.DeserializeAsync(bytes, false, new SerializationContext(MessageComponentType.Value, testTopic)).Result.Value);
        }

        [Fact]
        public void FieldEncryption()
        {
            string schemaStr = @"syntax = ""proto3"";
            import ""confluent/meta.proto"";

            package example;

            message Person {
                string favorite_color = 1;
                int32 favorite_number = 2;
                string name = 3 [(.confluent.field_meta) = { tags: ""PII"" }];
            }";
            
            var schema = new RegisteredSchema("topic-value", 1, 1, schemaStr, SchemaType.Protobuf, null);
            schema.Metadata = new Metadata(new Dictionary<string, ISet<string>>
                {
                    ["example.Person.name"] = new HashSet<string> { "PII" }

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
            var config = new ProtobufSerializerConfig
            {
                AutoRegisterSchemas = false,
                UseLatestVersion = true
            };
            config.Set("rules.secret", "mysecret");
            var serializer = new ProtobufSerializer<Person>(schemaRegistryClient, config);
            var deserializer = new ProtobufDeserializer<Person>(schemaRegistryClient, null);

            var user = new Person
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
}
