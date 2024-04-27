// Copyright 2022 Confluent Inc.
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

extern alias ProtobufNet;

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using Confluent.SchemaRegistry.Serdes.Protobuf;
using Google.Protobuf;
using Google.Protobuf.Reflection;
using ProtobufNet::ProtoBuf.Reflection;
using IFileSystem = ProtobufNet::Google.Protobuf.Reflection.IFileSystem;
using FileDescriptorSet = ProtobufNet::Google.Protobuf.Reflection.FileDescriptorSet;
using FileDescriptorProto = ProtobufNet::Google.Protobuf.Reflection.FileDescriptorProto;
using DescriptorProto = ProtobufNet::Google.Protobuf.Reflection.DescriptorProto;
using FieldDescriptorProto = ProtobufNet::Google.Protobuf.Reflection.FieldDescriptorProto;


namespace Confluent.SchemaRegistry.Serdes
{
    /// <summary>
    ///     Protobuf utilities
    /// </summary>
    public static class ProtobufUtils
    {
        public static IDictionary<string, string> BuiltIns = new Dictionary<string, string>
        {
            { "confluent/meta.proto", GetResource("confluent.meta.proto") },
            { "confluent/type/decimal.proto", GetResource("confluent.type.decimal.proto") },
            { "google/type/calendar_period.proto", GetResource("google.type.calendar_period.proto") },
            { "google/type/color.proto", GetResource("google.type.color.proto") },
            { "google/type/date.proto", GetResource("google.type.date.proto") },
            { "google/type/datetime.proto", GetResource("google.type.datetime.proto") },
            { "google/type/dayofweek.proto", GetResource("google.type.dayofweek.proto") },
            { "google/type/expr.proto", GetResource("google.type.expr.proto") },
            { "google/type/fraction.proto", GetResource("google.type.fraction.proto") },
            { "google/type/latlng.proto", GetResource("google.type.latlng.proto") },
            { "google/type/money.proto", GetResource("google.type.money.proto") },
            { "google/type/month.proto", GetResource("google.type.month.proto") },
            { "google/type/postal_address.proto", GetResource("google.type.postal_address.proto") },
            { "google/type/quaternion.proto", GetResource("google.type.quaternion.proto") },
            { "google/type/timeofday.proto", GetResource("google.type.timeofday.proto") },
            { "google/protobuf/any.proto", GetResource("google.protobuf.any.proto") },
            { "google/protobuf/api.proto", GetResource("google.protobuf.api.proto") },
            { "google/protobuf/descriptor.proto", GetResource("google.protobuf.descriptor.proto") },
            { "google/protobuf/duration.proto", GetResource("google.protobuf.duration.proto") },
            { "google/protobuf/empty.proto", GetResource("google.protobuf.empty.proto") },
            { "google/protobuf/field_mask.proto", GetResource("google.protobuf.field_mask.proto") },
            { "google/protobuf/source_context.proto", GetResource("google.protobuf.source_context.proto") },
            { "google/protobuf/struct.proto", GetResource("google.protobuf.struct.proto") },
            { "google/protobuf/timestamp.proto", GetResource("google.protobuf.timestamp.proto") },
            { "google/protobuf/type.proto", GetResource("google.protobuf.type.proto") },
            { "google/protobuf/wrappers.proto", GetResource("google.protobuf.wrappers.proto") }
        };
        
        private static string GetResource(string resourceName)
        {
            var info = Assembly.GetExecutingAssembly().GetName();
            var name = info.Name;
            var stream = Assembly
                .GetExecutingAssembly()
                .GetManifestResourceStream($"{name}.proto.{resourceName}");
            var streamReader = new StreamReader(stream, Encoding.UTF8);
            return streamReader.ReadToEnd();
        }

        public static object Transform(RuleContext ctx, object desc, object message,
            FieldTransform fieldTransform)
        {
            if (desc == null || message == null)
            {
                return message;
            }

            RuleContext.FieldContext fieldContext = ctx.CurrentField();
            
            if (message.GetType().IsGenericType &&
                (message.GetType().GetGenericTypeDefinition().IsAssignableFrom(typeof(List<>)) ||
                 message.GetType().GetGenericTypeDefinition().IsAssignableFrom(typeof(IList<>))))
            {
                IList<object> list = (IList<object>)message;
                return list.Select(it => Transform(ctx, desc, it, fieldTransform)).ToList();
            }
            else if (message.GetType().IsGenericType &&
                     (message.GetType().GetGenericTypeDefinition().IsAssignableFrom(typeof(Dictionary<,>)) ||
                      message.GetType().GetGenericTypeDefinition().IsAssignableFrom(typeof(IDictionary<,>))))
            {
                return message;
            }
            else if (message is IMessage)
            {
                IMessage copy = Copy((IMessage)message);
                string messageFullName = copy.Descriptor.FullName;
                if (!messageFullName.StartsWith("."))
                {
                    messageFullName = "." + messageFullName;
                }

                DescriptorProto messageType = FindMessageByName(desc, messageFullName);
                foreach (FieldDescriptor fd in copy.Descriptor.Fields.InDeclarationOrder())
                {
                    FieldDescriptorProto schemaFd = FindFieldByName(messageType, fd.Name);
                    using (ctx.EnterField(copy, fd.FullName, fd.Name, GetType(fd), GetInlineTags(schemaFd)))
                    {
                        object value = fd.Accessor.GetValue(copy);
                        DescriptorProto d = messageType;
                        if (value is IMessage)
                        {
                            // Pass the schema-based descriptor which has the metadata
                            d = schemaFd.GetMessageType();
                        }

                        object newValue = Transform(ctx, d, value, fieldTransform);
                        if (ctx.Rule.Kind == RuleKind.Condition)
                        {
                            if (newValue is bool b && !b)
                            {
                                throw new RuleConditionException(ctx.Rule);
                            }
                        }
                        else
                        {
                            fd.Accessor.SetValue(copy, newValue);
                        }
                    }
                }

                return copy;
            }
            else
            {
                if (fieldContext != null)
                {
                    ISet<string> intersect = new HashSet<string>(fieldContext.Tags);
                    intersect.IntersectWith(ctx.Rule.Tags);
                    if (intersect.Count != 0)
                    {
                        return fieldTransform.Invoke(ctx, fieldContext, message);
                    }
                }

                return message;
            }
        }

        private static DescriptorProto FindMessageByName(object desc, string messageFullName)
        {
            if (desc is FileDescriptorSet)
            {
                foreach (var file in ((FileDescriptorSet)desc).Files)
                {
                    foreach (var messageType in file.MessageTypes)
                    {
                        return FindMessageByName(messageType, messageFullName);
                    }
                }
            }
            else if (desc is DescriptorProto)
            {
                DescriptorProto messageType = (DescriptorProto)desc;
                if (messageType.GetFullyQualifiedName().Equals(messageFullName))
                {
                    return messageType;
                }

                foreach (DescriptorProto nestedType in messageType.NestedTypes)
                {
                    return FindMessageByName(nestedType, messageFullName);
                }
            }
            return null;
        }

        private static FieldDescriptorProto FindFieldByName(DescriptorProto desc, string fieldName)
        {
            foreach (FieldDescriptorProto fd in desc.Fields)
            {
                if (fd.Name.Equals(fieldName))
                {
                    return fd;
                }
            }

            return null;
        }

        private static IMessage Copy(IMessage message)
        {
            var builder = (IMessage)Activator.CreateInstance(message.GetType());
            builder.MergeFrom(message.ToByteArray());
            return builder;
        }

        private static RuleContext.Type GetType(FieldDescriptor field)
        {
            if (field.IsMap)
            {
                return RuleContext.Type.Map;
            }

            switch (field.FieldType)
            {
                case FieldType.Message:
                    return RuleContext.Type.Record;
                case FieldType.Enum:
                    return RuleContext.Type.Enum;
                case FieldType.String:
                    return RuleContext.Type.String;
                case FieldType.Bytes:
                    return RuleContext.Type.Bytes;
                case FieldType.Int32:
                case FieldType.UInt32:
                case FieldType.Fixed32:
                case FieldType.SFixed32:
                    return RuleContext.Type.Int;
                case FieldType.Int64:
                case FieldType.UInt64:
                case FieldType.Fixed64:
                case FieldType.SFixed64:
                    return RuleContext.Type.Long;
                case FieldType.Float:
                    return RuleContext.Type.Float;
                case FieldType.Double:
                    return RuleContext.Type.Double;
                case FieldType.Bool:
                    return RuleContext.Type.Boolean;
                default:
                    return RuleContext.Type.Null;
            }
        }

        private static ISet<string> GetInlineTags(FieldDescriptorProto fd)
        {
            ISet<string> tags = new HashSet<string>();
            /*
            if (fd.Options.hasExtension(MetaProto.fieldMeta))
            {
                Meta meta = fd.getOptions().getExtension(MetaProto.fieldMeta);
                annotations.addAll(meta.getAnnotationList());
            }
            */
            return tags;
        }

        public static FileDescriptorSet Parse(string schema, IDictionary<string, string> imports)
        {
            IDictionary<string, string> allImports = new Dictionary<string, string>(BuiltIns);
            imports?.ToList().ForEach(x => allImports.Add(x.Key, x.Value));
            
            var fds = new FileDescriptorSet();
            fds.FileSystem = new ProtobufImports(allImports);
            
            fds.Add("__root.proto", true, new StringReader(schema));
            foreach (KeyValuePair<string, string> import in allImports)
            {
                fds.AddImportPath(import.Key);
                
            }
            fds.Process();
            return fds;
        } 
        
        class ProtobufImports : IFileSystem
        {
            protected IDictionary<string, string> Imports { get; set; }

            public ProtobufImports(IDictionary<string, string> imports)
            {
                Imports = imports;
            }

            public bool Exists(string path)
            {
                return Imports.ContainsKey(path);
            }

            public TextReader OpenText(string path)
            {
                return new StringReader(Imports[path]);
            }
        }
    }
}