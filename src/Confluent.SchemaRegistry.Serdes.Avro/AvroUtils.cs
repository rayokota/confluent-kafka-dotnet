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

using System;
using System.Collections.Generic;
using System.Linq;
using Avro;
using Avro.Generic;
using Avro.Specific;


namespace Confluent.SchemaRegistry.Serdes
{
    /// <summary>
    ///     Avro utilities
    /// </summary>
    public static class AvroUtils
    {
        public static object Transform(RuleContext ctx, Avro.Schema schema, object message,
            FieldTransform fieldTransform)
        {
            if (schema == null || message == null)
            {
                return message;
            }

            IUnionResolver writer;
            switch (schema.Tag)
            {
                case Avro.Schema.Type.Union:
                    writer = GetResolver(schema, message);
                    UnionSchema us = (UnionSchema)schema;
                    int unionIndex = writer.Resolve(us, message);
                    return Transform(ctx, us[unionIndex], message, fieldTransform);
                case Avro.Schema.Type.Array:
                    ArraySchema a = (ArraySchema)schema;
                    return ((IList<object>)message)
                        .Select(it => Transform(ctx, a.ItemSchema, it, fieldTransform))
                        .ToList();
                case Avro.Schema.Type.Map:
                    MapSchema ms = (MapSchema)schema;
                    return ((IDictionary<object, object>)message)
                        .Select(it =>
                            new KeyValuePair<object, object>(it.Key,
                                Transform(ctx, ms.ValueSchema, it.Value, fieldTransform)))
                        .ToDictionary(it => it.Key, it => it.Value);
                case Avro.Schema.Type.Record:
                    RecordSchema rs = (RecordSchema)schema;
                    foreach (Field f in rs.Fields)
                    {
                        string fullName = rs.Fullname + "." + f.Name;
                        using (ctx.EnterField(ctx, message, fullName, f.Name, GetType(f), GetInlineAnnotations(f)))
                        {
                            if (message is ISpecificRecord)
                            {
                                ISpecificRecord specificRecord = (ISpecificRecord)message;
                                object value = specificRecord.Get(f.Pos);
                                object newValue = Transform(ctx, f.Schema, value, fieldTransform);
                                specificRecord.Put(f.Pos, newValue);
                            }
                            else if (message is GenericRecord)
                            {
                                GenericRecord genericRecord = (GenericRecord)message;
                                object value = genericRecord.GetValue(f.Pos);
                                object newValue = Transform(ctx, f.Schema, value, fieldTransform);
                                genericRecord.Add(f.Pos, newValue);
                            }
                            else
                            {
                                throw new ArgumentException("Unhandled field value of type " + message.GetType());
                            }
                        }
                    }

                    return message;
                default:
                    RuleContext.FieldContext fieldContext = ctx.CurrentField();
                    if (fieldContext != null)
                    {
                        ISet<string> intersect = new HashSet<string>(fieldContext.Annotations);
                        intersect.IntersectWith(ctx.Rule.Annotations);
                        if (intersect.Count != 0)
                        {
                            return fieldTransform.Invoke(ctx, fieldContext, message);
                        }
                    }

                    return message;
            }
        }

        private static RuleContext.Type GetType(Field field)
        {
            switch (field.Schema.Tag)
            {
                case Avro.Schema.Type.Record:
                    return RuleContext.Type.Record;
                case Avro.Schema.Type.Enumeration:
                    return RuleContext.Type.Enum;
                case Avro.Schema.Type.Array:
                    return RuleContext.Type.Array;
                case Avro.Schema.Type.Map:
                    return RuleContext.Type.Map;
                case Avro.Schema.Type.Union:
                    return RuleContext.Type.Combined;
                case Avro.Schema.Type.Fixed:
                    return RuleContext.Type.Fixed;
                case Avro.Schema.Type.String:
                    return RuleContext.Type.String;
                case Avro.Schema.Type.Bytes:
                    return RuleContext.Type.Bytes;
                case Avro.Schema.Type.Int:
                    return RuleContext.Type.Int;
                case Avro.Schema.Type.Long:
                    return RuleContext.Type.Long;
                case Avro.Schema.Type.Float:
                    return RuleContext.Type.Float;
                case Avro.Schema.Type.Double:
                    return RuleContext.Type.Double;
                case Avro.Schema.Type.Boolean:
                    return RuleContext.Type.Boolean;
                case Avro.Schema.Type.Null:
                default:
                    return RuleContext.Type.Null;
            }
        }

        private static ISet<string> GetInlineAnnotations(Field field)
        {
            ISet<string> annotations = new HashSet<string>();
            // TODO RULES
            /*
            if (fd.getOptions().hasExtension(MetaProto.fieldMeta))
            {
                Meta meta = fd.getOptions().getExtension(MetaProto.fieldMeta);
                annotations.addAll(meta.getAnnotationList());
            }
            */
            return annotations;
        }

        private static IUnionResolver GetResolver(Avro.Schema schema, object message)
        {
            if (message is ISpecificRecord)
            {
                return new AvroSpecificWriter(schema);
            }
            else
            {
                return new AvroGenericWriter(schema);
            }
        }

        private interface IUnionResolver
        {
            int Resolve(UnionSchema us, object obj);
        }

        private class AvroSpecificWriter : SpecificDefaultWriter, IUnionResolver
        {
            public AvroSpecificWriter(Avro.Schema schema) : base(schema)
            {
            }
            
            public int Resolve(UnionSchema us, object obj)
            {
                for (int i = 0; i < us.Count; i++)
                {
                    if (Matches(us[i], obj)) return i;
                }
                throw new AvroException("Cannot find a match for " + obj.GetType() + " in " + us);
            }
        }
        
        private class AvroGenericWriter : DefaultWriter, IUnionResolver
        {
            public AvroGenericWriter(Avro.Schema schema) : base(schema)
            {
            }
            
            public int Resolve(UnionSchema us, object obj)
            {
                for (int i = 0; i < us.Count; i++)
                {
                    if (Matches(us[i], obj)) return i;
                }
                throw new AvroException("Cannot find a match for " + obj.GetType() + " in " + us);
            }
        }
    }
}