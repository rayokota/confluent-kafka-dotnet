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
using Confluent.Kafka;

namespace Confluent.SchemaRegistry
{
    /// <summary>
    ///     A rule context.
    /// </summary>
    public class RuleContext
    {
        public Schema Source { get; set; }

        public Schema Target { get; set; }

        public string Subject { get; set; }

        public string Topic { get; set; }

        public Headers Headers { get; set; }

        public bool IsKey { get; set; }

        public RuleMode RuleMode { get; set; }

        public Rule Rule { get; set; }

        public IDictionary<object, object> CustomData { get; } = new Dictionary<object, object>();

        private Stack<FieldContext> fieldContexts = new Stack<FieldContext>();

        public RuleContext(Schema source, Schema target, string subject, string topic,
            Headers headers, bool isKey, RuleMode ruleMode, Rule rule)
        {
            Source = source;
            Target = target;
            Subject = subject;
            Topic = topic;
            Headers = headers;
            IsKey = isKey;
            RuleMode = ruleMode;
            Rule = rule;
        }

        internal ISet<string> getTags(string fullName)
        {
            ISet<string> tags = new HashSet<string>();
            Metadata metadata = Target.Metadata;
            if (metadata != null && metadata.Tags != null)
            {
                foreach (var entry in metadata.Tags)
                {
                    if (WildcardMatcher.Match(fullName, entry.Key))
                    {
                        tags.UnionWith(entry.Value);
                    } 
                }
            }

            return tags;
        }


        public FieldContext CurrentField()
        {
            return fieldContexts.Count != 0 ? fieldContexts.Peek() : null;
        }

        public FieldContext EnterField(object containingMessage,
            string fullName, string name, Type type, ISet<string> tags)
        {
            ISet<string> allTags = new HashSet<string>(tags);
            allTags.UnionWith(getTags(fullName));
            return new FieldContext(this, containingMessage, fullName, name, type, allTags);
        }

        public class FieldContext : IDisposable
        {
            public RuleContext RuleContext { get; set; }

            public object ContainingMessage { get; set; }

            public string FullName { get; set; }

            public string Name { get; set; }

            public Type Type { get; set; }

            public ISet<string> Tags { get; set; }

            public FieldContext(RuleContext ruleContext, object containingMessage, string fullName, string name,
                Type type, ISet<string> tags)
            {
                RuleContext = ruleContext;
                ContainingMessage = containingMessage;
                FullName = fullName;
                Name = name;
                Type = type;
                Tags = tags;
                RuleContext.fieldContexts.Push(this);
            }

            public void Dispose()
            {
                RuleContext.fieldContexts.Pop();
            }
        }

        public enum Type
        {
            Record,
            Enum,
            Array,
            Map,
            Combined,
            Fixed,
            String,
            Bytes,
            Int,
            Long,
            Float,
            Double,
            Boolean,
            Null
        }
    }
}