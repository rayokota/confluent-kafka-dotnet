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
using System.Runtime.Serialization;
using System.Threading.Tasks;
using Confluent.Kafka;


namespace Confluent.SchemaRegistry
{
    public class Migration
    {
        public Migration(RuleMode ruleMode, Schema source, Schema target)
        {
            RuleMode = ruleMode;
            Source = source;
            Target = target;
        }
        
        public RuleMode RuleMode { get; set; }
        
        public Schema Source { get; set; }
        
        public Schema Target { get; set; }
    }
    
    /// <summary>
    ///     Serde utilities
    /// </summary>
    public static class SerdeUtils
    {
        /// <summary>
        ///     Resolve references
        /// </summary>
        /// <param name="client"></param>
        /// <param name="schema"></param>
        /// <returns></returns>
        public static async Task<IDictionary<string, string>> ResolveReferences(ISchemaRegistryClient client, Schema schema)
        {
            IList<SchemaReference> references = schema.References;
            if (references == null)
            {
                return new Dictionary<string, string>();
            }

            IDictionary<string, string> result = new Dictionary<string, string>();
            ISet<string> visited = new HashSet<string>();
            result = await ResolveReferences(client, schema, result, visited);
            return result;
        }
        
        private static async Task<IDictionary<string, string>> ResolveReferences(
            ISchemaRegistryClient client, Schema schema, IDictionary<string, string> schemas, ISet<string> visited)
        {
            IList<SchemaReference> references = schema.References;
            foreach (SchemaReference reference in references)
            {
                if (visited.Contains(reference.Name))
                {
                    continue;
                }

                visited.Add(reference.Name);
                if (!schemas.ContainsKey(reference.Name))
                {
                    Schema s = await client.GetRegisteredSchemaAsync(reference.Subject, reference.Version)
                        .ConfigureAwait(continueOnCapturedContext: false);
                    if (s == null)
                    {
                        throw new SerializationException("Could not find schema " + reference.Subject + "-" + reference.Version);
                    }
                    schemas[reference.Name] = s.SchemaString;
                    await ResolveReferences(client, s, schemas, visited);
                }
            }

            return schemas;
        }

        public static IList<Migration> GetMigrations(
            ISchemaRegistryClient client, 
            string subject, 
            Schema writerSchema,
            Schema readerSchema)
        {
            RuleMode migrationMode;
            Schema first;
            Schema last;
            IList<Migration> migrations = new List<Migration>();
            if (writerSchema.Version < readerSchema.Version)
            {
                migrationMode = RuleMode.Upgrade;
                first = writerSchema;
                last = readerSchema;
            }
            else if (writerSchema.Version > readerSchema.Version)
            {
                migrationMode = RuleMode.Downgrade;
                first = readerSchema;
                last = writerSchema;
            }
            else
            {
                return migrations;
            }

            IList<Schema> versions = GetSchemasBetween(client, subject, first, last);
            Schema previous = null;
            for (int i = 0; i < versions.Count; i++) {
              Schema current = versions[i];
              if (i == 0) {
                // skip the first version
                previous = current;
                continue;
              }
              if (current.RuleSet != null && current.RuleSet.HasRules(migrationMode)) {
                Migration m;
                if (migrationMode == RuleMode.Upgrade) {
                  m = new Migration(migrationMode, previous, current);
                } else {
                  m = new Migration(migrationMode, current, previous);
                }
                migrations.Add(m);
              }
              previous = current;
            }
            if (migrationMode == RuleMode.Downgrade)
            {
                migrations = migrations.Reverse().ToList();
            }
            return migrations;
        }

        private static IList<Schema> GetSchemasBetween(
            ISchemaRegistryClient client,
            string subject,
            Schema first,
            Schema last)
        {
            if (last.Version - first.Version <= 1)
            {
                return new List<Schema> { first, last };
            }

            int version1 = first.Version;
            int version2 = last.Version;
            IList<Schema> schemas = new List<Schema>();
            schemas.Add(first);
            for (int i = version1 + 1; i < version2; i++) {
                schemas.Add(client.GetRegisteredSchemaAsync(subject, i).Result);
            }
            schemas.Add(last);
            return schemas;
        }
        
        public static RegisteredSchema GetReaderSchema(
            ISchemaRegistryClient client, 
            string subject, 
            IDictionary<string, string> useLatestMetadata, 
            bool useLatestVersion)
        {
            if (client == null)
            {
                return null;
            }
            if (useLatestMetadata != null && useLatestMetadata.Any())
            {
                return client.GetLatestWithMetadataAsync(subject, useLatestMetadata, false).Result;
            }
            if (useLatestVersion)
            {
                return client.GetLatestSchemaAsync(subject).Result;
            }

            return null;
        }
        
        /// <summary>
        ///     Execute rules 
        /// </summary>
        /// <param name="ruleExecutors"></param>
        /// <param name="ruleActions"></param>
        /// <param name="isKey"></param>
        /// <param name="subject"></param>
        /// <param name="topic"></param>
        /// <param name="headers"></param>
        /// <param name="ruleMode"></param>
        /// <param name="source"></param>
        /// <param name="target"></param>
        /// <param name="message"></param>
        /// <returns></returns>
        /// <exception cref="RuleConditionException"></exception>
        /// <exception cref="ArgumentException"></exception>
        public static object ExecuteRules(
            bool isKey, string subject, string topic, Headers headers,
            RuleMode ruleMode, Schema source, Schema target, object message,
            FieldTransformer fieldTransformer)
        {
            if (message == null || target == null)
            {
                return message;
            }

            IList<Rule> rules;
            if (ruleMode == RuleMode.Upgrade)
            {
                rules = target.RuleSet?.MigrationRules;
            }
            else if (ruleMode == RuleMode.Downgrade)
            {
                // Execute downgrade rules in reverse order for symmetry
                rules = source.RuleSet?.MigrationRules.Reverse().ToList();
            }
            else
            {
                rules = target.RuleSet?.DomainRules;
                if (rules != null && ruleMode == RuleMode.Read)
                {
                    // Execute read rules in reverse order for symmetry
                    rules = rules.Reverse().ToList();
                }
            }

            if (rules == null)
            {
                return message;
            }

            for (int i = 0; i < rules.Count; i++)
            {
                Rule rule = rules[i];
                if (rule.Disabled)
                {
                    continue;
                }
                if (rule.Mode == RuleMode.WriteRead)
                {
                    if (ruleMode != RuleMode.Read && ruleMode != RuleMode.Write)
                    {
                        continue;
                    }
                }
                else if (rule.Mode == RuleMode.UpDown)
                {
                    if (ruleMode != RuleMode.Upgrade && ruleMode != RuleMode.Downgrade)
                    {
                        continue;
                    }
                }
                else if (ruleMode != rule.Mode)
                {
                    continue;
                }

                RuleContext ctx = new RuleContext(source, target,
                    subject, topic, headers, isKey, ruleMode, rule, i, rules, fieldTransformer);
                if (RuleRegistry.TryGetRuleExecutor(rule.Type.ToUpper(), out IRuleExecutor ruleExecutor))
                {
                    try
                    {
                        object result = ruleExecutor.Transform(ctx, message);
                        switch (rule.Kind)
                        {
                            case RuleKind.Condition:
                                if (result is bool condition && !condition)
                                {
                                    throw new RuleConditionException(rule);
                                }

                                break;
                            case RuleKind.Transform:
                                message = result;
                                break;
                            default:
                                throw new ArgumentException("Unsupported rule kind " + rule.Kind);
                        }
                        RunAction(ctx, ruleMode, rule, message != null ? rule.OnSuccess : rule.OnFailure,
                            message, null, message != null ? null : ErrorAction.ActionType);
                    }
                    catch (RuleException ex)
                    {
                        RunAction(ctx, ruleMode, rule, rule.OnFailure, message, 
                            ex, ErrorAction.ActionType);
                    }
                }
                else
                {
                    RunAction(ctx, ruleMode, rule, rule.OnFailure, message, 
                        new RuleException("Could not find rule executor of type " + rule.Type), ErrorAction.ActionType);
                }
            }
            return message;
        }

        private static void RunAction(RuleContext ctx, RuleMode ruleMode, 
            Rule rule, string action, object message, RuleException ex, string defaultAction)
        {
            string actionName = GetRuleActionName(rule, ruleMode, action);
            if (actionName == null)
            {
                actionName = defaultAction;
            }
            if (actionName != null)
            {
                IRuleAction ruleAction = GetRuleAction(actionName);
                if (ruleAction == null)
                {
                    throw new SerializationException("Could not find rule action of type " + actionName);
                }

                try
                {
                    ruleAction.Run(ctx, message, ex);
                } catch (RuleException e)
                {
                    throw new SerializationException("Failed to run rule action " + actionName, e);
                }
            }
        }

        private static string GetRuleActionName(Rule rule, RuleMode ruleMode, string actionName)
        {
            if ((rule.Mode == RuleMode.WriteRead || rule.Mode == RuleMode.UpDown)
                && actionName != null
                && actionName.Contains(","))
            {
                String[] parts = actionName.Split(',');
                switch (ruleMode)
                {
                    case RuleMode.Write:
                    case RuleMode.Upgrade:
                        return parts[0];
                    case RuleMode.Read:
                    case RuleMode.Downgrade:
                        return parts[1];
                    default:
                        throw new ArgumentException("Unsupported rule mode " + ruleMode);
                }
            }
            return actionName;
        }

        private static IRuleAction GetRuleAction(string actionName)
        {
            if (actionName == ErrorAction.ActionType)
            {
                return new ErrorAction();
            }
            if (actionName == NoneAction.ActionType)
            {
                return new NoneAction();
            }
            RuleRegistry.TryGetRuleAction(actionName.ToUpper(), out IRuleAction action);
            return action;
        }
    }
}