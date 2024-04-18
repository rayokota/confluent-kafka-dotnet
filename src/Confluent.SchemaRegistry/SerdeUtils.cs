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
using System.Runtime.Serialization;
using Confluent.Kafka;


namespace Confluent.SchemaRegistry
{
    /// <summary>
    ///     Serde utilities
    /// </summary>
    public static class SerdeUtils
    {
        public static object ExecuteRules(
            IDictionary<string, IRuleExecutor> ruleExecutors, bool isKey,
            string subject, string topic, Headers headers,
            RuleMode ruleMode, Schema source, Schema target, object message)
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
                rules = source.RuleSet?.MigrationRules;
            }
            else
            {
                rules = target.RuleSet?.DomainRules;
            }

            if (rules == null)
            {
                return message;
            }

            for (int i = 0; i < rules.Count; i++)
            {
                Rule rule = rules[i];
                if (rule.Mode == RuleMode.ReadWrite)
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
                    subject, topic, headers, isKey, ruleMode, rule, i, rules);
                if (!ruleExecutors.TryGetValue(rule.Type.ToUpper(), out IRuleExecutor ruleExecutor)) 
                {
                    return message;
                }

                message = ruleExecutor.Transform(ctx, message);
                if (message == null)
                {
                    throw new SerializationException("Validation failed for rule " + rule);
                }
            }

            return message;
        }
    }
}