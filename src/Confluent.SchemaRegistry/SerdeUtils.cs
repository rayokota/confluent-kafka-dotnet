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
using Confluent.Kafka;


namespace Confluent.SchemaRegistry
{
    /// <summary>
    ///     Serde utilities
    /// </summary>
    public static class SerdeUtils
    {
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
            IDictionary<string, IRuleExecutor> ruleExecutors, 
            IDictionary<string, IRuleAction> ruleActions, bool isKey,
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
                    subject, topic, headers, isKey, ruleMode, rule, i, rules);
                if (ruleExecutors.TryGetValue(rule.Type.ToUpper(), out IRuleExecutor ruleExecutor))
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
                        RunAction(ruleActions, ctx, ruleMode, rule, message != null ? rule.OnSuccess : rule.OnFailure,
                            message, null, message != null ? null : ErrorAction.ActionType);
                    }
                    catch (RuleException ex)
                    {
                        RunAction(ruleActions, ctx, ruleMode, rule, rule.OnFailure, message, 
                            ex, ErrorAction.ActionType);
                    }
                }
                else
                {
                    RunAction(ruleActions, ctx, ruleMode, rule, rule.OnFailure, message, 
                        new RuleException("Could not find rule executor of type " + rule.Type), ErrorAction.ActionType);
                }
            }
            return message;
        }

        private static void RunAction(IDictionary<string, IRuleAction> ruleActions, RuleContext ctx, RuleMode ruleMode, 
            Rule rule, string action, object message, RuleException ex, string defaultAction)
        {
            string actionName = GetRuleActionName(rule, ruleMode, action);
            if (actionName == null)
            {
                actionName = defaultAction;
            }
            if (actionName != null)
            {
                IRuleAction ruleAction = GetRuleAction(ruleActions, actionName);
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

        private static IRuleAction GetRuleAction(IDictionary<string, IRuleAction> ruleActions, string actionName)
        {
            if (actionName == ErrorAction.ActionType)
            {
                return new ErrorAction();
            }
            if (actionName == NoneAction.ActionType)
            {
                return new NoneAction();
            }
            ruleActions.TryGetValue(actionName.ToUpper(), out IRuleAction action);
            return action;
        }
    }
}