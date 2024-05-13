using Jsonata.Net.Native;
using Jsonata.Net.Native.Json;
using Jsonata.Net.Native.JsonNet;

namespace Confluent.SchemaRegistry.Rules
{
    public class JsonataExecutor : IRuleExecutor
    {
        public static void Register()
        {
            RuleRegistry.RegisterRuleExecutor(new JsonataExecutor());
        }

        public static readonly string RuleType = "JSONATA";
        
        public JsonataExecutor()
        {
        }
        
        public void Configure(IEnumerable<KeyValuePair<string, string>> config)
        {
        }

        public string Type() => RuleType;


        public Task<object> Transform(RuleContext ctx, object message)
        {
            // TODO cache
            JToken jsonObj = JsonataExtensions.FromNewtonsoft((Newtonsoft.Json.Linq.JToken)message);
            JsonataQuery query = new JsonataQuery(ctx.Rule.Expr);
            JToken jtoken = query.Eval(jsonObj);
            object result = JsonataExtensions.ToNewtonsoft(jtoken);
            return Task.FromResult(result);
        }

        public void Dispose()
        {
        }
    }
}
