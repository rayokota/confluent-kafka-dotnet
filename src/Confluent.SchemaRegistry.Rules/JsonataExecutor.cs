using Jsonata.Net.Native;
using Jsonata.Net.Native.Json;
using Jsonata.Net.Native.JsonNet;

namespace Confluent.SchemaRegistry.Rules
{
    public class JsonataExecutor : IRuleExecutor
    {
        public static void Register()
        {
            RuleRegistry.RegisterRuleExecutor(RuleType, new JsonataExecutor());
        }

        public static readonly string RuleType = "JSONATA";
        
        public JsonataExecutor()
        {
        }
        
        public void Configure(IEnumerable<KeyValuePair<string, string>> config)
        {
        }

        public string Type() => RuleType;


        public object Transform(RuleContext ctx, object message)
        {
            // TODO cache
            JToken jsonObj = JsonataExtensions.FromNewtonsoft((Newtonsoft.Json.Linq.JToken)message);
            JsonataQuery query = new JsonataQuery(ctx.Rule.Expr);
            JToken result = query.Eval(jsonObj);
            return JsonataExtensions.ToNewtonsoft(result);
        }

        public void Dispose()
        {
        }
    }
}
