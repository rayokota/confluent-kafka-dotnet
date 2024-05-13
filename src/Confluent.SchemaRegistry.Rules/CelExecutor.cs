using Microsoft.Extensions.Logging.Abstractions;

namespace Confluent.SchemaRegistry.Rules
{
    public class CelExecutor : IRuleExecutor
    {
        public static void Register()
        {
            RuleRegistry.RegisterRuleExecutor(new CelExecutor());
        }

        public static readonly string RuleType = "CEL";
        
        public CelExecutor()
        {
        }
        
        public void Configure(IEnumerable<KeyValuePair<string, string>> config)
        {
        }

        public string Type() => RuleType;


        public Task<object> Transform(RuleContext ctx, object message)
        {
            // TODO cache
            object result = null;
            return Task.FromResult(result);
        }

        public void Dispose()
        {
        }
    }
}
