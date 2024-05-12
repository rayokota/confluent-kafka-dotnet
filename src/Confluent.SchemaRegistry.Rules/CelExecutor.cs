namespace Confluent.SchemaRegistry.Rules
{
    public class CelExecutor : IRuleExecutor
    {
        public static void Register()
        {
            RuleRegistry.RegisterRuleExecutor(RuleType, new CelExecutor());
        }

        public static readonly string RuleType = "CEL";
        
        public CelExecutor()
        {
        }
        
        public void Configure(IEnumerable<KeyValuePair<string, string>> config)
        {
        }

        public string Type() => RuleType;


        public object Transform(RuleContext ctx, object message)
        {
            // TODO cache
            return null;
        }

        public void Dispose()
        {
        }
    }
}
