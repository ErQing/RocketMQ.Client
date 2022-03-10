
namespace RocketMQ.Client
{
    public enum ConsumeType
    {
        UNKNOWN,
        CONSUME_ACTIVELY,
        CONSUME_PASSIVELY
        //public static readonly string CONSUME_ACTIVELY = "PULL";

        //public static readonly string CONSUME_PASSIVELY = "PUSH";

        //private string typeCN;

        //public ConsumeType(string typeCN)
        //{
        //    this.typeCN = typeCN;
        //}

        //public string getTypeCN()
        //{
        //    return typeCN;
        //}
    }
}
