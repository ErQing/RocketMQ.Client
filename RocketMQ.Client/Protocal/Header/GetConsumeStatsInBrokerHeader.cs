namespace RocketMQ.Client
{
    public class GetConsumeStatsInBrokerHeader : CommandCustomHeader
    {
        [CFNotNull]
        public bool isOrder { get; set; }

        public void checkFields() { }
    }
}
