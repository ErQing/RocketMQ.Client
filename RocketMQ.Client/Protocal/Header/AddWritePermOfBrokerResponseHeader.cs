namespace RocketMQ.Client
{
    public class AddWritePermOfBrokerResponseHeader : CommandCustomHeader
    {
        [CFNotNull]
        public int addTopicCount { get; set; }

        public void checkFields() { }

    }
}
