namespace RocketMQ.Client
{
    public class AddWritePermOfBrokerRequestHeader : CommandCustomHeader
    {
        [CFNotNull]
        public string brokerName { get; set; }

        public void checkFields() { }
    }
}
