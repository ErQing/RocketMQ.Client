using System;

namespace RocketMQ.Client
{
    public class ConsumeMessageDirectlyResultRequestHeader : CommandCustomHeader
    {
        [CFNotNull]
        public string consumerGroup { get; set; }
        [CFNullable]
        public string clientId { get; set; }
        [CFNullable]
        public string msgId { get; set; }
        [CFNullable]
        public string brokerName { get; set; }

        public void checkFields() { }
    }
}
