using System;

namespace RocketMQ.Client
{
    public class ConsumeMessageDirectlyResultRequestHeader : CommandCustomHeader
    {
        [CFNotNull]
        public String consumerGroup { get; set; }
        [CFNullable]
        public String clientId { get; set; }
        [CFNullable]
        public String msgId { get; set; }
        [CFNullable]
        public String brokerName { get; set; }

        public void checkFields() { }
    }
}
