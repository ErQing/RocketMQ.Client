using System;

namespace RocketMQ.Client
{
    public class GetConsumerRunningInfoRequestHeader : CommandCustomHeader
    {
        [CFNotNull]
        public String consumerGroup { get; set; }
        [CFNotNull]
        public String clientId { get; set; }
        [CFNullable]
        public bool jstackEnable { get; set; }

        //@Override
        public void checkFields()
        {
        }
    }
}
