using System;

namespace RocketMQ.Client
{
    public class GetConsumerRunningInfoRequestHeader : CommandCustomHeader
    {
        [CFNotNull]
        public string consumerGroup { get; set; }
        [CFNotNull]
        public string clientId { get; set; }
        [CFNullable]
        public bool jstackEnable { get; set; }

        //@Override
        public void checkFields()
        {
        }
    }
}
