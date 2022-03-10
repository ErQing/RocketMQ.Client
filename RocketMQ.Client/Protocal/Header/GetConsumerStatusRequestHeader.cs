using System;

namespace RocketMQ.Client
{
    public class GetConsumerStatusRequestHeader : CommandCustomHeader
    {
        [CFNotNull]
        public String topic { get; set; }
        [CFNotNull]
        public String group { get; set; }
        [CFNullable]
        public String clientAddr { get; set; }

        public void checkFields() { }
    }
}
