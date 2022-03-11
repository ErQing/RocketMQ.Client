using System;

namespace RocketMQ.Client
{
    public class GetConsumerStatusRequestHeader : CommandCustomHeader
    {
        [CFNotNull]
        public string topic { get; set; }
        [CFNotNull]
        public string group { get; set; }
        [CFNullable]
        public string clientAddr { get; set; }

        public void checkFields() { }
    }
}
