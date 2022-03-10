using System;

namespace RocketMQ.Client
{
    public class GetKVConfigResponseHeader : CommandCustomHeader
    {
        [CFNullable]
        public string value { get; set; }

        public void checkFields() { }
    }
}
