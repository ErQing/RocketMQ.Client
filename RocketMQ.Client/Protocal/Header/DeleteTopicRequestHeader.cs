using System;

namespace RocketMQ.Client
{
    public class DeleteTopicRequestHeader : CommandCustomHeader
    {
        [CFNotNull]
        public String topic { get; set; }

        public void checkFields() { }
    }
}
