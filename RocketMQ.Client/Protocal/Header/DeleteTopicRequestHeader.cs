using System;

namespace RocketMQ.Client
{
    public class DeleteTopicRequestHeader : CommandCustomHeader
    {
        [CFNotNull]
        public string topic { get; set; }

        public void checkFields() { }
    }
}
