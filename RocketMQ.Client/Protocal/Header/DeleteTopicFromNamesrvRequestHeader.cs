using System;

namespace RocketMQ.Client
{
    public class DeleteTopicFromNamesrvRequestHeader : CommandCustomHeader
    {
        [CFNotNull]
        public String topic { get; set; }

        public void checkFields() { }
    }
}
