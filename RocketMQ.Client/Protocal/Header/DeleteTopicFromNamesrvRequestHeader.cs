using System;

namespace RocketMQ.Client
{
    public class DeleteTopicFromNamesrvRequestHeader : CommandCustomHeader
    {
        [CFNotNull]
        public string topic { get; set; }

        public void checkFields() { }
    }
}
