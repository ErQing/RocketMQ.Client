using System;

namespace RocketMQ.Client
{
    public class SearchOffsetRequestHeader : CommandCustomHeader
    {
        [CFNotNull]
        public string topic { get; set; }
        [CFNotNull]
        public int queueId { get; set; }
        [CFNotNull]
        public long timestamp { get; set; }

        //@Override
        public void checkFields()
        {

        }
    }
}
