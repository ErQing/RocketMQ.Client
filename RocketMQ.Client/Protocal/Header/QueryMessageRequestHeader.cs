using System;

namespace RocketMQ.Client
{
    public class QueryMessageRequestHeader : CommandCustomHeader
    {
        [CFNotNull]
        public String topic { get; set; }
        [CFNotNull]
        public String key { get; set; }
        [CFNotNull]
        public int maxNum { get; set; }
        [CFNotNull]
        public long beginTimestamp { get; set; }
        [CFNotNull]
        public long endTimestamp { get; set; }

        public void checkFields()
        {
        }
    }
}
