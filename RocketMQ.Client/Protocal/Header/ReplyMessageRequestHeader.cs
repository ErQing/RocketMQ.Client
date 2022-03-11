using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RocketMQ.Client
{
    public class ReplyMessageRequestHeader : CommandCustomHeader
    {
        [CFNotNull]
        public string producerGroup { get; set; }
        [CFNotNull]
        public string topic { get; set; }
        [CFNotNull]
        public string defaultTopic { get; set; }
        [CFNotNull]
        public int defaultTopicQueueNums { get; set; }
        [CFNotNull]
        public int queueId { get; set; }
        [CFNotNull]
        public int sysFlag { get; set; }
        [CFNotNull]
        public long bornTimestamp { get; set; }
        [CFNotNull]
        public int flag { get; set; }
        [CFNullable]
        public string properties { get; set; }
        [CFNullable]
        public int reconsumeTimes { get; set; }
        [CFNullable]
        public bool unitMode { get; set; } = false;

        [CFNotNull]
        public string bornHost { get; set; }
        [CFNotNull]
        public string storeHost { get; set; }
        [CFNotNull]
        public long storeTimestamp { get; set; }

        public void checkFields()
        {
        }
    }
}
