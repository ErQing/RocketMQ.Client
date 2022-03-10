using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RocketMQ.Client
{
    public class SendMessageRequestHeader : CommandCustomHeader
    {
        [CFNotNull]
        public String producerGroup { get; set; }
        [CFNotNull]
        public String topic { get; set; }
        [CFNotNull]
        public String defaultTopic { get; set; }
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
        public String properties { get; set; }
        [CFNullable]
        public int reconsumeTimes { get; set; }
        [CFNullable]
        public bool unitMode { get; set; } = false;
        [CFNullable]
        public bool batch { get; set; } = false;
        public int maxReconsumeTimes { get; set; }

        public void checkFields()
        {
        }
    }
}
