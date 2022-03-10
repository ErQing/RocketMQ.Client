using System;

namespace RocketMQ.Client
{
    public class PullMessageRequestHeader : CommandCustomHeader
    {
        [CFNotNull]
        public string consumerGroup { get; set; }
        [CFNotNull]
        public String topic { get; set; }
        [CFNotNull]
        public int queueId { get; set; }
        [CFNotNull]
        public long queueOffset { get; set; }
        [CFNotNull]
        public int maxMsgNums { get; set; }
        [CFNotNull]
        public int sysFlag { get; set; }
        [CFNotNull]
        public long commitOffset { get; set; }
        [CFNotNull]
        public long suspendTimeoutMillis { get; set; }
        [CFNullable]
        public String subscription { get; set; }
        [CFNotNull]
        public long subVersion { get; set; }
        public String expressionType { get; set; }

        public void checkFields()
        {
        }
    }
}
