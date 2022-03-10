namespace RocketMQ.Client
{
    public class ConsumerSendMsgBackRequestHeader : CommandCustomHeader
    {
        [CFNotNull]
        public long offset { get; set; }
        [CFNotNull]
        public string group { get; set; }
        [CFNotNull]
        public int delayLevel { get; set; }
        public string originMsgId { get; set; }
        public string originTopic { get; set; }
        [CFNullable]
        public bool unitMode { get; set; } = false;
        public int maxReconsumeTimes { get; set; }

        public void checkFields() { }

        public override string ToString()
        {
            return "ConsumerSendMsgBackRequestHeader [group=" + group + ", originTopic=" + originTopic + ", originMsgId=" + originMsgId
                + ", delayLevel=" + delayLevel + ", unitMode=" + unitMode + ", maxReconsumeTimes=" + maxReconsumeTimes + "]";
        }
    }
}
