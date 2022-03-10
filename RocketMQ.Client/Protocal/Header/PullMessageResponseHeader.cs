namespace RocketMQ.Client
{
    public class PullMessageResponseHeader : CommandCustomHeader
    {
        [CFNotNull]
        public long suggestWhichBrokerId { get; set; }
        [CFNotNull]
        public long nextBeginOffset { get; set; }
        [CFNotNull]
        public long minOffset { get; set; }
        [CFNotNull]
        public long maxOffset { get; set; }

        //@Override
        public void checkFields()
        {
        }
    }
}
