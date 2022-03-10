namespace RocketMQ.Client
{
    public class CheckTransactionStateRequestHeader : CommandCustomHeader
    {
        [CFNotNull]
        public long tranStateTableOffset { get; set; }
        [CFNotNull]
        public long commitLogOffset { get; set; }
        public string msgId { get; set; }
        public string transactionId { get; set; }
        public string offsetMsgId { get; set; }

        public void checkFields() { }
    }
}
