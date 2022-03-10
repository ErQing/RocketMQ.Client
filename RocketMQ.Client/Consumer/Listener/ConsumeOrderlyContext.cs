namespace RocketMQ.Client
{
    public class ConsumeOrderlyContext
    {
        private readonly MessageQueue messageQueue;
        private bool autoCommit = true;
        private long suspendCurrentQueueTimeMillis = -1;

        public ConsumeOrderlyContext(MessageQueue messageQueue)
        {
            this.messageQueue = messageQueue;
        }

        public bool isAutoCommit()
        {
            return autoCommit;
        }

        public void setAutoCommit(bool autoCommit)
        {
            this.autoCommit = autoCommit;
        }

        public MessageQueue getMessageQueue()
        {
            return messageQueue;
        }

        public long getSuspendCurrentQueueTimeMillis()
        {
            return suspendCurrentQueueTimeMillis;
        }

        public void setSuspendCurrentQueueTimeMillis(long suspendCurrentQueueTimeMillis)
        {
            this.suspendCurrentQueueTimeMillis = suspendCurrentQueueTimeMillis;
        }
    }
}
