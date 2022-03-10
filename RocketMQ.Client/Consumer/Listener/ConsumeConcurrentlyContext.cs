namespace RocketMQ.Client
{
    public class ConsumeConcurrentlyContext
    {
        private MessageQueue messageQueue;
        /**
         * Message consume retry strategy<br>
         * -1,no retry,put into DLQ directly<br>
         * 0,broker control retry frequency<br>
         * >0,client control retry frequency
         */
        private int delayLevelWhenNextConsume = 0;
        private int ackIndex = int.MaxValue;

        public ConsumeConcurrentlyContext(MessageQueue messageQueue)
        {
            this.messageQueue = messageQueue;
        }

        public int getDelayLevelWhenNextConsume()
        {
            return delayLevelWhenNextConsume;
        }

        public void setDelayLevelWhenNextConsume(int delayLevelWhenNextConsume)
        {
            this.delayLevelWhenNextConsume = delayLevelWhenNextConsume;
        }

        public MessageQueue getMessageQueue()
        {
            return messageQueue;
        }

        public int getAckIndex()
        {
            return ackIndex;
        }

        public void setAckIndex(int ackIndex)
        {
            this.ackIndex = ackIndex;
        }

    }
}
