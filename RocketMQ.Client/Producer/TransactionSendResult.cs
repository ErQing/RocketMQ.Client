namespace RocketMQ.Client
{
    public class TransactionSendResult : SendResult
    {
        private LocalTransactionState localTransactionState;

        public TransactionSendResult()
        {
        }

        public LocalTransactionState getLocalTransactionState()
        {
            return localTransactionState;
        }

        public void setLocalTransactionState(LocalTransactionState localTransactionState)
        {
            this.localTransactionState = localTransactionState;
        }

    }
}
