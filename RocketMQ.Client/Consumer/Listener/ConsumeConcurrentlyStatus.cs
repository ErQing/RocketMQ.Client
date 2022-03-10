namespace RocketMQ.Client
{
    public enum ConsumeConcurrentlyStatus
    {
        UNKNOWN,
        /**
         * Success consumption
         */
        CONSUME_SUCCESS,
        /**
         * Failure consumption,later try to consume
         * 消费失败，稍后尝试
         */
        RECONSUME_LATER
    }
}
