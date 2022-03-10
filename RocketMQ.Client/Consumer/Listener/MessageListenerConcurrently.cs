using System.Collections.Generic;

namespace RocketMQ.Client
{
    public interface MessageListenerConcurrently : MessageListener
    {
        /**
         * It is not recommend to throw exception,rather than returning ConsumeConcurrentlyStatus.RECONSUME_LATER if
         * consumption failure
         *
         * @param msgs msgs.size() >= 1<br> DefaultMQPushConsumer.consumeMessageBatchMaxSize=1,you can modify here
         * @return The consume status
         */
        ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context);
    }
}
