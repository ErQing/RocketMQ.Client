using System.Collections.Generic;

namespace RocketMQ.Client
{
    public interface MQConsumer : IMQAdmin
    {
        /**
         * If consuming failure,message will be send back to the brokers,and delay consuming some time
         * @Deprecated
         */
        //[Obsolete]
        void sendMessageBack(MessageExt msg, int delayLevel);

        /**
         * If consuming failure,message will be send back to the broker,and delay consuming some time
         */
        void sendMessageBack(MessageExt msg, int delayLevel, string brokerName);

        /**
         * Fetch message queues from consumer cache according to the topic
         *
         * @param topic message topic
         * @return queue set
         */
        HashSet<MessageQueue> fetchSubscribeMessageQueues(string topic);
    }
}
