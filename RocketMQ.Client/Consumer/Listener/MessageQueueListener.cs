using System;
using System.Collections.Generic;

namespace RocketMQ.Client
{
    public interface MessageQueueListener
    {
        /**
         * @param topic message topic
         * @param mqAll all queues in this message topic
         * @param mqDivided collection of queues,assigned to the current consumer
         */
        void messageQueueChanged(String topic, HashSet<MessageQueue> mqAll, HashSet<MessageQueue> mqDivided);
    }
}
