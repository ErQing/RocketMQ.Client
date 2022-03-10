using System;
using System.Collections.Generic;

namespace RocketMQ.Client
{
    public interface TopicMessageQueueChangeListener
    {
        /**
         * This method will be invoked in the condition of queue numbers changed, These scenarios occur when the topic is
         * expanded or shrunk.
         *
         * @param messageQueues
         */
        void onChanged(String topic, HashSet<MessageQueue> messageQueues);
    }
}
