using System.Collections.Generic;

namespace RocketMQ.Client
{
    public interface AllocateMessageQueueStrategy
    {
        /**
     * Allocating by consumer id
     *
     * @param consumerGroup current consumer group
     * @param currentCID current consumer id
     * @param mqAll message queue set in current topic
     * @param cidAll consumer set in current consumer group
     * @return The allocate result of given strategy
     */
        List<MessageQueue> allocate(
            string consumerGroup,
            string currentCID,
            List<MessageQueue> mqAll,
            List<string> cidAll
        );

        /**
         * Algorithm name
         *
         * @return The strategy name
         */
        string getName();
    }
}
