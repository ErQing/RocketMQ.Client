using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RocketMQ.Client
{
    public interface LitePullConsumer
    {
        /**
     * Start the consumer
     */
        void start();

        /**
         * Shutdown the consumer
         */
        void shutdown();

        /**
         * This consumer is still running
         *
         * @return true if consumer is still running
         */
        bool isRunning();

        /**
         * Subscribe some topic with subExpression
         *
         * @param subExpression subscription expression.it only support or operation such as "tag1 || tag2 || tag3" <br> if
         * null or * expression,meaning subscribe all
         * @throws MQClientException if there is any client error.
         */
        void subscribe(string topic, string subExpression);

        /**
         * Subscribe some topic with selector.
         *
         * @param selector message selector({@link MessageSelector}), can be null.
         * @throws MQClientException if there is any client error.
         */
        void subscribe(string topic, MessageSelector selector);

        /**
         * Unsubscribe consumption some topic
         *
         * @param topic Message topic that needs to be unsubscribe.
         */
        void unsubscribe(string topic);

        /**
         * Manually assign a list of message queues to this consumer. This interface does not allow for incremental
         * assignment and will replace the previous assignment (if there is one).
         *
         * @param messageQueues Message queues that needs to be assigned.
         */
        void assign(ICollection<MessageQueue> messageQueues);

        /**
         * Fetch data for the topics or partitions specified using assign API
         *
         * @return list of message, can be null.
         */
        List<MessageExt> poll();

        /**
         * Fetch data for the topics or partitions specified using assign API
         *
         * @param timeout The amount time, in milliseconds, spent waiting in poll if data is not available. Must not be
         * negative
         * @return list of message, can be null.
         */
        List<MessageExt> poll(long timeout);

        /**
         * Overrides the fetch offsets that the consumer will use on the next poll. If this API is invoked for the same
         * message queue more than once, the latest offset will be used on the next poll(). Note that you may lose data if
         * this API is arbitrarily used in the middle of consumption.
         *
         * @param messageQueue
         * @param offset
         */
        void seek(MessageQueue messageQueue, long offset);

        /**
         * Suspend pulling from the requested message queues.
         *
         * Because of the implementation of pre-pull, fetch data in {@link #poll()} will not stop immediately until the
         * messages of the requested message queues drain.
         *
         * Note that this method does not affect message queue subscription. In particular, it does not cause a group
         * rebalance.
         *
         * @param messageQueues Message queues that needs to be paused.
         */
        void pause(ICollection<MessageQueue> messageQueues);

        /**
         * Resume specified message queues which have been paused with {@link #pause(Collection)}.
         *
         * @param messageQueues Message queues that needs to be resumed.
         */
        void resume(ICollection<MessageQueue> messageQueues);

        /**
         * Whether to enable auto-commit consume offset.
         *
         * @return true if enable auto-commit, false if disable auto-commit.
         */
        bool isAutoCommit();

        /**
         * Set whether to enable auto-commit consume offset.
         *
         * @param autoCommit Whether to enable auto-commit.
         */
        void setAutoCommit(bool autoCommit);

        /**
         * Get metadata about the message queues for a given topic.
         *
         * @param topic The topic that need to get metadata.
         * @return collection of message queues
         * @throws MQClientException if there is any client error.
         */
        ICollection<MessageQueue> fetchMessageQueues(string topic);

        /**
         * Look up the offsets for the given message queue by timestamp. The returned offset for each message queue is the
         * earliest offset whose timestamp is greater than or equal to the given timestamp in the corresponding message
         * queue.
         *
         * @param messageQueue Message queues that needs to get offset by timestamp.
         * @param timestamp
         * @return offset
         * @throws MQClientException if there is any client error.
         */
        long offsetForTimestamp(MessageQueue messageQueue, long timestamp);

        /**
         * Manually commit consume offset.
         */
        void commitSync();

        /**
         * Get the last committed offset for the given message queue.
         *
         * @param messageQueue
         * @return offset, if offset equals -1 means no offset in broker.
         * @throws MQClientException if there is any client error.
         */
        long committed(MessageQueue messageQueue);

        /**
         * Register a callback for sensing topic metadata changes.
         *
         * @param topic The topic that need to monitor.
         * @param topicMessageQueueChangeListener Callback when topic metadata changes, refer {@link
         * TopicMessageQueueChangeListener}
         * @throws MQClientException if there is any client error.
         */
        void registerTopicMessageQueueChangeListener(string topic, TopicMessageQueueChangeListener topicMessageQueueChangeListener);

        /**
         * Update name server addresses.
         */
        void updateNameServerAddress(string nameServerAddress);

        /**
         * Overrides the fetch offsets with the begin offset that the consumer will use on the next poll. If this API is
         * invoked for the same message queue more than once, the latest offset will be used on the next poll(). Note that
         * you may lose data if this API is arbitrarily used in the middle of consumption.
         *
         * @param messageQueue
         */
        void seekToBegin(MessageQueue messageQueue);

        /**
         * Overrides the fetch offsets with the end offset that the consumer will use on the next poll. If this API is
         * invoked for the same message queue more than once, the latest offset will be used on the next poll(). Note that
         * you may lose data if this API is arbitrarily used in the middle of consumption.
         *
         * @param messageQueue
         */
        void seekToEnd(MessageQueue messageQueue);
    }
}
