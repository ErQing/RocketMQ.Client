using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RocketMQ.Client//.Consumer
{
    public interface MQPullConsumer : MQConsumer
    {
        /**
     * Start the consumer
     */
        ///<exception cref="MQClientException"/>
        void start();

        /**
         * Shutdown the consumer
         */
        void shutdown();

        /**
         * Register the message queue listener
         */
        void registerMessageQueueListener(String topic, MessageQueueListener listener);

        /**
         * Pulling the messages,not blocking
         *
         * @param mq from which message queue
         * @param subExpression subscription expression.it only support or operation such as "tag1 || tag2 || tag3" <br> if
         * null or * expression,meaning subscribe
         * all
         * @param offset from where to pull
         * @param maxNums max pulling numbers
         * @return The resulting {@code PullRequest}
         */
        ///<exception cref="MQClientException"/>
        ///<exception cref="RemotingException"/>
        ///<exception cref="MQBrokerException"/>
        ///<exception cref="InterruptedException"/>
        PullResult pull(MessageQueue mq, string subExpression, long offset, int maxNums) ;

        /**
         * Pulling the messages in the specified timeout
         *
         * @return The resulting {@code PullRequest}
         */
        ///<exception cref="MQClientException"/>
        ///<exception cref="RemotingException"/>
        ///<exception cref="MQBrokerException"/>
        ///<exception cref="InterruptedException"/>
        PullResult pull(MessageQueue mq, string subExpression, long offset, int maxNums, long timeout) ;

        /**
         * Pulling the messages, not blocking
         * <p>
         * support other message selection, such as {@link org.apache.rocketmq.common.filter.ExpressionType#SQL92}
         * </p>
         *
         * @param mq from which message queue
         * @param selector message selector({@link MessageSelector}), can be null.
         * @param offset from where to pull
         * @param maxNums max pulling numbers
         * @return The resulting {@code PullRequest}
         */
        ///<exception cref="MQClientException"/>
        ///<exception cref="RemotingException"/>
        ///<exception cref="MQBrokerException"/>
        ///<exception cref="InterruptedException"/>
        PullResult pull(MessageQueue mq, MessageSelector selector, long offset, int maxNums);

        /**
         * Pulling the messages in the specified timeout
         * <p>
         * support other message selection, such as {@link org.apache.rocketmq.common.filter.ExpressionType#SQL92}
         * </p>
         *
         * @param mq from which message queue
         * @param selector message selector({@link MessageSelector}), can be null.
         * @param offset from where to pull
         * @param maxNums max pulling numbers
         * @param timeout Pulling the messages in the specified timeout
         * @return The resulting {@code PullRequest}
         */
        ///<exception cref="MQClientException"/>
        ///<exception cref="RemotingException"/>
        ///<exception cref="MQBrokerException"/>
        ///<exception cref="InterruptedException"/>
        PullResult pull(MessageQueue mq, MessageSelector selector, long offset, int maxNums, long timeout) ;

        /**
         * Pulling the messages in a async. way
         */
        ///<exception cref="MQClientException"/>
        ///<exception cref="RemotingException"/>
        ///<exception cref="InterruptedException"/>
        void pull(MessageQueue mq, string subExpression, long offset, int maxNums,
            PullCallback pullCallback);

        /**
         * Pulling the messages in a async. way
         */
        ///<exception cref="MQClientException"/>
        ///<exception cref="RemotingException"/>
        ///<exception cref="InterruptedException"/>
        void pull(MessageQueue mq, string subExpression, long offset, int maxNums,
            PullCallback pullCallback, long timeout);

        /**
         * Pulling the messages in a async. way. Support message selection
         */
        ///<exception cref="MQClientException"/>
        ///<exception cref="RemotingException"/>
        ///<exception cref="InterruptedException"/>
        void pull(MessageQueue mq, MessageSelector selector, long offset, int maxNums,
            PullCallback pullCallback);

        /**
         * Pulling the messages in a async. way. Support message selection
         */
        ///<exception cref="MQClientException"/>
        ///<exception cref="RemotingException"/>
        ///<exception cref="InterruptedException"/>
        void pull(MessageQueue mq, MessageSelector selector, long offset, int maxNums,
            PullCallback pullCallback, long timeout);

        /**
         * Pulling the messages,if no message arrival,blocking some time
         *
         * @return The resulting {@code PullRequest}
         */
        ///<exception cref="MQClientException"/>
        ///<exception cref="RemotingException"/>
        ///<exception cref="InterruptedException"/>
        ///<exception cref="MQBrokerException"/>
        PullResult pullBlockIfNotFound(MessageQueue mq, string subExpression,
            long offset, int maxNums);

        /**
         * Pulling the messages through callback function,if no message arrival,blocking.
         */
        ///<exception cref="MQClientException"/>
        ///<exception cref="RemotingException"/>
        ///<exception cref="InterruptedException"/>
        void pullBlockIfNotFound(MessageQueue mq, string subExpression, long offset,
            int maxNums, PullCallback pullCallback);

        /**
         * Update the offset
         */
        ///<exception cref="MQClientException"/>
        void updateConsumeOffset(MessageQueue mq, long offset);

        /**
         * Fetch the offset
         *
         * @return The fetched offset of given queue
         */
        ///<exception cref="MQClientException"/>
        long fetchConsumeOffset(MessageQueue mq, bool fromStore);

        /**
         * Fetch the message queues according to the topic
         *
         * @param topic message topic
         * @return message queue set
         */
        ///<exception cref="MQClientException"/>
        HashSet<MessageQueue> fetchMessageQueuesInBalance(String topic);

        /**
         * If consuming failure,message will be send back to the broker,and delay consuming in some time later.<br>
         * Mind! message can only be consumed in the same group.
         */
        ///<exception cref="MQClientException"/>
        ///<exception cref="RemotingException"/>
        ///<exception cref="InterruptedException"/>
        ///<exception cref="MQBrokerException"/>
        void sendMessageBack(MessageExt msg, int delayLevel, string brokerName, string consumerGroup);
    }
}
