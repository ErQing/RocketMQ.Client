using System;

namespace RocketMQ.Client
{
    public interface MQPushConsumer
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
         * Register the message listener @Deprecated
         */
        [Obsolete]
        void registerMessageListener(MessageListener messageListener);

        void registerMessageListener(MessageListenerConcurrently messageListener);

        void registerMessageListener(MessageListenerOrderly messageListener);

        /**
         * Subscribe some topic
         *
         * @param subExpression subscription expression.it only support or operation such as "tag1 || tag2 || tag3" <br> if
         * null or * expression,meaning subscribe
         * all
         */
        void subscribe(string topic, string subExpression);

        /**
         * This method will be removed in the version 5.0.0,because filterServer was removed,and method <code>subscribe(final string topic, final MessageSelector messageSelector)</code>
         * is recommended.
         *
         * Subscribe some topic
         *
         * @param fullClassName full class name,must extend org.apache.rocketmq.common.filter. MessageFilter
         * @param filterClassSource class source code,used UTF-8 file encoding,must be responsible for your code safety
         * @Deprecated
         */
        [Obsolete]
        ///<exception cref="MQClientException"/>
        void subscribe(string topic, string fullClassName, string filterClassSource);

        /**
         * Subscribe some topic with selector.
         * <p>
         * This interface also has the ability of {@link #subscribe(string, string)},
         * and, support other message selection, such as {@link org.apache.rocketmq.common.filter.ExpressionType#SQL92}.
         * </p>
         * <p/>
         * <p>
         * Choose Tag: {@link MessageSelector#byTag(java.lang.string)}
         * </p>
         * <p/>
         * <p>
         * Choose SQL92: {@link MessageSelector#bySql(java.lang.string)}
         * </p>
         *
         * @param selector message selector({@link MessageSelector}), can be null.
         */
        void subscribe(string topic, MessageSelector selector);

        /**
         * Unsubscribe consumption some topic
         *
         * @param topic message topic
         */
        void unsubscribe(string topic);

        /**
         * Update the consumer thread pool size Dynamically
         */
        void updateCorePoolSize(int corePoolSize);

        /**
         * Suspend the consumption
         */
        void suspend();

        /**
         * Resume the consumption
         */
        void resume();
    }
}
