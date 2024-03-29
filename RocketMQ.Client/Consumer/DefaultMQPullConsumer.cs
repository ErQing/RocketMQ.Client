﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RocketMQ.Client//.Consumer
{
    public class DefaultMQPullConsumer : ClientConfig, MQPullConsumer
    {
        //protected final transient DefaultMQPullConsumerImpl defaultMQPullConsumerImpl;

        protected readonly DefaultMQPullConsumerImpl defaultMQPullConsumerImpl;

        /**
         * Do the same thing for the same Group, the application must be set,and guarantee Globally unique
         */
        private string consumerGroup;
        /**
         * Long polling mode, the Consumer connection max suspend time, it is not recommended to modify
         */
        private long brokerSuspendMaxTimeMillis = 1000 * 20;
        /**
         * Long polling mode, the Consumer connection timeout(must greater than brokerSuspendMaxTimeMillis), it is not
         * recommended to modify
         */
        private long consumerTimeoutMillisWhenSuspend = 1000 * 30;
        /**
         * The socket timeout in milliseconds
         */
        private long consumerPullTimeoutMillis = 1000 * 10;
        /**
         * Consumption pattern,default is clustering
         */
        private MessageModel messageModel = MessageModel.CLUSTERING;
        /**
         * Message queue listener
         */
        private MessageQueueListener messageQueueListener;
        /**
         * Offset Storage
         */
        private OffsetStore offsetStore;
        /**
         * Topic set you want to register
         */
        private HashSet<String> registerTopics = new HashSet<String>();
        /**
         * Queue allocation algorithm
         */
        private AllocateMessageQueueStrategy allocateMessageQueueStrategy = new AllocateMessageQueueAveragely();
        /**
         * Whether the unit of subscription group
         */
        private bool unitMode = false;

        private int maxReconsumeTimes = 16;

        public DefaultMQPullConsumer() : this(null, MixAll.DEFAULT_CONSUMER_GROUP, null)
        {

        }

        public DefaultMQPullConsumer(String consumerGroup) : this(null, consumerGroup, null)
        {

        }

        public DefaultMQPullConsumer(RPCHook rpcHook) : this(null, MixAll.DEFAULT_CONSUMER_GROUP, rpcHook)
        {

        }

        public DefaultMQPullConsumer(String consumerGroup, RPCHook rpcHook) : this(null, consumerGroup, rpcHook)
        {

        }

        public DefaultMQPullConsumer(String nameSpace, string consumerGroup) : this(nameSpace, consumerGroup, null)
        {

        }
        /**
         * Constructor specifying namespace, consumer group and RPC hook.
         *
         * @param consumerGroup Consumer group.
         * @param rpcHook RPC hook to execute before each remoting command.
         */
        public DefaultMQPullConsumer(String nameSpace, string consumerGroup, RPCHook rpcHook)
        {
            this.nameSpace = nameSpace;
            this.consumerGroup = consumerGroup;
            defaultMQPullConsumerImpl = new DefaultMQPullConsumerImpl(this, rpcHook);
        }

        /**
         * This method will be removed in a certain version after April 5, 2020, so please do not use this method.
         */
        [Obsolete]//@Deprecated @Override
        public void createTopic(String key, string newTopic, int queueNum)
        {
            createTopic(key, withNamespace(newTopic), queueNum, 0);
        }

        /**
         * This method will be removed in a certain version after April 5, 2020, so please do not use this method.
         */
        [Obsolete]//@Deprecated @Override
        public void createTopic(String key, string newTopic, int queueNum, int topicSysFlag)
        {
            this.defaultMQPullConsumerImpl.createTopic(key, withNamespace(newTopic), queueNum, topicSysFlag);
        }

        /**
         * This method will be removed in a certain version after April 5, 2020, so please do not use this method.
         */
        [Obsolete]//@Deprecated @Override
        public long searchOffset(MessageQueue mq, long timestamp)
        {
            return this.defaultMQPullConsumerImpl.searchOffset(queueWithNamespace(mq), timestamp);
        }

        /**
         * This method will be removed in a certain version after April 5, 2020, so please do not use this method.
         */
        [Obsolete]//@Deprecated @Override
        public long maxOffset(MessageQueue mq)
        {
            return this.defaultMQPullConsumerImpl.maxOffset(queueWithNamespace(mq));
        }

        /**
         * This method will be removed in a certain version after April 5, 2020, so please do not use this method.
         */
        [Obsolete]//@Deprecated @Override
        public long minOffset(MessageQueue mq)
        {
            return this.defaultMQPullConsumerImpl.minOffset(queueWithNamespace(mq));
        }

        /**
         * This method will be removed in a certain version after April 5, 2020, so please do not use this method.
         */
        [Obsolete]//@Deprecated @Override
        public long earliestMsgStoreTime(MessageQueue mq)
        {
            return this.defaultMQPullConsumerImpl.earliestMsgStoreTime(queueWithNamespace(mq));
        }

        /**
         * This method will be removed in a certain version after April 5, 2020, so please do not use this method.
         */
        [Obsolete]//@Deprecated @Override
        public MessageExt viewMessage(String offsetMsgId)
        {
            return this.defaultMQPullConsumerImpl.viewMessage(offsetMsgId);
        }

        /**
         * This method will be removed in a certain version after April 5, 2020, so please do not use this method.
         */
        [Obsolete]//@Deprecated @Override
        public QueryResult queryMessage(String topic, string key, int maxNum, long begin, long end)
        {
            return this.defaultMQPullConsumerImpl.queryMessage(withNamespace(topic), key, maxNum, begin, end);
        }

        public AllocateMessageQueueStrategy getAllocateMessageQueueStrategy()
        {
            return allocateMessageQueueStrategy;
        }

        public void setAllocateMessageQueueStrategy(AllocateMessageQueueStrategy allocateMessageQueueStrategy)
        {
            this.allocateMessageQueueStrategy = allocateMessageQueueStrategy;
        }

        public long getBrokerSuspendMaxTimeMillis()
        {
            return brokerSuspendMaxTimeMillis;
        }

        /**
         * This method will be removed in a certain version after April 5, 2020, so please do not use this method.
         */
        [Obsolete]//@Deprecated 
        public void setBrokerSuspendMaxTimeMillis(long brokerSuspendMaxTimeMillis)
        {
            this.brokerSuspendMaxTimeMillis = brokerSuspendMaxTimeMillis;
        }

        public string getConsumerGroup()
        {
            return consumerGroup;
        }

        public void setConsumerGroup(String consumerGroup)
        {
            this.consumerGroup = consumerGroup;
        }

        public long getConsumerPullTimeoutMillis()
        {
            return consumerPullTimeoutMillis;
        }

        public void setConsumerPullTimeoutMillis(long consumerPullTimeoutMillis)
        {
            this.consumerPullTimeoutMillis = consumerPullTimeoutMillis;
        }

        public long getConsumerTimeoutMillisWhenSuspend()
        {
            return consumerTimeoutMillisWhenSuspend;
        }

        public void setConsumerTimeoutMillisWhenSuspend(long consumerTimeoutMillisWhenSuspend)
        {
            this.consumerTimeoutMillisWhenSuspend = consumerTimeoutMillisWhenSuspend;
        }

        public MessageModel getMessageModel()
        {
            return messageModel;
        }

        public void setMessageModel(MessageModel messageModel)
        {
            this.messageModel = messageModel;
        }

        public MessageQueueListener getMessageQueueListener()
        {
            return messageQueueListener;
        }

        public void setMessageQueueListener(MessageQueueListener messageQueueListener)
        {
            this.messageQueueListener = messageQueueListener;
        }

        public HashSet<String> getRegisterTopics()
        {
            return registerTopics;
        }

        public void setRegisterTopics(HashSet<String> registerTopics)
        {
            this.registerTopics = withNamespace(registerTopics);
        }

        /**
         * This method will be removed or it's visibility will be changed in a certain version after April 5, 2020, so
         * please do not use this method.
         */
        [Obsolete]//@Deprecated @Override
        public void sendMessageBack(MessageExt msg, int delayLevel)
        {
            msg.setTopic(withNamespace(msg.getTopic()));
            this.defaultMQPullConsumerImpl.sendMessageBack(msg, delayLevel, null);
        }

        /**
         * This method will be removed or it's visibility will be changed in a certain version after April 5, 2020, so
         * please do not use this method.
         */
        [Obsolete]//@Deprecated @Override
        public void sendMessageBack(MessageExt msg, int delayLevel, string brokerName)
        {
            msg.setTopic(withNamespace(msg.getTopic()));
            this.defaultMQPullConsumerImpl.sendMessageBack(msg, delayLevel, brokerName);
        }

        //@Override
        public HashSet<MessageQueue> fetchSubscribeMessageQueues(String topic)
        {
            return this.defaultMQPullConsumerImpl.fetchSubscribeMessageQueues(withNamespace(topic));
        }

        //@Override
        public void start()
        {
            this.setConsumerGroup(NamespaceUtil.wrapNamespace(this.getNamespace(), this.consumerGroup));
            this.defaultMQPullConsumerImpl.start();
        }

        //@Override
        public void shutdown()
        {
            this.defaultMQPullConsumerImpl.shutdown();
        }

        //@Override
        public void registerMessageQueueListener(String topic, MessageQueueListener listener)
        {
            lock (this.registerTopics)
            {
                this.registerTopics.Add(withNamespace(topic));
                if (listener != null)
                {
                    this.messageQueueListener = listener;
                }
            }
        }

        //@Override
        public PullResult pull(MessageQueue mq, string subExpression, long offset, int maxNums)
        {
            return this.defaultMQPullConsumerImpl.pull(queueWithNamespace(mq), subExpression, offset, maxNums);
        }

        //@Override
        public PullResult pull(MessageQueue mq, string subExpression, long offset, int maxNums, long timeout)
        {
            return this.defaultMQPullConsumerImpl.pull(queueWithNamespace(mq), subExpression, offset, maxNums, timeout);
        }

        //@Override
        public PullResult pull(MessageQueue mq, MessageSelector messageSelector, long offset, int maxNums)
        {
            return this.defaultMQPullConsumerImpl.pull(queueWithNamespace(mq), messageSelector, offset, maxNums);
        }

        //@Override
        public PullResult pull(MessageQueue mq, MessageSelector messageSelector, long offset, int maxNums, long timeout)
        {
            return this.defaultMQPullConsumerImpl.pull(queueWithNamespace(mq), messageSelector, offset, maxNums, timeout);
        }

        //@Override
        public void pull(MessageQueue mq, string subExpression, long offset, int maxNums, PullCallback pullCallback)
        {
            this.defaultMQPullConsumerImpl.pull(queueWithNamespace(mq), subExpression, offset, maxNums, pullCallback);
        }

        //@Override
        public void pull(MessageQueue mq, string subExpression, long offset, int maxNums, PullCallback pullCallback,
            long timeout)
        {
            this.defaultMQPullConsumerImpl.pull(queueWithNamespace(mq), subExpression, offset, maxNums, pullCallback, timeout);
        }

        //@Override
        public void pull(MessageQueue mq, MessageSelector messageSelector, long offset, int maxNums,
            PullCallback pullCallback)
        {
            this.defaultMQPullConsumerImpl.pull(queueWithNamespace(mq), messageSelector, offset, maxNums, pullCallback);
        }

        //@Override
        public void pull(MessageQueue mq, MessageSelector messageSelector, long offset, int maxNums,
            PullCallback pullCallback, long timeout)
        {
            this.defaultMQPullConsumerImpl.pull(queueWithNamespace(mq), messageSelector, offset, maxNums, pullCallback, timeout);
        }

        //@Override
        public PullResult pullBlockIfNotFound(MessageQueue mq, string subExpression, long offset, int maxNums)
        {
            return this.defaultMQPullConsumerImpl.pullBlockIfNotFound(queueWithNamespace(mq), subExpression, offset, maxNums);
        }

        //@Override
        public void pullBlockIfNotFound(MessageQueue mq, string subExpression, long offset, int maxNums,
            PullCallback pullCallback)
        {
            this.defaultMQPullConsumerImpl.pullBlockIfNotFound(queueWithNamespace(mq), subExpression, offset, maxNums, pullCallback);
        }

        //@Override
        public void updateConsumeOffset(MessageQueue mq, long offset)
        {
            this.defaultMQPullConsumerImpl.updateConsumeOffset(queueWithNamespace(mq), offset);
        }

        //@Override
        public long fetchConsumeOffset(MessageQueue mq, bool fromStore)
        {
            return this.defaultMQPullConsumerImpl.fetchConsumeOffset(queueWithNamespace(mq), fromStore);
        }

        //@Override
        public HashSet<MessageQueue> fetchMessageQueuesInBalance(String topic)
        {
            return this.defaultMQPullConsumerImpl.fetchMessageQueuesInBalance(withNamespace(topic));
        }

        //@Override
        public MessageExt viewMessage(String topic,
            string uniqKey)
        {
            try
            {
                MessageDecoder.decodeMessageId(uniqKey);
                return this.viewMessage(uniqKey);
            }
            catch (Exception e)
            {
                // Ignore
            }
            return this.defaultMQPullConsumerImpl.queryMessageByUniqKey(withNamespace(topic), uniqKey);
        }

        //@Override
        public void sendMessageBack(MessageExt msg, int delayLevel, string brokerName, string consumerGroup)
        {
            msg.setTopic(withNamespace(msg.getTopic()));
            this.defaultMQPullConsumerImpl.sendMessageBack(msg, delayLevel, brokerName, consumerGroup);
        }

        /**
         * This method will be removed in a certain version after April 5, 2020, so please do not use this method.
         */
        [Obsolete]//@Deprecated 
        public OffsetStore getOffsetStore()
        {
            return offsetStore;
        }

        /**
         * This method will be removed in a certain version after April 5, 2020, so please do not use this method.
         */
        [Obsolete]//@Deprecated 
        public void setOffsetStore(OffsetStore offsetStore)
        {
            this.offsetStore = offsetStore;
        }

        /**
         * This method will be removed in a certain version after April 5, 2020, so please do not use this method.
         */
        [Obsolete]//@Deprecated 
        public DefaultMQPullConsumerImpl getDefaultMQPullConsumerImpl()
        {
            return defaultMQPullConsumerImpl;
        }

        public bool isUnitMode()
        {
            return unitMode;
        }

        public void setUnitMode(bool isUnitMode)
        {
            this.unitMode = isUnitMode;
        }

        public int getMaxReconsumeTimes()
        {
            return maxReconsumeTimes;
        }

        public void setMaxReconsumeTimes(int maxReconsumeTimes)
        {
            this.maxReconsumeTimes = maxReconsumeTimes;
        }
    }
}
