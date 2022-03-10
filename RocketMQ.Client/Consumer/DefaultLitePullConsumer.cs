using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RocketMQ.Client
{
    public class DefaultLitePullConsumer : ClientConfig, LitePullConsumer
    {
        //private readonly InternalLogger log = ClientLogger.getLog();
        static NLog.Logger log = NLog.LogManager.GetCurrentClassLogger();

        private readonly DefaultLitePullConsumerImpl defaultLitePullConsumerImpl;

        /**
         * Consumers belonging to the same consumer group share a group id. The consumers in a group then divides the topic
         * as fairly amongst themselves as possible by establishing that each queue is only consumed by a single consumer
         * from the group. If all consumers are from the same group, it functions as a traditional message queue. Each
         * message would be consumed by one consumer of the group only. When multiple consumer groups exist, the flow of the
         * data consumption model aligns with the traditional publish-subscribe model. The messages are broadcast to all
         * consumer groups.
         */
        private String consumerGroup;

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
         * Queue allocation algorithm
         */
        private AllocateMessageQueueStrategy allocateMessageQueueStrategy = new AllocateMessageQueueAveragely();
        /**
         * Whether the unit of subscription group
         */
        private bool unitMode = false;

        /**
         * The flag for auto commit offset
         */
        private bool autoCommit = true;

        /**
         * Pull thread number
         */
        private int pullThreadNums = 20;

        /**
         * Minimum commit offset interval time in milliseconds.
         */
        private static readonly long MIN_AUTOCOMMIT_INTERVAL_MILLIS = 1000;

        /**
         * Maximum commit offset interval time in milliseconds.
         */
        private long autoCommitIntervalMillis = 5 * 1000;

        /**
         * Maximum number of messages pulled each time.
         */
        private int pullBatchSize = 10;

        /**
         * Flow control threshold for consume request, each consumer will cache at most 10000 consume requests by default.
         * Consider the {@code pullBatchSize}, the instantaneous value may exceed the limit
         */
        private long pullThresholdForAll = 10000;

        /**
         * Consume max span offset.
         */
        private int consumeMaxSpan = 2000;

        /**
         * Flow control threshold on queue level, each message queue will cache at most 1000 messages by default, Consider
         * the {@code pullBatchSize}, the instantaneous value may exceed the limit
         */
        private int pullThresholdForQueue = 1000;

        /**
         * Limit the cached message size on queue level, each message queue will cache at most 100 MiB messages by default,
         * Consider the {@code pullBatchSize}, the instantaneous value may exceed the limit
         *
         * <p>
         * The size of a message only measured by message body, so it's not accurate
         */
        private int pullThresholdSizeForQueue = 100;

        /**
         * The poll timeout in milliseconds
         */
        private long pollTimeoutMillis = 1000 * 5;

        /**
         * Interval time in in milliseconds for checking changes in topic metadata.
         */
        private long topicMetadataCheckIntervalMillis = 30 * 1000;

        private ConsumeFromWhere consumeFromWhere = ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET;

        /**
         * Backtracking consumption time with second precision. Time format is 20131223171201<br> Implying Seventeen twelve
         * and 01 seconds on December 23, 2013 year<br> Default backtracking consumption time Half an hour ago.
         */
        private String consumeTimestamp = UtilAll.timeMillisToHumanString3(TimeUtils.CurrentTimeMillis(true) - (1000 * 60 * 30));

        /**
         * Interface of asynchronous transfer data
         */
        private TraceDispatcher traceDispatcher = null;

        /**
         * The flag for message trace
         */
        private bool enableMsgTrace = false;

        /**
         * The name value of message trace topic.If you don't config,you can use the default trace topic name.
         */
        private String customizedTraceTopic;

        /**
         * Default constructor.
         */
        public DefaultLitePullConsumer() : this(null, MixAll.DEFAULT_CONSUMER_GROUP, null)
        {

        }

        /**
         * Constructor specifying consumer group.
         *
         * @param consumerGroup Consumer group.
         */
        public DefaultLitePullConsumer(string consumerGroup) : this(null, consumerGroup, null)
        {

        }

        /**
         * Constructor specifying RPC hook.
         *
         * @param rpcHook RPC hook to execute before each remoting command.
         */
        public DefaultLitePullConsumer(RPCHook rpcHook) : this(null, MixAll.DEFAULT_CONSUMER_GROUP, rpcHook)
        {

        }

        /**
         * Constructor specifying consumer group, RPC hook
         *
         * @param consumerGroup Consumer group.
         * @param rpcHook RPC hook to execute before each remoting command.
         */
        public DefaultLitePullConsumer(string consumerGroup, RPCHook rpcHook) : this(null, consumerGroup, rpcHook)
        {

        }

        /**
         * Constructor specifying namespace, consumer group and RPC hook.
         *
         * @param consumerGroup Consumer group.
         * @param rpcHook RPC hook to execute before each remoting command.
         */
        public DefaultLitePullConsumer(String nameSpace, String consumerGroup, RPCHook rpcHook)
        {
            this.nameSpace = nameSpace;
            this.consumerGroup = consumerGroup;
            defaultLitePullConsumerImpl = new DefaultLitePullConsumerImpl(this, rpcHook);
        }

        public void start()
        {
            setTraceDispatcher();
            setConsumerGroup(NamespaceUtil.wrapNamespace(this.getNamespace(), this.consumerGroup));
            this.defaultLitePullConsumerImpl.start();
            if (null != traceDispatcher)
            {
                try
                {
                    traceDispatcher.start(this.getNamesrvAddr(), this.getAccessChannel());
                }
                catch (MQClientException e)
                {
                    log.Warn("trace dispatcher start failed ", e);
                }
            }
        }

        public void shutdown()
        {
            this.defaultLitePullConsumerImpl.shutdown();
            if (null != traceDispatcher)
            {
                traceDispatcher.shutdown();
            }
        }

        public bool isRunning()
        {
            return this.defaultLitePullConsumerImpl.isRunning();
        }

        public void subscribe(String topic, String subExpression)
        {
            this.defaultLitePullConsumerImpl.subscribe(withNamespace(topic), subExpression);
        }

        public void subscribe(String topic, MessageSelector messageSelector)
        {
            this.defaultLitePullConsumerImpl.subscribe(withNamespace(topic), messageSelector);
        }

        public void unsubscribe(String topic)
        {
            this.defaultLitePullConsumerImpl.unsubscribe(withNamespace(topic));
        }

        public void assign(ICollection<MessageQueue> messageQueues)
        {
            defaultLitePullConsumerImpl.assign(queuesWithNamespace(messageQueues));
        }

        public List<MessageExt> poll()
        {
            return defaultLitePullConsumerImpl.poll(this.getPollTimeoutMillis());
        }

        public List<MessageExt> poll(long timeout)
        {
            return defaultLitePullConsumerImpl.poll(timeout);
        }

        public void seek(MessageQueue messageQueue, long offset)
        {
            this.defaultLitePullConsumerImpl.seek(queueWithNamespace(messageQueue), offset);
        }

        public void pause(ICollection<MessageQueue> messageQueues)
        {
            this.defaultLitePullConsumerImpl.pause(queuesWithNamespace(messageQueues));
        }

        public void resume(ICollection<MessageQueue> messageQueues)
        {
            this.defaultLitePullConsumerImpl.resume(queuesWithNamespace(messageQueues));
        }

        public ICollection<MessageQueue> fetchMessageQueues(String topic)
        {
            return this.defaultLitePullConsumerImpl.fetchMessageQueues(withNamespace(topic));
        }

        public long offsetForTimestamp(MessageQueue messageQueue, long timestamp)
        {
            return this.defaultLitePullConsumerImpl.searchOffset(queueWithNamespace(messageQueue), timestamp);
        }

        public void registerTopicMessageQueueChangeListener(String topic,
            TopicMessageQueueChangeListener topicMessageQueueChangeListener)
        {
            this.defaultLitePullConsumerImpl.registerTopicMessageQueueChangeListener(withNamespace(topic), topicMessageQueueChangeListener);
        }

        public void commitSync()
        {
            this.defaultLitePullConsumerImpl.commitAll();
        }

        public long committed(MessageQueue messageQueue)
        {
            return this.defaultLitePullConsumerImpl.committed(queueWithNamespace(messageQueue));
        }

        public void updateNameServerAddress(String nameServerAddress)
        {
            this.defaultLitePullConsumerImpl.updateNameServerAddr(nameServerAddress);
        }

        public void seekToBegin(MessageQueue messageQueue)
        {
            this.defaultLitePullConsumerImpl.seekToBegin(queueWithNamespace(messageQueue));
        }

        public void seekToEnd(MessageQueue messageQueue)
        {
            this.defaultLitePullConsumerImpl.seekToEnd(queueWithNamespace(messageQueue));
        }

        public bool isAutoCommit()
        {
            return autoCommit;
        }

        public void setAutoCommit(bool autoCommit)
        {
            this.autoCommit = autoCommit;
        }

        public bool isConnectBrokerByUser()
        {
            return this.defaultLitePullConsumerImpl.getPullAPIWrapper().isConnectBrokerByUser();
        }

        public void setConnectBrokerByUser(bool connectBrokerByUser)
        {
            this.defaultLitePullConsumerImpl.getPullAPIWrapper().setConnectBrokerByUser(connectBrokerByUser);
        }

        public long getDefaultBrokerId()
        {
            return this.defaultLitePullConsumerImpl.getPullAPIWrapper().getDefaultBrokerId();
        }

        public void setDefaultBrokerId(long defaultBrokerId)
        {
            this.defaultLitePullConsumerImpl.getPullAPIWrapper().setDefaultBrokerId(defaultBrokerId);
        }

        public int getPullThreadNums()
        {
            return pullThreadNums;
        }

        public void setPullThreadNums(int pullThreadNums)
        {
            this.pullThreadNums = pullThreadNums;
        }

        public long getAutoCommitIntervalMillis()
        {
            return autoCommitIntervalMillis;
        }

        public void setAutoCommitIntervalMillis(long autoCommitIntervalMillis)
        {
            if (autoCommitIntervalMillis >= MIN_AUTOCOMMIT_INTERVAL_MILLIS)
            {
                this.autoCommitIntervalMillis = autoCommitIntervalMillis;
            }
        }

        public int getPullBatchSize()
        {
            return pullBatchSize;
        }

        public void setPullBatchSize(int pullBatchSize)
        {
            this.pullBatchSize = pullBatchSize;
        }

        public long getPullThresholdForAll()
        {
            return pullThresholdForAll;
        }

        public void setPullThresholdForAll(long pullThresholdForAll)
        {
            this.pullThresholdForAll = pullThresholdForAll;
        }

        public int getConsumeMaxSpan()
        {
            return consumeMaxSpan;
        }

        public void setConsumeMaxSpan(int consumeMaxSpan)
        {
            this.consumeMaxSpan = consumeMaxSpan;
        }

        public int getPullThresholdForQueue()
        {
            return pullThresholdForQueue;
        }

        public void setPullThresholdForQueue(int pullThresholdForQueue)
        {
            this.pullThresholdForQueue = pullThresholdForQueue;
        }

        public int getPullThresholdSizeForQueue()
        {
            return pullThresholdSizeForQueue;
        }

        public void setPullThresholdSizeForQueue(int pullThresholdSizeForQueue)
        {
            this.pullThresholdSizeForQueue = pullThresholdSizeForQueue;
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

        public long getPollTimeoutMillis()
        {
            return pollTimeoutMillis;
        }

        public void setPollTimeoutMillis(long pollTimeoutMillis)
        {
            this.pollTimeoutMillis = pollTimeoutMillis;
        }

        public OffsetStore getOffsetStore()
        {
            return offsetStore;
        }

        public void setOffsetStore(OffsetStore offsetStore)
        {
            this.offsetStore = offsetStore;
        }

        public bool isUnitMode()
        {
            return unitMode;
        }

        public void setUnitMode(bool isUnitMode)
        {
            this.unitMode = isUnitMode;
        }

        public MessageModel getMessageModel()
        {
            return messageModel;
        }

        public void setMessageModel(MessageModel messageModel)
        {
            this.messageModel = messageModel;
        }

        public String getConsumerGroup()
        {
            return consumerGroup;
        }

        public MessageQueueListener getMessageQueueListener()
        {
            return messageQueueListener;
        }

        public void setMessageQueueListener(MessageQueueListener messageQueueListener)
        {
            this.messageQueueListener = messageQueueListener;
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

        public long getTopicMetadataCheckIntervalMillis()
        {
            return topicMetadataCheckIntervalMillis;
        }

        public void setTopicMetadataCheckIntervalMillis(long topicMetadataCheckIntervalMillis)
        {
            this.topicMetadataCheckIntervalMillis = topicMetadataCheckIntervalMillis;
        }

        public void setConsumerGroup(String consumerGroup)
        {
            this.consumerGroup = consumerGroup;
        }

        public ConsumeFromWhere getConsumeFromWhere()
        {
            return consumeFromWhere;
        }

        public void setConsumeFromWhere(ConsumeFromWhere consumeFromWhere)
        {
            if (consumeFromWhere != ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET
                && consumeFromWhere != ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET
                && consumeFromWhere != ConsumeFromWhere.CONSUME_FROM_TIMESTAMP)
            {
                throw new Exception("Invalid ConsumeFromWhere Value", null);
            }
            this.consumeFromWhere = consumeFromWhere;
        }

        public String getConsumeTimestamp()
        {
            return consumeTimestamp;
        }

        public void setConsumeTimestamp(String consumeTimestamp)
        {
            this.consumeTimestamp = consumeTimestamp;
        }

        public TraceDispatcher getTraceDispatcher()
        {
            return traceDispatcher;
        }

        public void setCustomizedTraceTopic(String customizedTraceTopic)
        {
            this.customizedTraceTopic = customizedTraceTopic;
        }

        private void setTraceDispatcher()
        {
            if (isEnableMsgTrace())
            {
                try
                {
                    AsyncTraceDispatcher traceDispatcher = new AsyncTraceDispatcher(consumerGroup, TraceDispatcher.Type.CONSUME, customizedTraceTopic, null);
                    traceDispatcher.getTraceProducer().setUseTLS(this.isUseTLS());
                    this.traceDispatcher = traceDispatcher;
                    this.defaultLitePullConsumerImpl.registerConsumeMessageHook(
                        new ConsumeMessageTraceHookImpl(traceDispatcher));
                }
                catch (Exception e)
                {
                    log.Error("system mqtrace hook init failed ,maybe can't send msg trace data" + e.ToString());
                }
            }
        }

        public String getCustomizedTraceTopic()
        {
            return customizedTraceTopic;
        }

        public bool isEnableMsgTrace()
        {
            return enableMsgTrace;
        }

        public void setEnableMsgTrace(bool enableMsgTrace)
        {
            this.enableMsgTrace = enableMsgTrace;
        }
    }
}
