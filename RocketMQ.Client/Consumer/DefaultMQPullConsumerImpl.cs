using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;

namespace RocketMQ.Client
{
    public class DefaultMQPullConsumerImpl : MQConsumerInner
    {
        //private final InternalLogger log = ClientLogger.getLog();
        static NLog.Logger log = NLog.LogManager.GetCurrentClassLogger();
        private readonly DefaultMQPullConsumer defaultMQPullConsumer;
        private readonly long consumerStartTimestamp = Sys.currentTimeMillis();
        private readonly RPCHook rpcHook;
        private readonly List<ConsumeMessageHook> consumeMessageHookList = new List<ConsumeMessageHook>();
        private readonly List<FilterMessageHook> filterMessageHookList = new List<FilterMessageHook>();
        private volatile ServiceState serviceState = ServiceState.CREATE_JUST;
        protected MQClientInstance mQClientFactory;
        private PullAPIWrapper pullAPIWrapper;
        private OffsetStore offsetStore;
        //private RebalanceImpl rebalanceImpl = new RebalancePullImpl(this);
        private RebalanceImpl rebalanceImpl = null; //???

        public DefaultMQPullConsumerImpl(DefaultMQPullConsumer defaultMQPullConsumer, RPCHook rpcHook)
        {
            this.defaultMQPullConsumer = defaultMQPullConsumer;
            this.rpcHook = rpcHook;
            rebalanceImpl = new RebalancePullImpl(this);
        }

        public void registerConsumeMessageHook(ConsumeMessageHook hook)
        {
            this.consumeMessageHookList.Add(hook);
            log.Info("register consumeMessageHook Hook, {}", hook.hookName());
        }

        ///<exception cref="MQClientException"/>
        public void createTopic(String key, string newTopic, int queueNum)
        {
            createTopic(key, newTopic, queueNum, 0);
        }

        ///<exception cref="MQClientException"/>
        public void createTopic(String key, string newTopic, int queueNum, int topicSysFlag)
        {
            this.isRunning();
            this.mQClientFactory.getMQAdminImpl().createTopic(key, newTopic, queueNum, topicSysFlag);
        }

        ///<exception cref="MQClientException"/>
        private void isRunning()
        {
            if (this.serviceState != ServiceState.RUNNING)
            {
                throw new MQClientException("The consumer is not in running status, "
                    + this.serviceState
                    + FAQUrl.suggestTodo(FAQUrl.CLIENT_SERVICE_NOT_OK),
                    null);
            }
        }

        ///<exception cref="MQClientException"/>
        public long fetchConsumeOffset(MessageQueue mq, bool fromStore)
        {
            this.isRunning();
            return this.offsetStore.readOffset(mq, fromStore ? ReadOffsetType.READ_FROM_STORE : ReadOffsetType.MEMORY_FIRST_THEN_STORE);
        }

        ///<exception cref="MQClientException"/>
        public HashSet<MessageQueue> fetchMessageQueuesInBalance(String topic)
        {
            this.isRunning();
            if (null == topic)
            {
                throw new ArgumentException("topic is null");
            }

            ConcurrentDictionary<MessageQueue, ProcessQueue> mqTable = this.rebalanceImpl.getProcessQueueTable();
            HashSet<MessageQueue> mqResult = new HashSet<MessageQueue>();
            foreach (MessageQueue mq in mqTable.Keys)
            {
                if (mq.getTopic().Equals(topic))
                {
                    mqResult.Add(mq);
                }
            }

            return parseSubscribeMessageQueues(mqResult);
        }

        ///<exception cref="MQClientException"/>
        public List<MessageQueue> fetchPublishMessageQueues(String topic)
        {
            this.isRunning();
            return this.mQClientFactory.getMQAdminImpl().fetchPublishMessageQueues(topic);
        }

        ///<exception cref="MQClientException"/>
        public HashSet<MessageQueue> fetchSubscribeMessageQueues(String topic)
        {
            this.isRunning();
            // check if has info in memory, otherwise invoke api.
            //HashSet<MessageQueue> result = this.rebalanceImpl.getTopicSubscribeInfoTable().get(topic);
            this.rebalanceImpl.getTopicSubscribeInfoTable().TryGetValue(topic, out HashSet<MessageQueue> result);
            if (null == result)
            {
                result = this.mQClientFactory.getMQAdminImpl().fetchSubscribeMessageQueues(topic);
            }

            return parseSubscribeMessageQueues(result);
        }

        public HashSet<MessageQueue> parseSubscribeMessageQueues(HashSet<MessageQueue> queueSet)
        {
            HashSet<MessageQueue> resultQueues = new HashSet<MessageQueue>();
            foreach (MessageQueue messageQueue in queueSet)
            {
                string userTopic = NamespaceUtil.withoutNamespace(messageQueue.getTopic(),
                    this.defaultMQPullConsumer.getNamespace());
                resultQueues.Add(new MessageQueue(userTopic, messageQueue.getBrokerName(), messageQueue.getQueueId()));
            }
            return resultQueues;
        }

        ///<exception cref="MQClientException"/>
        public long earliestMsgStoreTime(MessageQueue mq)
        {
            this.isRunning();
            return this.mQClientFactory.getMQAdminImpl().earliestMsgStoreTime(mq);
        }

        ///<exception cref="MQClientException"/>
        public long maxOffset(MessageQueue mq)
        {
            this.isRunning();
            return this.mQClientFactory.getMQAdminImpl().maxOffset(mq);
        }

        ///<exception cref="MQClientException"/>
        public long minOffset(MessageQueue mq)
        {
            this.isRunning();
            return this.mQClientFactory.getMQAdminImpl().minOffset(mq);
        }

        ///<exception cref="MQClientException"/>
        ///<exception cref="RemotingException"/>
        ///<exception cref="MQBrokerException"/>
        ///<exception cref="InterruptedException"/>
        public PullResult pull(MessageQueue mq, string subExpression, long offset, int maxNums)
        {
            return pull(mq, subExpression, offset, maxNums, this.defaultMQPullConsumer.getConsumerPullTimeoutMillis());
        }

        ///<exception cref="MQClientException"/>
        ///<exception cref="RemotingException"/>
        ///<exception cref="MQBrokerException"/>
        ///<exception cref="InterruptedException"/>
        public PullResult pull(MessageQueue mq, string subExpression, long offset, int maxNums, long timeout)
        {
            SubscriptionData subscriptionData = getSubscriptionData(mq, subExpression);
            return this.pullSyncImpl(mq, subscriptionData, offset, maxNums, false, timeout);
        }

        ///<exception cref="MQClientException"/>
        ///<exception cref="RemotingException"/>
        ///<exception cref="MQBrokerException"/>
        ///<exception cref="InterruptedException"/>
        public PullResult pull(MessageQueue mq, MessageSelector messageSelector, long offset, int maxNums)
        {
            return pull(mq, messageSelector, offset, maxNums, this.defaultMQPullConsumer.getConsumerPullTimeoutMillis());
        }

        ///<exception cref="MQClientException"/>
        ///<exception cref="RemotingException"/>
        ///<exception cref="MQBrokerException"/>
        ///<exception cref="InterruptedException"/>
        public PullResult pull(MessageQueue mq, MessageSelector messageSelector, long offset, int maxNums, long timeout)
        {
            SubscriptionData subscriptionData = getSubscriptionData(mq, messageSelector);
            return this.pullSyncImpl(mq, subscriptionData, offset, maxNums, false, timeout);
        }

        ///<exception cref="MQClientException"/>
        private SubscriptionData getSubscriptionData(MessageQueue mq, string subExpression)
        {

            if (null == mq)
            {
                throw new MQClientException("mq is null", null);
            }

            try
            {
                return FilterAPI.buildSubscriptionData(mq.getTopic(), subExpression);
            }
            catch (Exception e)
            {
                throw new MQClientException("parse subscription error", e);
            }
        }

        ///<exception cref="MQClientException"/>
        private SubscriptionData getSubscriptionData(MessageQueue mq, MessageSelector messageSelector)
        {

            if (null == mq)
            {
                throw new MQClientException("mq is null", null);
            }

            try
            {
                return FilterAPI.build(mq.getTopic(),
                    messageSelector.getExpression(), messageSelector.getExpressionType());
            }
            catch (Exception e)
            {
                throw new MQClientException("parse subscription error", e);
            }
        }

        ///<exception cref="MQClientException"/>
        ///<exception cref="RemotingException"/>
        ///<exception cref="MQBrokerException"/>
        ///<exception cref="InterruptedException"/>
        private PullResult pullSyncImpl(MessageQueue mq, SubscriptionData subscriptionData, long offset, int maxNums, bool block,
    long timeout)
        {
            this.isRunning();

            if (null == mq)
            {
                throw new MQClientException("mq is null", null);
            }

            if (offset < 0)
            {
                throw new MQClientException("offset < 0", null);
            }

            if (maxNums <= 0)
            {
                throw new MQClientException("maxNums <= 0", null);
            }

            this.subscriptionAutomatically(mq.getTopic());

            int sysFlag = PullSysFlag.buildSysFlag(false, block, true, false);

            long timeoutMillis = block ? this.defaultMQPullConsumer.getConsumerTimeoutMillisWhenSuspend() : timeout;

            bool isTagType = ExpressionType.isTagType(subscriptionData.expressionType);
            PullResult pullResult = this.pullAPIWrapper.pullKernelImpl(
                mq,
                subscriptionData.subString,
                subscriptionData.expressionType,
                isTagType ? 0L : subscriptionData.subVersion,
                offset,
                maxNums,
                sysFlag,
                0,
                this.defaultMQPullConsumer.getBrokerSuspendMaxTimeMillis(),
                timeoutMillis,
                CommunicationMode.SYNC,
                null
            );
            this.pullAPIWrapper.processPullResult(mq, pullResult, subscriptionData);
            //If namespace is not null , reset Topic without namespace.
            this.resetTopic(pullResult.getMsgFoundList());
            if (!this.consumeMessageHookList.IsEmpty())
            {
                ConsumeMessageContext consumeMessageContext = null;
                consumeMessageContext = new ConsumeMessageContext();
                consumeMessageContext.setNamespace(defaultMQPullConsumer.getNamespace());
                consumeMessageContext.setConsumerGroup(this.groupName());
                consumeMessageContext.setMq(mq);
                consumeMessageContext.setMsgList(pullResult.getMsgFoundList());
                consumeMessageContext.setSuccess(false);
                this.executeHookBefore(consumeMessageContext);
                consumeMessageContext.setStatus(ConsumeConcurrentlyStatus.CONSUME_SUCCESS.ToString());
                consumeMessageContext.setSuccess(true);
                this.executeHookAfter(consumeMessageContext);
            }
            return pullResult;
        }

        public void resetTopic(List<MessageExt> msgList)
        {
            if (null == msgList || msgList.Count == 0)
            {
                return;
            }

            //If namespace not null , reset Topic without namespace.
            foreach (MessageExt messageExt in msgList)
            {
                if (null != this.getDefaultMQPullConsumer().getNamespace())
                {
                    messageExt.setTopic(NamespaceUtil.withoutNamespace(messageExt.getTopic(), this.defaultMQPullConsumer.getNamespace()));
                }
            }

        }

        public void subscriptionAutomatically(String topic)
        {
            if (!this.rebalanceImpl.getSubscriptionInner().ContainsKey(topic))
            {
                try
                {
                    SubscriptionData subscriptionData = FilterAPI.buildSubscriptionData(topic, SubscriptionData.SUB_ALL);
                    this.rebalanceImpl.subscriptionInner.PutIfAbsent(topic, subscriptionData);
                }
                catch (Exception ignore)
                {
                }
            }
        }

        public void unsubscribe(String topic)
        {
            //this.rebalanceImpl.getSubscriptionInner().remove(topic);
            this.rebalanceImpl.getSubscriptionInner().TryRemove(topic, out _);
        }

        //@Override
        public string groupName()
        {
            return this.defaultMQPullConsumer.getConsumerGroup();
        }

        public void executeHookBefore(ConsumeMessageContext context)
        {
            if (!this.consumeMessageHookList.IsEmpty())
            {
                foreach (ConsumeMessageHook hook in this.consumeMessageHookList)
                {
                    try
                    {
                        hook.consumeMessageBefore(context);
                    }
                    catch (Exception ignored)
                    {
                    }
                }
            }
        }

        public void executeHookAfter(ConsumeMessageContext context)
        {
            if (!this.consumeMessageHookList.IsEmpty())
            {
                foreach (ConsumeMessageHook hook in this.consumeMessageHookList)
                {
                    try
                    {
                        hook.consumeMessageAfter(context);
                    }
                    catch (Exception ignored)
                    {
                    }
                }
            }
        }

        //@Override
        public MessageModel messageModel()
        {
            return this.defaultMQPullConsumer.getMessageModel();
        }

        //@Override
        public ConsumeType consumeType()
        {
            return ConsumeType.CONSUME_ACTIVELY;
        }

        //@Override
        public ConsumeFromWhere consumeFromWhere()
        {
            return ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET;
        }

        //@Override
        public HashSet<SubscriptionData> subscriptions()
        {
            HashSet<SubscriptionData> result = new HashSet<SubscriptionData>();

            HashSet<String> topics = this.defaultMQPullConsumer.getRegisterTopics();
            if (topics != null)
            {
                lock (topics)
                {
                    foreach (String t in topics)
                    {
                        SubscriptionData ms = null;
                        try
                        {
                            ms = FilterAPI.buildSubscriptionData(t, SubscriptionData.SUB_ALL);
                        }
                        catch (Exception e)
                        {
                            log.Error("parse subscription error", e.ToString());
                        }
                        ms.subVersion = 0L;
                        result.Add(ms);
                    }
                }
            }

            return result;
        }

        //@Override
        public void doRebalance()
        {
            if (this.rebalanceImpl != null)
            {
                this.rebalanceImpl.doRebalance(false);
            }
        }

        //@Override
        public void persistConsumerOffset()
        {
            try
            {
                this.isRunning();
                //HashSet<MessageQueue> mqs = new HashSet<MessageQueue>();
                //HashSet<MessageQueue> allocateMq = this.rebalanceImpl.getProcessQueueTable().keySet();
                //mqs.addAll(allocateMq);
                HashSet<MessageQueue> mqs = this.rebalanceImpl.getProcessQueueTable().Keys.ToHashSet();
                this.offsetStore.persistAll(mqs);
            }
            catch (Exception e)
            {
                log.Error("group: " + this.defaultMQPullConsumer.getConsumerGroup() + " persistConsumerOffset exception", e.ToString());
            }
        }

        //@Override
        public void updateTopicSubscribeInfo(String topic, HashSet<MessageQueue> info)
        {
            var subTable = this.rebalanceImpl.getSubscriptionInner();
            if (subTable != null)
            {
                if (subTable.ContainsKey(topic))
                {
                    //this.rebalanceImpl.getTopicSubscribeInfoTable().put(topic, info)
                    this.rebalanceImpl.getTopicSubscribeInfoTable()[topic] = info;
                }
            }
        }

        //@Override
        public bool isSubscribeTopicNeedUpdate(String topic)
        {
            var subTable = this.rebalanceImpl.getSubscriptionInner();
            if (subTable != null)
            {
                if (subTable.ContainsKey(topic))
                {
                    return !this.rebalanceImpl.topicSubscribeInfoTable.ContainsKey(topic);
                }
            }

            return false;
        }

        //@Override
        public bool isUnitMode()
        {
            return this.defaultMQPullConsumer.isUnitMode();
        }

        //@Override
        public ConsumerRunningInfo consumerRunningInfo()
        {
            ConsumerRunningInfo info = new ConsumerRunningInfo();

            Properties prop = MixAll.object2Properties(this.defaultMQPullConsumer);
            prop.Put(ConsumerRunningInfo.PROP_CONSUMER_START_TIMESTAMP, Str.valueOf(this.consumerStartTimestamp));
            info.properties = prop;

            info.subscriptionSet.AddAll(this.subscriptions());
            return info;
        }

        ///<exception cref="MQClientException"/>
        ///<exception cref="RemotingException"/>
        ///<exception cref="InterruptedException"/>
        public void pull(MessageQueue mq, string subExpression, long offset, int maxNums, PullCallback pullCallback)
        {
            pull(mq, subExpression, offset, maxNums, pullCallback, this.defaultMQPullConsumer.getConsumerPullTimeoutMillis());
        }

        ///<exception cref="MQClientException"/>
        ///<exception cref="RemotingException"/>
        ///<exception cref="InterruptedException"/>
        public void pull(MessageQueue mq, string subExpression, long offset, int maxNums, PullCallback pullCallback,
        long timeout)
        {
            SubscriptionData subscriptionData = getSubscriptionData(mq, subExpression);
            this.pullAsyncImpl(mq, subscriptionData, offset, maxNums, pullCallback, false, timeout);
        }

        ///<exception cref="MQClientException"/>
        ///<exception cref="RemotingException"/>
        ///<exception cref="InterruptedException"/>
        public void pull(MessageQueue mq, MessageSelector messageSelector, long offset, int maxNums,
        PullCallback pullCallback)
        {
            pull(mq, messageSelector, offset, maxNums, pullCallback, this.defaultMQPullConsumer.getConsumerPullTimeoutMillis());
        }

        ///<exception cref="MQClientException"/>
        ///<exception cref="RemotingException"/>
        ///<exception cref="InterruptedException"/>
        public void pull(MessageQueue mq, MessageSelector messageSelector, long offset, int maxNums,
        PullCallback pullCallback,
        long timeout)
        {
            SubscriptionData subscriptionData = getSubscriptionData(mq, messageSelector);
            this.pullAsyncImpl(mq, subscriptionData, offset, maxNums, pullCallback, false, timeout);
        }

        ///<exception cref="MQClientException"/>
        ///<exception cref="RemotingException"/>
        ///<exception cref="InterruptedException"/>
        private void pullAsyncImpl(
        MessageQueue mq,
        SubscriptionData subscriptionData,
        long offset,
        int maxNums,
        PullCallback pullCallback,
        bool block,
        long timeout)
        {
            this.isRunning();

            if (null == mq)
            {
                throw new MQClientException("mq is null", null);
            }

            if (offset < 0)
            {
                throw new MQClientException("offset < 0", null);
            }

            if (maxNums <= 0)
            {
                throw new MQClientException("maxNums <= 0", null);
            }

            if (null == pullCallback)
            {
                throw new MQClientException("pullCallback is null", null);
            }

            this.subscriptionAutomatically(mq.getTopic());

            try
            {
                int sysFlag = PullSysFlag.buildSysFlag(false, block, true, false);

                long timeoutMillis = block ? this.defaultMQPullConsumer.getConsumerTimeoutMillisWhenSuspend() : timeout;

                bool isTagType = ExpressionType.isTagType(subscriptionData.expressionType);
                this.pullAPIWrapper.pullKernelImpl(
                    mq,
                    subscriptionData.subString,
                    subscriptionData.expressionType,
                    isTagType ? 0L : subscriptionData.subVersion,
                    offset,
                    maxNums,
                    sysFlag,
                    0,
                    this.defaultMQPullConsumer.getBrokerSuspendMaxTimeMillis(),
                    timeoutMillis,
                    CommunicationMode.ASYNC,
                    new PullCallback()
                    {

                        //public void onSuccess(PullResult pullResult)
                        OnSuccess = (pullResult) =>
                        {
                            PullResult userPullResult = pullAPIWrapper.processPullResult(mq, pullResult, subscriptionData);
                            resetTopic(userPullResult.getMsgFoundList());
                            pullCallback.OnSuccess(userPullResult);
                        },

                        //public void onException(Throwable e)
                        OnException = (e) =>
                         {
                             pullCallback.OnException(e);
                         }
                    });
            }
            catch (MQBrokerException e)
            {
                throw new MQClientException("pullAsync unknow exception", e);
            }
        }

        ///<exception cref="MQClientException"/>
        ///<exception cref="RemotingException"/>
        ///<exception cref="MQBrokerException"/>
        ///<exception cref="InterruptedException"/>
        public PullResult pullBlockIfNotFound(MessageQueue mq, string subExpression, long offset, int maxNums)
        {
            SubscriptionData subscriptionData = getSubscriptionData(mq, subExpression);
            return this.pullSyncImpl(mq, subscriptionData, offset, maxNums, true, this.getDefaultMQPullConsumer().getConsumerPullTimeoutMillis());
        }

        public DefaultMQPullConsumer getDefaultMQPullConsumer()
        {
            return defaultMQPullConsumer;
        }

        ///<exception cref="MQClientException"/>
        ///<exception cref="RemotingException"/>
        ///<exception cref="InterruptedException"/>
        public void pullBlockIfNotFound(MessageQueue mq, string subExpression, long offset, int maxNums,
    PullCallback pullCallback)
        {
            SubscriptionData subscriptionData = getSubscriptionData(mq, subExpression);
            this.pullAsyncImpl(mq, subscriptionData, offset, maxNums, pullCallback, true,
                this.getDefaultMQPullConsumer().getConsumerPullTimeoutMillis());
        }

        ///<exception cref="MQClientException"/>
        ///<exception cref="InterruptedException"/>
        public QueryResult queryMessage(String topic, string key, int maxNum, long begin, long end)
        {
            this.isRunning();
            return this.mQClientFactory.getMQAdminImpl().queryMessage(topic, key, maxNum, begin, end);
        }

        ///<exception cref="MQClientException"/>
        ///<exception cref="InterruptedException"/>
        public MessageExt queryMessageByUniqKey(String topic, string uniqKey)
        {
            this.isRunning();
            return this.mQClientFactory.getMQAdminImpl().queryMessageByUniqKey(topic, uniqKey);
        }

        ///<exception cref="MQClientException"/>
        public long searchOffset(MessageQueue mq, long timestamp)
        {
            this.isRunning();
            return this.mQClientFactory.getMQAdminImpl().searchOffset(mq, timestamp);
        }

        ///<exception cref="RemotingException"/>
        ///<exception cref="MQBrokerException"/>
        ///<exception cref="InterruptedException"/>
        ///<exception cref="MQClientException"/>
        public void sendMessageBack(MessageExt msg, int delayLevel, string brokerName)
        {
            sendMessageBack(msg, delayLevel, brokerName, this.defaultMQPullConsumer.getConsumerGroup());
        }

        ///<exception cref="RemotingException"/>
        ///<exception cref="MQBrokerException"/>
        ///<exception cref="InterruptedException"/>
        ///<exception cref="MQClientException"/>
        public void updateConsumeOffsetToBroker(MessageQueue mq, long offset, bool isOneway)
        {
            this.offsetStore.updateConsumeOffsetToBroker(mq, offset, isOneway);
        }

        ///<exception cref="RemotingException"/>
        ///<exception cref="MQBrokerException"/>
        ///<exception cref="InterruptedException"/>
        ///<exception cref="MQClientException"/>
        public void sendMessageBack(MessageExt msg, int delayLevel, string brokerName, string consumerGroup)
        {
            try
            {
                string brokerAddr = (null != brokerName) ? this.mQClientFactory.findBrokerAddressInPublish(brokerName)
                    : RemotingHelper.parseSocketAddressAddr(msg.getStoreHost());

                if (UtilAll.isBlank(consumerGroup))
                {
                    consumerGroup = this.defaultMQPullConsumer.getConsumerGroup();
                }

                this.mQClientFactory.getMQClientAPIImpl().consumerSendMessageBack(brokerAddr, msg, consumerGroup, delayLevel, 3000,
                    this.defaultMQPullConsumer.getMaxReconsumeTimes());
            }
            catch (Exception e)
            {
                log.Error("sendMessageBack Exception, " + this.defaultMQPullConsumer.getConsumerGroup(), e.ToString());

                Message newMsg = new Message(MixAll.getRetryTopic(this.defaultMQPullConsumer.getConsumerGroup()), msg.getBody());
                string originMsgId = MessageAccessor.getOriginMessageId(msg);
                MessageAccessor.setOriginMessageId(newMsg, UtilAll.isBlank(originMsgId) ? msg.getMsgId() : originMsgId);
                newMsg.setFlag(msg.getFlag());
                MessageAccessor.setProperties(newMsg, msg.getProperties());
                MessageAccessor.putProperty(newMsg, MessageConst.PROPERTY_RETRY_TOPIC, msg.getTopic());
                MessageAccessor.setReconsumeTime(newMsg, Str.valueOf(msg.getReconsumeTimes() + 1));
                MessageAccessor.setMaxReconsumeTimes(newMsg, Str.valueOf(this.defaultMQPullConsumer.getMaxReconsumeTimes()));
                newMsg.setDelayTimeLevel(3 + msg.getReconsumeTimes());
                this.mQClientFactory.getDefaultMQProducer().send(newMsg);
            }
            finally
            {
                msg.setTopic(NamespaceUtil.withoutNamespace(msg.getTopic(), this.defaultMQPullConsumer.getNamespace()));
            }
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public void shutdown()
        {
            switch (this.serviceState)
            {
                case ServiceState.CREATE_JUST:
                    break;
                case ServiceState.RUNNING:
                    this.persistConsumerOffset();
                    this.mQClientFactory.unregisterConsumer(this.defaultMQPullConsumer.getConsumerGroup());
                    this.mQClientFactory.shutdown();
                    log.Info("the consumer [{}] shutdown OK", this.defaultMQPullConsumer.getConsumerGroup());
                    this.serviceState = ServiceState.SHUTDOWN_ALREADY;
                    break;
                case ServiceState.SHUTDOWN_ALREADY:
                    break;
                default:
                    break;
            }
        }

        ///<exception cref="MQClientException"/>
        [MethodImpl(MethodImplOptions.Synchronized)]
        public void start()
        {
            switch (this.serviceState)
            {
                case ServiceState.CREATE_JUST:
                    this.serviceState = ServiceState.START_FAILED;

                    this.checkConfig();

                    this.copySubscription();

                    if (this.defaultMQPullConsumer.getMessageModel() == MessageModel.CLUSTERING)
                    {
                        this.defaultMQPullConsumer.changeInstanceNameToPID();
                    }

                    this.mQClientFactory = MQClientManager.getInstance().getOrCreateMQClientInstance(this.defaultMQPullConsumer, this.rpcHook);

                    this.rebalanceImpl.setConsumerGroup(this.defaultMQPullConsumer.getConsumerGroup());
                    this.rebalanceImpl.setMessageModel(this.defaultMQPullConsumer.getMessageModel());
                    this.rebalanceImpl.setAllocateMessageQueueStrategy(this.defaultMQPullConsumer.getAllocateMessageQueueStrategy());
                    this.rebalanceImpl.setmQClientFactory(this.mQClientFactory);

                    this.pullAPIWrapper = new PullAPIWrapper(
                        mQClientFactory,
                        this.defaultMQPullConsumer.getConsumerGroup(), isUnitMode());
                    this.pullAPIWrapper.registerFilterMessageHook(filterMessageHookList);

                    if (this.defaultMQPullConsumer.getOffsetStore() != null)
                    {
                        this.offsetStore = this.defaultMQPullConsumer.getOffsetStore();
                    }
                    else
                    {
                        switch (this.defaultMQPullConsumer.getMessageModel())
                        {
                            case MessageModel.BROADCASTING:
                                this.offsetStore = new LocalFileOffsetStore(this.mQClientFactory, this.defaultMQPullConsumer.getConsumerGroup());
                                break;
                            case MessageModel.CLUSTERING:
                                this.offsetStore = new RemoteBrokerOffsetStore(this.mQClientFactory, this.defaultMQPullConsumer.getConsumerGroup());
                                break;
                            default:
                                break;
                        }
                        this.defaultMQPullConsumer.setOffsetStore(this.offsetStore);
                    }

                    this.offsetStore.load();

                    bool registerOK = mQClientFactory.registerConsumer(this.defaultMQPullConsumer.getConsumerGroup(), this);
                    if (!registerOK)
                    {
                        this.serviceState = ServiceState.CREATE_JUST;

                        throw new MQClientException("The consumer group[" + this.defaultMQPullConsumer.getConsumerGroup()
                            + "] has been created before, specify another name please." + FAQUrl.suggestTodo(FAQUrl.GROUP_NAME_DUPLICATE_URL),
                            null);
                    }

                    mQClientFactory.start();
                    log.Info("the consumer [{}] start OK", this.defaultMQPullConsumer.getConsumerGroup());
                    this.serviceState = ServiceState.RUNNING;
                    break;
                case ServiceState.RUNNING:
                case ServiceState.START_FAILED:
                case ServiceState.SHUTDOWN_ALREADY:
                    throw new MQClientException("The PullConsumer service state not OK, maybe started once, "
                        + this.serviceState
                        + FAQUrl.suggestTodo(FAQUrl.CLIENT_SERVICE_NOT_OK),
                        null);
                default:
                    break;
            }

        }

        ///<exception cref="MQClientException"/>
        private void checkConfig()
        {
            // check consumerGroup
            Validators.checkGroup(this.defaultMQPullConsumer.getConsumerGroup());

            // consumerGroup
            if (null == this.defaultMQPullConsumer.getConsumerGroup())
            {
                throw new MQClientException(
                    "consumerGroup is null"
                        + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                    null);
            }

            // consumerGroup
            if (this.defaultMQPullConsumer.getConsumerGroup().Equals(MixAll.DEFAULT_CONSUMER_GROUP))
            {
                throw new MQClientException(
                    "consumerGroup can not equal "
                        + MixAll.DEFAULT_CONSUMER_GROUP
                        + ", please specify another one."
                        + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                    null);
            }

            // messageModel
            if (null == this.defaultMQPullConsumer.getMessageModel())
            {
                throw new MQClientException(
                    "messageModel is null"
                        + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                    null);
            }

            // allocateMessageQueueStrategy
            if (null == this.defaultMQPullConsumer.getAllocateMessageQueueStrategy())
            {
                throw new MQClientException(
                    "allocateMessageQueueStrategy is null"
                        + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                    null);
            }

            // allocateMessageQueueStrategy
            if (this.defaultMQPullConsumer.getConsumerTimeoutMillisWhenSuspend() < this.defaultMQPullConsumer.getBrokerSuspendMaxTimeMillis())
            {
                throw new MQClientException(
                    "Long polling mode, the consumer consumerTimeoutMillisWhenSuspend must greater than brokerSuspendMaxTimeMillis"
                        + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                    null);
            }
        }

        ///<exception cref="MQClientException"/>
        private void copySubscription()
        {
            try
            {
                HashSet<String> registerTopics = this.defaultMQPullConsumer.getRegisterTopics();
                if (registerTopics != null)
                {
                    foreach (String topic in registerTopics)
                    {
                        SubscriptionData subscriptionData = FilterAPI.buildSubscriptionData(topic, SubscriptionData.SUB_ALL);
                        //this.rebalanceImpl.getSubscriptionInner().put(topic, subscriptionData);
                        this.rebalanceImpl.getSubscriptionInner()[topic] = subscriptionData;
                    }
                }
            }
            catch (Exception e)
            {
                throw new MQClientException("subscription exception", e);
            }
        }

        ///<exception cref="MQClientException"/>
        public void updateConsumeOffset(MessageQueue mq, long offset)
        {
            this.isRunning();
            this.offsetStore.updateOffset(mq, offset, false);
        }

        ///<exception cref="RemotingException"/>
        ///<exception cref="MQBrokerException"/>
        ///<exception cref="InterruptedException"/>
        ///<exception cref="MQClientException"/>
        public MessageExt viewMessage(String msgId)
        {
            this.isRunning();
            return this.mQClientFactory.getMQAdminImpl().viewMessage(msgId);
        }

        public void registerFilterMessageHook(FilterMessageHook hook)
        {
            this.filterMessageHookList.Add(hook);
            log.Info("register FilterMessageHook Hook, {}", hook.hookName());
        }

        public OffsetStore getOffsetStore()
        {
            return offsetStore;
        }

        public void setOffsetStore(OffsetStore offsetStore)
        {
            this.offsetStore = offsetStore;
        }

        public PullAPIWrapper getPullAPIWrapper()
        {
            return pullAPIWrapper;
        }

        public void setPullAPIWrapper(PullAPIWrapper pullAPIWrapper)
        {
            this.pullAPIWrapper = pullAPIWrapper;
        }

        public ServiceState getServiceState()
        {
            return serviceState;
        }

        //Don't use this deprecated setter, which will be removed soon.
        [Obsolete]//@Deprecated
        public void setServiceState(ServiceState serviceState)
        {
            this.serviceState = serviceState;
        }

        public long getConsumerStartTimestamp()
        {
            return consumerStartTimestamp;
        }

        public RebalanceImpl getRebalanceImpl()
        {
            return rebalanceImpl;
        }
    }
}
