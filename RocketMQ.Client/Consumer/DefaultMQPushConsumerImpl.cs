using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;

namespace RocketMQ.Client
{
    public class DefaultMQPushConsumerImpl : MQConsumerInner
    {
        /**
     * Delay some time when exception occur
     */
        private long pullTimeDelayMillsWhenException = 3000;
        /**
         * Flow control interval
         */
        private static readonly long PULL_TIME_DELAY_MILLS_WHEN_FLOW_CONTROL = 50;
        /**
         * Delay some time when suspend pull service
         */
        private static readonly long PULL_TIME_DELAY_MILLS_WHEN_SUSPEND = 1000;
        private static readonly long BROKER_SUSPEND_MAX_TIME_MILLIS = 1000 * 15;
        private static readonly long CONSUMER_TIMEOUT_MILLIS_WHEN_SUSPEND = 1000 * 30;
        //private readonly InternalLogger log = ClientLogger.getLog();
        static NLog.Logger log = NLog.LogManager.GetCurrentClassLogger();
        private readonly DefaultMQPushConsumer defaultMQPushConsumer;
        private readonly RebalanceImpl rebalanceImpl;//= new RebalancePushImpl(this);
        private readonly List<FilterMessageHook> filterMessageHookList = new List<FilterMessageHook>();
        private readonly long consumerStartTimestamp = Sys.currentTimeMillis();
        private readonly List<ConsumeMessageHook> consumeMessageHookList = new List<ConsumeMessageHook>();
        private readonly RPCHook rpcHook;
        private volatile ServiceState serviceState = ServiceState.CREATE_JUST;
        private MQClientInstance mQClientFactory;
        private PullAPIWrapper pullAPIWrapper;
        private volatile bool pause = false;
        private bool consumeOrderly = false;
        private MessageListener messageListenerInner;
        private OffsetStore offsetStore;
        private ConsumeMessageService consumeMessageService;
        private long queueFlowControlTimes = 0;
        private long queueMaxSpanFlowControlTimes = 0;

        public DefaultMQPushConsumerImpl(DefaultMQPushConsumer defaultMQPushConsumer, RPCHook rpcHook)
        {
            rebalanceImpl = new RebalancePushImpl(this);
            this.defaultMQPushConsumer = defaultMQPushConsumer;
            this.rpcHook = rpcHook;
            this.pullTimeDelayMillsWhenException = defaultMQPushConsumer.getPullTimeDelayMillsWhenException();
        }

        public void registerFilterMessageHook(FilterMessageHook hook)
        {
            this.filterMessageHookList.Add(hook);
            log.Info("register FilterMessageHook Hook, {}", hook.hookName());
        }

        public bool hasHook()
        {
            return !this.consumeMessageHookList.isEmpty();
        }

        public void registerConsumeMessageHook(ConsumeMessageHook hook)
        {
            this.consumeMessageHookList.Add(hook);
            log.Info("register consumeMessageHook Hook, {}", hook.hookName());
        }

        public void executeHookBefore(ConsumeMessageContext context)
        {
            if (!this.consumeMessageHookList.isEmpty())
            {
                foreach (ConsumeMessageHook hook in this.consumeMessageHookList)
                {
                    try
                    {
                        hook.consumeMessageBefore(context);
                    }
                    catch (Exception e)
                    {
                    }
                }
            }
        }

        public void executeHookAfter(ConsumeMessageContext context)
        {
            if (!this.consumeMessageHookList.isEmpty())
            {
                foreach (ConsumeMessageHook hook in this.consumeMessageHookList)
                {
                    try
                    {
                        hook.consumeMessageAfter(context);
                    }
                    catch (Exception e)
                    {
                    }
                }
            }
        }

        ///<exception cref="MQClientException"/>
        public void createTopic(String key, String newTopic, int queueNum)
        {
            createTopic(key, newTopic, queueNum, 0);
        }

        ///<exception cref="MQClientException"/>
        public void createTopic(String key, String newTopic, int queueNum, int topicSysFlag)
        {
            this.mQClientFactory.getMQAdminImpl().createTopic(key, newTopic, queueNum, topicSysFlag);
        }

        ///<exception cref="MQClientException"/>
        public HashSet<MessageQueue> fetchSubscribeMessageQueues(String topic)
        {
            //HashSet<MessageQueue> result = this.rebalanceImpl.getTopicSubscribeInfoTable().get(topic);
            this.rebalanceImpl.getTopicSubscribeInfoTable().TryGetValue(topic, out HashSet<MessageQueue> result);
            if (null == result)
            {
                this.mQClientFactory.updateTopicRouteInfoFromNameServer(topic);
                //result = this.rebalanceImpl.getTopicSubscribeInfoTable().get(topic);
                this.rebalanceImpl.getTopicSubscribeInfoTable().TryGetValue(topic, out result);
            }

            if (null == result)
            {
                throw new MQClientException("The topic[" + topic + "] not exist", null);
            }

            return parseSubscribeMessageQueues(result);
        }

        public HashSet<MessageQueue> parseSubscribeMessageQueues(HashSet<MessageQueue> messageQueueList)
        {
            HashSet<MessageQueue> resultQueues = new HashSet<MessageQueue>();
            foreach (MessageQueue queue in messageQueueList)
            {
                String userTopic = NamespaceUtil.withoutNamespace(queue.getTopic(), this.defaultMQPushConsumer.getNamespace());
                resultQueues.Add(new MessageQueue(userTopic, queue.getBrokerName(), queue.getQueueId()));
            }

            return resultQueues;
        }

        public DefaultMQPushConsumer getDefaultMQPushConsumer()
        {
            return defaultMQPushConsumer;
        }

        ///<exception cref="MQClientException"/>
        public long earliestMsgStoreTime(MessageQueue mq)
        {
            return this.mQClientFactory.getMQAdminImpl().earliestMsgStoreTime(mq);
        }

        ///<exception cref="MQClientException"/>
        public long maxOffset(MessageQueue mq)
        {
            return this.mQClientFactory.getMQAdminImpl().maxOffset(mq);
        }

        ///<exception cref="MQClientException"/>
        public long minOffset(MessageQueue mq)
        {
            return this.mQClientFactory.getMQAdminImpl().minOffset(mq);
        }

        public OffsetStore getOffsetStore()
        {
            return offsetStore;
        }

        public void setOffsetStore(OffsetStore offsetStore)
        {
            this.offsetStore = offsetStore;
        }

        public void pullMessage(PullRequest pullRequest)
        {
            ProcessQueue processQueue = pullRequest.getProcessQueue();
            if (processQueue.isDropped())
            {
                log.Info("the pull request[{}] is dropped.", pullRequest.ToString());
                return;
            }

            pullRequest.getProcessQueue().LastPullTimestamp = Sys.currentTimeMillis();

            try
            {
                this.makeSureStateOK();
            }
            catch (MQClientException e)
            {
                log.Warn("pullMessage exception, consumer state not ok", e);
                this.executePullRequestLater(pullRequest, pullTimeDelayMillsWhenException);
                return;
            }

            if (this.isPause())
            {
                log.Warn("consumer was paused, execute pull request later. instanceName={}, group={}", this.defaultMQPushConsumer.getInstanceName(), this.defaultMQPushConsumer.getConsumerGroup());
                this.executePullRequestLater(pullRequest, PULL_TIME_DELAY_MILLS_WHEN_SUSPEND);
                return;
            }

            long cachedMessageCount = processQueue.getMsgCount().get();
            long cachedMessageSizeInMiB = processQueue.getMsgSize().get() / (1024 * 1024);

            if (cachedMessageCount > this.defaultMQPushConsumer.getPullThresholdForQueue())
            {
                this.executePullRequestLater(pullRequest, PULL_TIME_DELAY_MILLS_WHEN_FLOW_CONTROL);
                if ((queueFlowControlTimes++ % 1000) == 0)
                {
                    log.Warn(
                        "the cached message count exceeds the threshold {}, so do flow control, minOffset={}, maxOffset={}, count={}, size={} MiB, pullRequest={}, flowControlTimes={}",
                        this.defaultMQPushConsumer.getPullThresholdForQueue(), processQueue.getMsgTreeMap().First().Key, processQueue.getMsgTreeMap().Last().Key, cachedMessageCount, cachedMessageSizeInMiB, pullRequest, queueFlowControlTimes);
                }
                return;
            }

            if (cachedMessageSizeInMiB > this.defaultMQPushConsumer.getPullThresholdSizeForQueue())
            {
                this.executePullRequestLater(pullRequest, PULL_TIME_DELAY_MILLS_WHEN_FLOW_CONTROL);
                if ((queueFlowControlTimes++ % 1000) == 0)
                {
                    log.Warn(
                        "the cached message size exceeds the threshold {} MiB, so do flow control, minOffset={}, maxOffset={}, count={}, size={} MiB, pullRequest={}, flowControlTimes={}",
                        this.defaultMQPushConsumer.getPullThresholdSizeForQueue(), processQueue.getMsgTreeMap().First().Key, processQueue.getMsgTreeMap().Last().Key, cachedMessageCount, cachedMessageSizeInMiB, pullRequest, queueFlowControlTimes);
                }
                return;
            }

            if (!this.consumeOrderly)
            {
                if (processQueue.getMaxSpan() > this.defaultMQPushConsumer.getConsumeConcurrentlyMaxSpan())
                {
                    this.executePullRequestLater(pullRequest, PULL_TIME_DELAY_MILLS_WHEN_FLOW_CONTROL);
                    if ((queueMaxSpanFlowControlTimes++ % 1000) == 0)
                    {
                        log.Warn(
                            "the queue's messages, span too long, so do flow control, minOffset={}, maxOffset={}, maxSpan={}, pullRequest={}, flowControlTimes={}",
                            processQueue.getMsgTreeMap().First().Key, processQueue.getMsgTreeMap().Last().Key, processQueue.getMaxSpan(),
                            pullRequest, queueMaxSpanFlowControlTimes);
                    }
                    return;
                }
            }
            else
            {
                if (processQueue.isLocked())
                {
                    if (!pullRequest.isPreviouslyLocked())
                    {
                        long offset = -1L;
                        try
                        {
                            offset = this.rebalanceImpl.computePullFromWhereWithException(pullRequest.getMessageQueue());
                        }
                        catch (Exception e)
                        {
                            this.executePullRequestLater(pullRequest, pullTimeDelayMillsWhenException);
                            log.Error("Failed to compute pull offset, pullResult: {}", pullRequest, e);
                            return;
                        }
                        bool brokerBusy = offset < pullRequest.getNextOffset();
                        log.Info("the first time to pull message, so fix offset from broker. pullRequest: {} NewOffset: {} brokerBusy: {}",
                            pullRequest, offset, brokerBusy);
                        if (brokerBusy)
                        {
                            log.Info("[NOTIFYME]the first time to pull message, but pull request offset larger than broker consume offset. pullRequest: {} NewOffset: {}",
                                pullRequest, offset);
                        }

                        pullRequest.setPreviouslyLocked(true);
                        pullRequest.setNextOffset(offset);
                    }
                }
                else
                {
                    this.executePullRequestLater(pullRequest, pullTimeDelayMillsWhenException);
                    log.Info("pull message later because not locked in broker, {}", pullRequest);
                    return;
                }
            }

            //SubscriptionData subscriptionData = this.rebalanceImpl.getSubscriptionInner().get(pullRequest.getMessageQueue().getTopic());

            this.rebalanceImpl.getSubscriptionInner().TryGetValue(pullRequest.getMessageQueue().getTopic(), out SubscriptionData subscriptionData);
            if (null == subscriptionData)
            {
                this.executePullRequestLater(pullRequest, pullTimeDelayMillsWhenException);
                log.Warn("find the consumer's subscription failed, {}", pullRequest);
                return;
            }

            long beginTimestamp = Sys.currentTimeMillis();

            PullCallback pullCallback = new PullCallback();
            {

                pullCallback.OnSuccess = (pullResult) =>
                {
                    if (pullResult != null)
                    {
                        pullResult = pullAPIWrapper.processPullResult(pullRequest.getMessageQueue(), pullResult, subscriptionData);

                        switch (pullResult.getPullStatus())
                        {
                            case PullStatus.FOUND:
                                long prevRequestOffset = pullRequest.getNextOffset();
                                pullRequest.setNextOffset(pullResult.getNextBeginOffset());
                                long pullRT = Sys.currentTimeMillis() - beginTimestamp;
                                getConsumerStatsManager().incPullRT(pullRequest.getConsumerGroup(),
                                    pullRequest.getMessageQueue().getTopic(), pullRT);

                                long firstMsgOffset = long.MaxValue;
                                if (pullResult.getMsgFoundList() == null || pullResult.getMsgFoundList().isEmpty())
                                {
                                    executePullRequestImmediately(pullRequest);
                                }
                                else
                                {
                                    firstMsgOffset = pullResult.getMsgFoundList().get(0).getQueueOffset();

                                    getConsumerStatsManager().incPullTPS(pullRequest.getConsumerGroup(),
                                        pullRequest.getMessageQueue().getTopic(), pullResult.getMsgFoundList().Count);

                                    bool dispatchToConsume = processQueue.putMessage(pullResult.getMsgFoundList());
                                    consumeMessageService.submitConsumeRequest(
                                        pullResult.getMsgFoundList(),
                                        processQueue,
                                        pullRequest.getMessageQueue(),
                                        dispatchToConsume);

                                    if (defaultMQPushConsumer.getPullInterval() > 0)
                                    {
                                        executePullRequestLater(pullRequest,
                                            defaultMQPushConsumer.getPullInterval());
                                    }
                                    else
                                    {
                                        executePullRequestImmediately(pullRequest);
                                    }
                                }

                                if (pullResult.getNextBeginOffset() < prevRequestOffset
                                    || firstMsgOffset < prevRequestOffset)
                                {
                                    log.Warn(
                                        "[BUG] pull message result maybe data wrong, nextBeginOffset: {} firstMsgOffset: {} prevRequestOffset: {}",
                                        pullResult.getNextBeginOffset(),
                                        firstMsgOffset,
                                        prevRequestOffset);
                                }

                                break;
                            case PullStatus.NO_NEW_MSG:
                            case PullStatus.NO_MATCHED_MSG:
                                pullRequest.setNextOffset(pullResult.getNextBeginOffset());
                                correctTagsOffset(pullRequest);
                                executePullRequestImmediately(pullRequest);
                                break;
                            case PullStatus.OFFSET_ILLEGAL:
                                log.Warn("the pull request offset illegal, {} {}",
                                    pullRequest.ToString(), pullResult.ToString());
                                pullRequest.setNextOffset(pullResult.getNextBeginOffset());

                                pullRequest.getProcessQueue().setDropped(true);
                                executeTaskLater(new Runnable()
                                {
                                    Run = () =>
                                    {
                                        try
                                        {
                                            offsetStore.updateOffset(pullRequest.getMessageQueue(),
                                                pullRequest.getNextOffset(), false);

                                            offsetStore.persist(pullRequest.getMessageQueue());

                                            rebalanceImpl.removeProcessQueue(pullRequest.getMessageQueue());

                                            log.Warn("fix the pull request offset, {}", pullRequest);
                                        }
                                        catch (Exception e)
                                        {
                                            log.Error("executeTaskLater Exception", e.ToString());
                                        }
                                    }
                                }, 10000);
                                break;
                            default:
                                break;
                        }
                    }
                };

                pullCallback.OnException = (e) =>
                {
                    if (!pullRequest.getMessageQueue().getTopic().StartsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX))
                    {
                        log.Warn("execute the pull request exception", e.ToString());
                    }
                    executePullRequestLater(pullRequest, pullTimeDelayMillsWhenException);
                };



                bool commitOffsetEnable = false;
                long commitOffsetValue = 0L;
                if (MessageModel.CLUSTERING == this.defaultMQPushConsumer.getMessageModel())
                {
                    commitOffsetValue = this.offsetStore.readOffset(pullRequest.getMessageQueue(), ReadOffsetType.READ_FROM_MEMORY);
                    if (commitOffsetValue > 0)
                    {
                        commitOffsetEnable = true;
                    }
                }

                String subExpression = null;
                bool classFilter = false;
                //SubscriptionData sd = this.rebalanceImpl.getSubscriptionInner().get(pullRequest.getMessageQueue().getTopic());
                this.rebalanceImpl.getSubscriptionInner().TryGetValue(pullRequest.getMessageQueue().getTopic(), out SubscriptionData sd);
                if (sd != null)
                {
                    if (this.defaultMQPushConsumer.isPostSubscriptionWhenPull() && !sd.classFilterMode)
                    {
                        subExpression = sd.subString;
                    }

                    classFilter = sd.classFilterMode;
                }

                int sysFlag = PullSysFlag.buildSysFlag(
                    commitOffsetEnable, // commitOffset
                    true, // suspend
                    subExpression != null, // subscription
                    classFilter // class filter
                );
                try
                {
                    this.pullAPIWrapper.pullKernelImpl(
                        pullRequest.getMessageQueue(),
                        subExpression,
                        subscriptionData.expressionType,
                        subscriptionData.subVersion,
                        pullRequest.getNextOffset(),
                        this.defaultMQPushConsumer.getPullBatchSize(),
                        sysFlag,
                        commitOffsetValue,
                        BROKER_SUSPEND_MAX_TIME_MILLIS,
                        CONSUMER_TIMEOUT_MILLIS_WHEN_SUSPEND,
                        CommunicationMode.ASYNC,
                        pullCallback
                    );
                }
                catch (Exception e)
                {
                    log.Error("pullKernelImpl exception", e.ToString());
                    this.executePullRequestLater(pullRequest, pullTimeDelayMillsWhenException);
                }
            }
        }

        ///<exception cref="MQClientException"/>
        private void makeSureStateOK()
        {
            if (this.serviceState != ServiceState.RUNNING)
            {
                throw new MQClientException("The consumer service state not OK, "
                    + this.serviceState
                    + FAQUrl.suggestTodo(FAQUrl.CLIENT_SERVICE_NOT_OK),
                    null);
            }
        }

        private void executePullRequestLater(PullRequest pullRequest, long timeDelay)
        {
            this.mQClientFactory.getPullMessageService().executePullRequestLater(pullRequest, timeDelay);
        }

        public bool isPause()
        {
            return pause;
        }

        public void setPause(bool pause)
        {
            this.pause = pause;
        }

        public ConsumerStatsManager getConsumerStatsManager()
        {
            return this.mQClientFactory.getConsumerStatsManager();
        }

        public void executePullRequestImmediately(PullRequest pullRequest)
        {
            this.mQClientFactory.getPullMessageService().executePullRequestImmediately(pullRequest);
        }

        private void correctTagsOffset(PullRequest pullRequest)
        {
            if (0L == pullRequest.getProcessQueue().getMsgCount().get())
            {
                this.offsetStore.updateOffset(pullRequest.getMessageQueue(), pullRequest.getNextOffset(), true);
            }
        }

        public void executeTaskLater(Runnable r, long timeDelay)
        {
            this.mQClientFactory.getPullMessageService().executeTaskLater(r, timeDelay);
        }

        ///<exception cref="MQClientException"/>
        ///<exception cref="InterruptedException"/>
        public QueryResult queryMessage(String topic, String key, int maxNum, long begin, long end)
        {
            return this.mQClientFactory.getMQAdminImpl().queryMessage(topic, key, maxNum, begin, end);
        }

        ///<exception cref="MQClientException"/>
        ///<exception cref="InterruptedException"/>
        public MessageExt queryMessageByUniqKey(String topic, String uniqKey)
        {
            return this.mQClientFactory.getMQAdminImpl().queryMessageByUniqKey(topic, uniqKey);
        }

        public void registerMessageListener(MessageListener messageListener)
        {
            this.messageListenerInner = messageListener;
        }

        public void resume()
        {
            this.pause = false;
            doRebalance();
            log.Info("resume this consumer, {}", this.defaultMQPushConsumer.getConsumerGroup());
        }

        ///<exception cref="RemotingException"/>
        ///<exception cref="MQBrokerException"/>
        ///<exception cref="InterruptedException"/>
        ///<exception cref="MQClientException"/>
        public void sendMessageBack(MessageExt msg, int delayLevel, String brokerName)
        {
            try
            {
                String brokerAddr = (null != brokerName) ? this.mQClientFactory.findBrokerAddressInPublish(brokerName)
                    : RemotingHelper.parseSocketAddressAddr(msg.getStoreHost());
                this.mQClientFactory.getMQClientAPIImpl().consumerSendMessageBack(brokerAddr, msg,
                    this.defaultMQPushConsumer.getConsumerGroup(), delayLevel, 5000, getMaxReconsumeTimes());
            }
            catch (Exception e)
            {
                log.Error("sendMessageBack Exception, " + this.defaultMQPushConsumer.getConsumerGroup(), e.ToString());

                Message newMsg = new Message(MixAll.getRetryTopic(this.defaultMQPushConsumer.getConsumerGroup()), msg.getBody());

                String originMsgId = MessageAccessor.getOriginMessageId(msg);
                MessageAccessor.setOriginMessageId(newMsg, UtilAll.isBlank(originMsgId) ? msg.getMsgId() : originMsgId);

                newMsg.setFlag(msg.getFlag());
                MessageAccessor.setProperties(newMsg, msg.getProperties());
                MessageAccessor.putProperty(newMsg, MessageConst.PROPERTY_RETRY_TOPIC, msg.getTopic());
                MessageAccessor.setReconsumeTime(newMsg, (msg.getReconsumeTimes() + 1).ToString());
                MessageAccessor.setMaxReconsumeTimes(newMsg, getMaxReconsumeTimes().ToString());
                MessageAccessor.clearProperty(newMsg, MessageConst.PROPERTY_TRANSACTION_PREPARED);
                newMsg.setDelayTimeLevel(3 + msg.getReconsumeTimes());

                this.mQClientFactory.getDefaultMQProducer().send(newMsg);
            }
            finally
            {
                msg.setTopic(NamespaceUtil.withoutNamespace(msg.getTopic(), this.defaultMQPushConsumer.getNamespace()));
            }
        }

        private int getMaxReconsumeTimes()
        {
            // default reconsume times: 16
            if (this.defaultMQPushConsumer.getMaxReconsumeTimes() == -1)
            {
                return 16;
            }
            else
            {
                return this.defaultMQPushConsumer.getMaxReconsumeTimes();
            }
        }

        public void shutdown()
        {
            shutdown(0);
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public void shutdown(long awaitTerminateMillis)
        {
            switch (this.serviceState)
            {
                case ServiceState.CREATE_JUST:
                    break;
                case ServiceState.RUNNING:
                    this.consumeMessageService.shutdown(awaitTerminateMillis);
                    this.persistConsumerOffset();
                    this.mQClientFactory.unregisterConsumer(this.defaultMQPushConsumer.getConsumerGroup());
                    this.mQClientFactory.shutdown();
                    log.Info("the consumer [{}] shutdown OK", this.defaultMQPushConsumer.getConsumerGroup());
                    this.rebalanceImpl.destroy();
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
                    log.Info("the consumer [{}] start beginning. messageModel={}, isUnitMode={}", this.defaultMQPushConsumer.getConsumerGroup(),
                        this.defaultMQPushConsumer.getMessageModel(), this.defaultMQPushConsumer.isUnitMode());
                    this.serviceState = ServiceState.START_FAILED;

                    this.checkConfig();

                    this.copySubscription();

                    if (this.defaultMQPushConsumer.getMessageModel() == MessageModel.CLUSTERING)
                    {
                        this.defaultMQPushConsumer.changeInstanceNameToPID();
                    }

                    this.mQClientFactory = MQClientManager.getInstance().getOrCreateMQClientInstance(this.defaultMQPushConsumer, this.rpcHook);

                    this.rebalanceImpl.setConsumerGroup(this.defaultMQPushConsumer.getConsumerGroup());
                    this.rebalanceImpl.setMessageModel(this.defaultMQPushConsumer.getMessageModel());
                    this.rebalanceImpl.setAllocateMessageQueueStrategy(this.defaultMQPushConsumer.getAllocateMessageQueueStrategy());
                    this.rebalanceImpl.setmQClientFactory(this.mQClientFactory);

                    this.pullAPIWrapper = new PullAPIWrapper(
                        mQClientFactory,
                        this.defaultMQPushConsumer.getConsumerGroup(), isUnitMode());
                    this.pullAPIWrapper.registerFilterMessageHook(filterMessageHookList);

                    if (this.defaultMQPushConsumer.getOffsetStore() != null)
                    {
                        this.offsetStore = this.defaultMQPushConsumer.getOffsetStore();
                    }
                    else
                    {
                        switch (this.defaultMQPushConsumer.getMessageModel())
                        {
                            case MessageModel.BROADCASTING:
                                this.offsetStore = new LocalFileOffsetStore(this.mQClientFactory, this.defaultMQPushConsumer.getConsumerGroup());
                                break;
                            case MessageModel.CLUSTERING:
                                this.offsetStore = new RemoteBrokerOffsetStore(this.mQClientFactory, this.defaultMQPushConsumer.getConsumerGroup());
                                break;
                            default:
                                break;
                        }
                        this.defaultMQPushConsumer.setOffsetStore(this.offsetStore);
                    }
                    this.offsetStore.load();

                    if (this.getMessageListenerInner() is MessageListenerOrderly) {
                        this.consumeOrderly = true;
                        this.consumeMessageService =
                            new ConsumeMessageOrderlyService(this, (MessageListenerOrderly)this.getMessageListenerInner());
                    } else if (this.getMessageListenerInner() is MessageListenerConcurrently) {
                        this.consumeOrderly = false;
                        this.consumeMessageService =
                            new ConsumeMessageConcurrentlyService(this, (MessageListenerConcurrently)this.getMessageListenerInner());
                    }

                    this.consumeMessageService.start();

                    bool registerOK = mQClientFactory.registerConsumer(this.defaultMQPushConsumer.getConsumerGroup(), this);
                    if (!registerOK)
                    {
                        this.serviceState = ServiceState.CREATE_JUST;
                        this.consumeMessageService.shutdown(defaultMQPushConsumer.getAwaitTerminationMillisWhenShutdown());
                        throw new MQClientException("The consumer group[" + this.defaultMQPushConsumer.getConsumerGroup()
                            + "] has been created before, specify another name please." + FAQUrl.suggestTodo(FAQUrl.GROUP_NAME_DUPLICATE_URL),
                            null);
                    }

                    mQClientFactory.start();
                    log.Info("the consumer [{}] start OK.", this.defaultMQPushConsumer.getConsumerGroup());
                    this.serviceState = ServiceState.RUNNING;
                    break;
                case ServiceState.RUNNING:
                case ServiceState.START_FAILED:
                case ServiceState.SHUTDOWN_ALREADY:
                    throw new MQClientException("The PushConsumer service state not OK, maybe started once, "
                        + this.serviceState
                        + FAQUrl.suggestTodo(FAQUrl.CLIENT_SERVICE_NOT_OK),
                        null);
                default:
                    break;
            }

            this.updateTopicSubscribeInfoWhenSubscriptionChanged();
            this.mQClientFactory.checkClientInBroker();
            this.mQClientFactory.sendHeartbeatToAllBrokerWithLock();
            this.mQClientFactory.rebalanceImmediately();
        }

        ///<exception cref="MQClientException"/>
        private void checkConfig()
        {
            Validators.checkGroup(this.defaultMQPushConsumer.getConsumerGroup());

            if (null == this.defaultMQPushConsumer.getConsumerGroup())
            {
                throw new MQClientException(
                    "consumerGroup is null"
                        + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                    null);
            }

            if (this.defaultMQPushConsumer.getConsumerGroup().Equals(MixAll.DEFAULT_CONSUMER_GROUP))
            {
                throw new MQClientException(
                    "consumerGroup can not equal "
                        + MixAll.DEFAULT_CONSUMER_GROUP
                        + ", please specify another one."
                        + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                    null);
            }

            if (null == this.defaultMQPushConsumer.getMessageModel())
            {
                throw new MQClientException(
                    "messageModel is null"
                        + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                    null);
            }

            if (null == this.defaultMQPushConsumer.getConsumeFromWhere())
            {
                throw new MQClientException(
                    "consumeFromWhere is null"
                        + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                    null);
            }

            DateTime dt = UtilAll.parseDate(this.defaultMQPushConsumer.getConsumeTimestamp(), UtilAll.YYYYMMDDHHMMSS);
            if (null == dt)
            {
                throw new MQClientException(
                    "consumeTimestamp is invalid, the valid format is yyyyMMddHHmmss,but received "
                        + this.defaultMQPushConsumer.getConsumeTimestamp()
                        + " " + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL), null);
            }

            // allocateMessageQueueStrategy
            if (null == this.defaultMQPushConsumer.getAllocateMessageQueueStrategy())
            {
                throw new MQClientException(
                    "allocateMessageQueueStrategy is null"
                        + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                    null);
            }

            // subscription
            if (null == this.defaultMQPushConsumer.getSubscription())
            {
                throw new MQClientException(
                    "subscription is null"
                        + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                    null);
            }

            // messageListener
            if (null == this.defaultMQPushConsumer.getMessageListener())
            {
                throw new MQClientException(
                    "messageListener is null"
                        + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                    null);
            }

            bool orderly = this.defaultMQPushConsumer.getMessageListener() is MessageListenerOrderly;
            bool concurrently = this.defaultMQPushConsumer.getMessageListener() is MessageListenerConcurrently;
            if (!orderly && !concurrently)
            {
                throw new MQClientException(
                    "messageListener must be instanceof MessageListenerOrderly or MessageListenerConcurrently"
                        + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                    null);
            }

            // consumeThreadMin
            if (this.defaultMQPushConsumer.getConsumeThreadMin() < 1
                || this.defaultMQPushConsumer.getConsumeThreadMin() > 1000)
            {
                throw new MQClientException(
                    "consumeThreadMin Out of range [1, 1000]"
                        + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                    null);
            }

            // consumeThreadMax
            if (this.defaultMQPushConsumer.getConsumeThreadMax() < 1 || this.defaultMQPushConsumer.getConsumeThreadMax() > 1000)
            {
                throw new MQClientException(
                    "consumeThreadMax Out of range [1, 1000]"
                        + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                    null);
            }

            // consumeThreadMin can't be larger than consumeThreadMax
            if (this.defaultMQPushConsumer.getConsumeThreadMin() > this.defaultMQPushConsumer.getConsumeThreadMax())
            {
                throw new MQClientException(
                    "consumeThreadMin (" + this.defaultMQPushConsumer.getConsumeThreadMin() + ") "
                        + "is larger than consumeThreadMax (" + this.defaultMQPushConsumer.getConsumeThreadMax() + ")",
                    null);
            }

            // consumeConcurrentlyMaxSpan
            if (this.defaultMQPushConsumer.getConsumeConcurrentlyMaxSpan() < 1
                || this.defaultMQPushConsumer.getConsumeConcurrentlyMaxSpan() > 65535)
            {
                throw new MQClientException(
                    "consumeConcurrentlyMaxSpan Out of range [1, 65535]"
                        + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                    null);
            }

            // pullThresholdForQueue
            if (this.defaultMQPushConsumer.getPullThresholdForQueue() < 1 || this.defaultMQPushConsumer.getPullThresholdForQueue() > 65535)
            {
                throw new MQClientException(
                    "pullThresholdForQueue Out of range [1, 65535]"
                        + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                    null);
            }

            // pullThresholdForTopic
            if (this.defaultMQPushConsumer.getPullThresholdForTopic() != -1)
            {
                if (this.defaultMQPushConsumer.getPullThresholdForTopic() < 1 || this.defaultMQPushConsumer.getPullThresholdForTopic() > 6553500)
                {
                    throw new MQClientException(
                        "pullThresholdForTopic Out of range [1, 6553500]"
                            + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                        null);
                }
            }

            // pullThresholdSizeForQueue
            if (this.defaultMQPushConsumer.getPullThresholdSizeForQueue() < 1 || this.defaultMQPushConsumer.getPullThresholdSizeForQueue() > 1024)
            {
                throw new MQClientException(
                    "pullThresholdSizeForQueue Out of range [1, 1024]"
                        + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                    null);
            }

            if (this.defaultMQPushConsumer.getPullThresholdSizeForTopic() != -1)
            {
                // pullThresholdSizeForTopic
                if (this.defaultMQPushConsumer.getPullThresholdSizeForTopic() < 1 || this.defaultMQPushConsumer.getPullThresholdSizeForTopic() > 102400)
                {
                    throw new MQClientException(
                        "pullThresholdSizeForTopic Out of range [1, 102400]"
                            + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                        null);
                }
            }

            // pullInterval
            if (this.defaultMQPushConsumer.getPullInterval() < 0 || this.defaultMQPushConsumer.getPullInterval() > 65535)
            {
                throw new MQClientException(
                    "pullInterval Out of range [0, 65535]"
                        + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                    null);
            }

            // consumeMessageBatchMaxSize
            if (this.defaultMQPushConsumer.getConsumeMessageBatchMaxSize() < 1
                || this.defaultMQPushConsumer.getConsumeMessageBatchMaxSize() > 1024)
            {
                throw new MQClientException(
                    "consumeMessageBatchMaxSize Out of range [1, 1024]"
                        + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                    null);
            }

            // pullBatchSize
            if (this.defaultMQPushConsumer.getPullBatchSize() < 1 || this.defaultMQPushConsumer.getPullBatchSize() > 1024)
            {
                throw new MQClientException(
                    "pullBatchSize Out of range [1, 1024]"
                        + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                    null);
            }
        }

        ///<exception cref="MQClientException"/>
        private void copySubscription()
        {
            try
            {
                Dictionary<String, String> sub = this.defaultMQPushConsumer.getSubscription();
                if (sub != null)
                {
                    foreach (var entry in sub)
                    {
                        String topic = entry.Key;
                        String subString = entry.Value;
                        SubscriptionData subscriptionData = FilterAPI.buildSubscriptionData(topic, subString);
                        //this.rebalanceImpl.getSubscriptionInner().put(topic, subscriptionData);
                        this.rebalanceImpl.getSubscriptionInner()[topic] = subscriptionData;
                    }
                }

                if (null == this.messageListenerInner)
                {
                    this.messageListenerInner = this.defaultMQPushConsumer.getMessageListener();
                }

                switch (this.defaultMQPushConsumer.getMessageModel())
                {
                    case MessageModel.BROADCASTING:
                        break;
                    case MessageModel.CLUSTERING:
                        String retryTopic = MixAll.getRetryTopic(this.defaultMQPushConsumer.getConsumerGroup());
                        SubscriptionData subscriptionData = FilterAPI.buildSubscriptionData(retryTopic, SubscriptionData.SUB_ALL);
                        //this.rebalanceImpl.getSubscriptionInner().put(retryTopic, subscriptionData);
                        this.rebalanceImpl.getSubscriptionInner()[retryTopic] = subscriptionData;
                        break;
                    default:
                        break;
                }
            }
            catch (Exception e)
            {
                throw new MQClientException("subscription exception", e);
            }
        }

        public MessageListener getMessageListenerInner()
        {
            return messageListenerInner;
        }

        private void updateTopicSubscribeInfoWhenSubscriptionChanged()
        {
            ConcurrentDictionary<String, SubscriptionData> subTable = this.getSubscriptionInner();
            if (subTable != null)
            {
                foreach (var entry in subTable)
                {
                    String topic = entry.Key;
                    this.mQClientFactory.updateTopicRouteInfoFromNameServer(topic);
                }
            }
        }

        public ConcurrentDictionary<String, SubscriptionData> getSubscriptionInner()
        {
            return this.rebalanceImpl.getSubscriptionInner();
        }

        ///<exception cref="MQClientException"/>
        public void subscribe(String topic, String subExpression)
        {
            try
            {
                SubscriptionData subscriptionData = FilterAPI.buildSubscriptionData(topic, subExpression);
                this.rebalanceImpl.getSubscriptionInner().put(topic, subscriptionData);
                if (this.mQClientFactory != null)
                {
                    this.mQClientFactory.sendHeartbeatToAllBrokerWithLock();
                }
            }
            catch (Exception e)
            {
                throw new MQClientException("subscription exception", e);
            }
        }
        ///<exception cref="MQClientException"/>
        public void subscribe(String topic, String fullClassName, String filterClassSource)
        {
            try
            {
                SubscriptionData subscriptionData = FilterAPI.buildSubscriptionData(topic, "*");
                subscriptionData.subString = fullClassName;
                subscriptionData.classFilterMode = true;
                subscriptionData.filterClassSource = filterClassSource;
                this.rebalanceImpl.getSubscriptionInner().put(topic, subscriptionData);
                if (this.mQClientFactory != null)
                {
                    this.mQClientFactory.sendHeartbeatToAllBrokerWithLock();
                }
            }
            catch (Exception e)
            {
                throw new MQClientException("subscription exception", e);
            }
        }

        ///<exception cref="MQClientException"/>
        public void subscribe(String topic, MessageSelector messageSelector)
        {
            try
            {
                if (messageSelector == null)
                {
                    subscribe(topic, SubscriptionData.SUB_ALL);
                    return;
                }

                SubscriptionData subscriptionData = FilterAPI.build(topic,
                    messageSelector.getExpression(), messageSelector.getExpressionType());

                this.rebalanceImpl.getSubscriptionInner().put(topic, subscriptionData);
                if (this.mQClientFactory != null)
                {
                    this.mQClientFactory.sendHeartbeatToAllBrokerWithLock();
                }
            }
            catch (Exception e)
            {
                throw new MQClientException("subscription exception", e);
            }
        }

        public void suspend()
        {
            this.pause = true;
            log.Info("suspend this consumer, {}", this.defaultMQPushConsumer.getConsumerGroup());
        }

        public void unsubscribe(String topic)
        {
            //this.rebalanceImpl.getSubscriptionInner().remove(topic);
            this.rebalanceImpl.getSubscriptionInner().TryRemove(topic, out _);
        }

        public void updateConsumeOffset(MessageQueue mq, long offset)
        {
            this.offsetStore.updateOffset(mq, offset, false);
        }

        public void updateCorePoolSize(int corePoolSize)
        {
            this.consumeMessageService.updateCorePoolSize(corePoolSize);
        }

        ///<exception cref="RemotingException"/>
        ///<exception cref="MQBrokerException"/>
        ///<exception cref="InterruptedException"/>
        ///<exception cref="MQClientException"/>
        public MessageExt viewMessage(String msgId)
        {
            return this.mQClientFactory.getMQAdminImpl().viewMessage(msgId);
        }

        public RebalanceImpl getRebalanceImpl()
        {
            return rebalanceImpl;
        }

        public bool isConsumeOrderly()
        {
            return consumeOrderly;
        }

        public void setConsumeOrderly(bool consumeOrderly)
        {
            this.consumeOrderly = consumeOrderly;
        }

        ///<exception cref="RemotingException"/>
        ///<exception cref="MQBrokerException"/>
        ///<exception cref="InterruptedException"/>
        ///<exception cref="MQClientException"/>
        public void resetOffsetByTimeStamp(long timeStamp)
        {
            foreach (var entry in rebalanceImpl.getSubscriptionInner())
            {
                string topic = entry.Key;
                //HashSet<MessageQueue> mqs = rebalanceImpl.getTopicSubscribeInfoTable().get(entry.Key);
                rebalanceImpl.getTopicSubscribeInfoTable().TryGetValue(topic, out HashSet<MessageQueue> mqs);
                Dictionary<MessageQueue, long> offsetTable = new Dictionary<MessageQueue, long>();
                if (mqs != null)
                {
                    foreach (MessageQueue mq in mqs)
                    {
                        long offset = searchOffset(mq, timeStamp);
                        //offsetTable.put(mq, offset);
                        offsetTable[mq] = offset;
                    }
                    this.mQClientFactory.resetOffset(topic, groupName(), offsetTable);
                }
            }
        }

        ///<exception cref="MQClientException"/>
        public long searchOffset(MessageQueue mq, long timestamp)
        {
            return this.mQClientFactory.getMQAdminImpl().searchOffset(mq, timestamp);
        }

        //@Override
        public String groupName()
        {
            return this.defaultMQPushConsumer.getConsumerGroup();
        }

        //@Override
        public MessageModel messageModel()
        {
            return this.defaultMQPushConsumer.getMessageModel();
        }

        //@Override
        public ConsumeType consumeType()
        {
            return ConsumeType.CONSUME_PASSIVELY;
        }

        //@Override
        public ConsumeFromWhere consumeFromWhere()
        {
            return this.defaultMQPushConsumer.getConsumeFromWhere();
        }

        //@Override
        public HashSet<SubscriptionData> subscriptions()
        {
            HashSet<SubscriptionData> subSet = new HashSet<SubscriptionData>();

            //subSet.addAll(this.rebalanceImpl.getSubscriptionInner().values());
            subSet.addAll(this.rebalanceImpl.getSubscriptionInner().Values);

            return subSet;
        }

        //@Override
        public void doRebalance()
        {
            if (!this.pause)
            {
                this.rebalanceImpl.doRebalance(this.isConsumeOrderly());
            }
        }

        //@Override
        public void persistConsumerOffset()
        {
            try
            {
                this.makeSureStateOK();
                //HashSet<MessageQueue> mqs = new HashSet<MessageQueue>();
                //HashSet<MessageQueue> allocateMq = this.rebalanceImpl.getProcessQueueTable().ketSet();
                //mqs.addAll(allocateMq);
                HashSet<MessageQueue> mqs = this.rebalanceImpl.getProcessQueueTable().Keys.ToHashSet();
                this.offsetStore.persistAll(mqs);
            }
            catch (Exception e)
            {
                log.Error("group: " + this.defaultMQPushConsumer.getConsumerGroup() + " persistConsumerOffset exception", e.ToString());
            }
        }

        //@Override
        public void updateTopicSubscribeInfo(String topic, HashSet<MessageQueue> info)
        {
            ConcurrentDictionary<String, SubscriptionData> subTable = this.getSubscriptionInner();
            if (subTable != null)
            {
                if (subTable.ContainsKey(topic))
                {
                    this.rebalanceImpl.topicSubscribeInfoTable.put(topic, info);
                }
            }
        }

        //@Override
        public bool isSubscribeTopicNeedUpdate(String topic)
        {
            ConcurrentDictionary<String, SubscriptionData> subTable = this.getSubscriptionInner();
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
            return this.defaultMQPushConsumer.isUnitMode();
        }

        //@Override
        public ConsumerRunningInfo consumerRunningInfo()
        {
            ConsumerRunningInfo info = new ConsumerRunningInfo();

            Properties prop = MixAll.object2Properties(this.defaultMQPushConsumer);

            prop.put(ConsumerRunningInfo.PROP_CONSUME_ORDERLY, /*String.valueOf*/(this.consumeOrderly).ToString());
            prop.put(ConsumerRunningInfo.PROP_THREADPOOL_CORE_SIZE, /*String.valueOf*/(this.consumeMessageService.getCorePoolSize()).ToString());
            prop.put(ConsumerRunningInfo.PROP_CONSUMER_START_TIMESTAMP,/*String.valueOf*/(this.consumerStartTimestamp).ToString());

            info.properties = prop;

            HashSet<SubscriptionData> subSet = this.subscriptions();
            info.subscriptionSet.addAll(subSet);

            //Iterator<Entry<MessageQueue, ProcessQueue>> it = this.rebalanceImpl.getProcessQueueTable().entrySet().iterator();
            //while (it.hasNext())
            foreach (var entry in this.rebalanceImpl.getProcessQueueTable())
            {
                //Entry<MessageQueue, ProcessQueue> next = it.next();
                MessageQueue mq = entry.Key;
                ProcessQueue pq = entry.Value;

                ProcessQueueInfo pqinfo = new ProcessQueueInfo();
                pqinfo.setCommitOffset(this.offsetStore.readOffset(mq, ReadOffsetType.MEMORY_FIRST_THEN_STORE));
                pq.fillProcessQueueInfo(pqinfo);
                info.mqTable.put(mq, pqinfo);
            }

            foreach (SubscriptionData sd in subSet)
            {
                ConsumeStatus consumeStatus = this.mQClientFactory.getConsumerStatsManager().consumeStatus(this.groupName(), sd.topic);
                info.statusTable.put(sd.topic, consumeStatus);
            }

            return info;
        }

        public MQClientInstance getmQClientFactory()
        {
            return mQClientFactory;
        }

        public void setmQClientFactory(MQClientInstance mQClientFactory)
        {
            this.mQClientFactory = mQClientFactory;
        }

        public ServiceState getServiceState()
        {
            return serviceState;
        }

        //Don't use this deprecated setter, which will be removed soon.
        [Obsolete]//@Deprecated
        [MethodImpl(MethodImplOptions.Synchronized)]
        public void setServiceState(ServiceState serviceState)
        {
            this.serviceState = serviceState;
        }

        public void adjustThreadPool()
        {
            long computeAccTotal = this.computeAccumulationTotal();
            long adjustThreadPoolNumsThreshold = this.defaultMQPushConsumer.getAdjustThreadPoolNumsThreshold();

            long incThreshold = (long)(adjustThreadPoolNumsThreshold * 1.0);

            long decThreshold = (long)(adjustThreadPoolNumsThreshold * 0.8);

            if (computeAccTotal >= incThreshold)
            {
                this.consumeMessageService.incCorePoolSize();
            }

            if (computeAccTotal < decThreshold)
            {
                this.consumeMessageService.decCorePoolSize();
            }
        }

        private long computeAccumulationTotal()
        {
            long msgAccTotal = 0;
            ConcurrentDictionary<MessageQueue, ProcessQueue> processQueueTable = this.rebalanceImpl.getProcessQueueTable();
            //Iterator<Entry<MessageQueue, ProcessQueue>> it = processQueueTable.entrySet().iterator();
            //while (it.hasNext())
            foreach (var entry in processQueueTable)
            {
                //Entry<MessageQueue, ProcessQueue> next = it.next();
                ProcessQueue value = entry.Value;
                msgAccTotal += value.MsgAccCnt;
            }

            return msgAccTotal;
        }

        ///<exception cref="RemotingException"/>
        ///<exception cref="MQClientException"/>
        ///<exception cref="InterruptedException"/>
        ///<exception cref="MQBrokerException"/>
        public List<QueueTimeSpan> queryConsumeTimeSpan(String topic)
        {
            List<QueueTimeSpan> queueTimeSpan = new List<QueueTimeSpan>();
            TopicRouteData routeData = this.mQClientFactory.getMQClientAPIImpl().getTopicRouteInfoFromNameServer(topic, 3000);
            foreach (BrokerData brokerData in routeData.brokerDatas)
            {
                String addr = brokerData.selectBrokerAddr();
                queueTimeSpan.addAll(this.mQClientFactory.getMQClientAPIImpl().queryConsumeTimeSpan(addr, topic, groupName(), 3000));
            }

            return queueTimeSpan;
        }

        public void resetRetryAndNamespace(List<MessageExt> msgs, String consumerGroup)
        {
            String groupTopic = MixAll.getRetryTopic(consumerGroup);
            foreach (MessageExt msg in msgs)
            {
                String retryTopic = msg.getProperty(MessageConst.PROPERTY_RETRY_TOPIC);
                if (retryTopic != null && groupTopic.Equals(msg.getTopic()))
                {
                    msg.setTopic(retryTopic);
                }

                if (UtilAll.isNotEmpty(this.defaultMQPushConsumer.getNamespace()))
                {
                    msg.setTopic(NamespaceUtil.withoutNamespace(msg.getTopic(), this.defaultMQPushConsumer.getNamespace()));
                }
            }
        }

        public ConsumeMessageService getConsumeMessageService()
        {
            return consumeMessageService;
        }

        public void setConsumeMessageService(ConsumeMessageService consumeMessageService)
        {
            this.consumeMessageService = consumeMessageService;

        }

        public void setPullTimeDelayMillsWhenException(long pullTimeDelayMillsWhenException)
        {
            this.pullTimeDelayMillsWhenException = pullTimeDelayMillsWhenException;
        }
    }
}
