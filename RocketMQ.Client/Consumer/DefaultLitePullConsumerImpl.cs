using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;

namespace RocketMQ.Client
{
    public class DefaultLitePullConsumerImpl : MQConsumerInner
    {
        //private readonly InternalLogger log = ClientLogger.getLog();
        static NLog.Logger log = NLog.LogManager.GetCurrentClassLogger();

        //private readonly long consumerStartTimestamp = Sys.currentTimeMillis();
        private readonly long consumerStartTimestamp = TimeUtils.CurrentTimeMillis(true);

        private readonly RPCHook rpcHook;

        private readonly List<FilterMessageHook> filterMessageHookList = new List<FilterMessageHook>();

        private volatile ServiceState serviceState = ServiceState.CREATE_JUST;

        protected MQClientInstance mQClientFactory;

        private PullAPIWrapper pullAPIWrapper;

        private OffsetStore offsetStore;

        private RebalanceImpl rebalanceImpl = null;// new RebalanceLitePullImpl(this);

        private enum SubscriptionType
        {
            NONE, SUBSCRIBE, ASSIGN
        }

        private static readonly String NOT_RUNNING_EXCEPTION_MESSAGE = "The consumer not running, please start it first.";

        private static readonly String SUBSCRIPTION_CONFLICT_EXCEPTION_MESSAGE = "Subscribe and assign are mutually exclusive.";
        /**
         * the type of subscription
         */
        private SubscriptionType subscriptionType = SubscriptionType.NONE;
        /**
         * Delay some time when exception occur
         */
        private long pullTimeDelayMillsWhenException = 1000;
        /**
         * Flow control interval
         */
        private static readonly long PULL_TIME_DELAY_MILLS_WHEN_FLOW_CONTROL = 50;
        /**
         * Delay some time when suspend pull service
         */
        private static readonly long PULL_TIME_DELAY_MILLS_WHEN_PAUSE = 1000;

        private static readonly long PULL_TIME_DELAY_MILLS_ON_EXCEPTION = 3 * 1000;

        private DefaultLitePullConsumer defaultLitePullConsumer;

        private readonly ConcurrentDictionary<MessageQueue, PullTaskImpl> taskTable =
            new ConcurrentDictionary<MessageQueue, PullTaskImpl>();

        private AssignedMessageQueue assignedMessageQueue = new AssignedMessageQueue();

        //private readonly BlockingQueue<ConsumeRequest> consumeRequestCache = new LinkedBlockingQueue<ConsumeRequest>();
        private readonly LinkedBlockingQueue<ConsumeRequest> consumeRequestCache = new LinkedBlockingQueue<ConsumeRequest>();

        //private ScheduledThreadPoolExecutor scheduledThreadPoolExecutor;
        private ScheduledExecutorService scheduledThreadPoolExecutor;

        private readonly ScheduledExecutorService scheduledExecutorService;

        private Dictionary<String, TopicMessageQueueChangeListener> topicMessageQueueChangeListenerMap = new Dictionary<String, TopicMessageQueueChangeListener>();

        private Dictionary<String, HashSet<MessageQueue>> messageQueuesForTopic = new Dictionary<String, HashSet<MessageQueue>>();

        private long consumeRequestFlowControlTimes = 0L;

        private long queueFlowControlTimes = 0L;

        private long queueMaxSpanFlowControlTimes = 0L;

        private long nextAutoCommitDeadline = -1L;

        private readonly MessageQueueLock messageQueueLock = new MessageQueueLock();

        private readonly List<ConsumeMessageHook> consumeMessageHookList = new List<ConsumeMessageHook>();

        public DefaultLitePullConsumerImpl(DefaultLitePullConsumer defaultLitePullConsumer, RPCHook rpcHook)
        {
            rebalanceImpl = new RebalanceLitePullImpl(this);
            this.defaultLitePullConsumer = defaultLitePullConsumer;
            this.rpcHook = rpcHook;
            //    this.scheduledThreadPoolExecutor = new ScheduledThreadPoolExecutor(
            //        this.defaultLitePullConsumer.getPullThreadNums(),
            //        new ThreadFactoryImpl("PullMsgThread-" + this.defaultLitePullConsumer.getConsumerGroup())
            //    );
            //    this.scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
            //    @Override
            //    public Thread newThread(Runnable r)
            //    {
            //        return new Thread(r, "MonitorMessageQueueChangeThread");
            //    }
            //});
            scheduledThreadPoolExecutor = new ScheduledExecutorService(defaultLitePullConsumer.getPullThreadNums());
            scheduledExecutorService = new ScheduledExecutorService();
            this.pullTimeDelayMillsWhenException = defaultLitePullConsumer.getPullTimeDelayMillsWhenException();
        }

        public void registerConsumeMessageHook(ConsumeMessageHook hook)
        {
            this.consumeMessageHookList.Add(hook);
            log.Info("register consumeMessageHook Hook, {}", hook.hookName());
        }

        public void executeHookBefore(ConsumeMessageContext context)
        {
            if (this.consumeMessageHookList.Count > 0)
            {
                foreach (ConsumeMessageHook hook in this.consumeMessageHookList)
                {
                    try
                    {
                        hook.consumeMessageBefore(context);
                    }
                    catch (Exception e)
                    {
                        log.Error("consumeMessageHook {} executeHookBefore exception", hook.hookName(), e);
                    }
                }
            }
        }

        public void executeHookAfter(ConsumeMessageContext context)
        {
            if (this.consumeMessageHookList.Count > 0)
            {
                foreach (ConsumeMessageHook hook in this.consumeMessageHookList)
                {
                    try
                    {
                        hook.consumeMessageAfter(context);
                    }
                    catch (Exception e)
                    {
                        log.Error("consumeMessageHook {} executeHookAfter exception", hook.hookName(), e);
                    }
                }
            }
        }

        private void checkServiceState()
        {
            if (this.serviceState != ServiceState.RUNNING)
            {
                throw new InvalidOperationException(NOT_RUNNING_EXCEPTION_MESSAGE);
            }
        }

        public void updateNameServerAddr(String newAddresses)
        {
            this.mQClientFactory.getMQClientAPIImpl().updateNameServerAddressList(newAddresses);
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        private void setSubscriptionType(SubscriptionType type)
        {
            if (this.subscriptionType == SubscriptionType.NONE)
            {
                this.subscriptionType = type;
            }
            else if (this.subscriptionType != type)
            {
                throw new InvalidOperationException(SUBSCRIPTION_CONFLICT_EXCEPTION_MESSAGE);
            }
        }

        private void updateAssignedMessageQueue(String topic, HashSet<MessageQueue> assignedMessageQueue)
        {
            this.assignedMessageQueue.updateAssignedMessageQueue(topic, assignedMessageQueue);
        }

        private void updatePullTask(string topic, HashSet<MessageQueue> mqNewSet)
        {
            //Iterator<Map.Entry<MessageQueue, PullTaskImpl>> it = this.taskTable.entrySet().iterator();
            //while (it.hasNext())
            //{
            //    Map.Entry<MessageQueue, PullTaskImpl> next = it.next();
            //    if (next.getKey().getTopic().Equals(topic))
            //    {
            //        if (!mqNewSet.contains(next.getKey()))
            //        {
            //            next.getValue().setCancelled(true);
            //            it.remove();
            //        }
            //    }
            //}
            //startPullTask(mqNewSet);
            var keys = this.taskTable.Keys.ToList();
            foreach (var key in keys)
            {
                if (key.getTopic().Equals(topic))
                {
                    if (!mqNewSet.Contains(key))
                    {
                        taskTable[key].setCancelled(true);
                        taskTable.Remove(key, out _);
                    }
                }
            }
            startPullTask(mqNewSet);

        }

        class MessageQueueListenerImpl : MessageQueueListener
        {

            private DefaultLitePullConsumerImpl owner;

            public MessageQueueListenerImpl(DefaultLitePullConsumerImpl owner)
            {
                this.owner = owner;  
            }

            public void messageQueueChanged(String topic, HashSet<MessageQueue> mqAll, HashSet<MessageQueue> mqDivided)
            {
                MessageModel messageModel = owner.defaultLitePullConsumer.getMessageModel();
                switch (messageModel)
                {
                    case MessageModel.BROADCASTING:
                        owner.updateAssignedMessageQueue(topic, mqAll);
                        owner.updatePullTask(topic, mqAll);
                        break;
                    case MessageModel.CLUSTERING:
                        owner.updateAssignedMessageQueue(topic, mqDivided);
                        owner.updatePullTask(topic, mqDivided);
                        break;
                    default:
                        break;
                }
            }
        }

        /// <summary>
        /// synchronized
        /// </summary>
        [MethodImpl(MethodImplOptions.Synchronized)]
        public void shutdown()
        {
            switch (this.serviceState)
            {
                case ServiceState.CREATE_JUST:
                    break;
                case ServiceState.RUNNING:
                    persistConsumerOffset();
                    this.mQClientFactory.unregisterConsumer(this.defaultLitePullConsumer.getConsumerGroup());
                    scheduledThreadPoolExecutor.Shutdown();
                    scheduledExecutorService.Shutdown();
                    this.mQClientFactory.shutdown();
                    this.serviceState = ServiceState.SHUTDOWN_ALREADY;
                    log.Info("the consumer [{}] shutdown OK", this.defaultLitePullConsumer.getConsumerGroup());
                    break;
                default:
                    break;
            }
        }

        /// <summary>
        /// synchronized
        /// </summary>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.Synchronized)]
        public bool isRunning()
        {
            return this.serviceState == ServiceState.RUNNING;
        }

        /// <summary>
        /// synchronized
        /// </summary>
        [MethodImpl(MethodImplOptions.Synchronized)]
        public void start()
        {
            switch (this.serviceState)
            {
                case ServiceState.CREATE_JUST:
                    this.serviceState = ServiceState.START_FAILED;

                    this.checkConfig();

                    if (this.defaultLitePullConsumer.getMessageModel() == MessageModel.CLUSTERING)
                    {
                        this.defaultLitePullConsumer.changeInstanceNameToPID();
                    }

                    initMQClientFactory();

                    initRebalanceImpl();

                    initPullAPIWrapper();

                    initOffsetStore();

                    mQClientFactory.start();

                    startScheduleTask();

                    this.serviceState = ServiceState.RUNNING;

                    log.Info("the consumer [{}] start OK", this.defaultLitePullConsumer.getConsumerGroup());

                    operateAfterRunning();

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

        private void initMQClientFactory()
        {
            this.mQClientFactory = MQClientManager.getInstance().getOrCreateMQClientInstance(this.defaultLitePullConsumer, this.rpcHook);
            bool registerOK = mQClientFactory.registerConsumer(this.defaultLitePullConsumer.getConsumerGroup(), this);
            if (!registerOK)
            {
                this.serviceState = ServiceState.CREATE_JUST;

                throw new MQClientException("The consumer group[" + this.defaultLitePullConsumer.getConsumerGroup()
                    + "] has been created before, specify another name please." + FAQUrl.suggestTodo(FAQUrl.GROUP_NAME_DUPLICATE_URL),
                    null);
            }
        }

        private void initRebalanceImpl()
        {
            this.rebalanceImpl.setConsumerGroup(this.defaultLitePullConsumer.getConsumerGroup());
            this.rebalanceImpl.setMessageModel(this.defaultLitePullConsumer.getMessageModel());
            this.rebalanceImpl.setAllocateMessageQueueStrategy(this.defaultLitePullConsumer.getAllocateMessageQueueStrategy());
            this.rebalanceImpl.setmQClientFactory(this.mQClientFactory);
        }

        private void initPullAPIWrapper()
        {
            this.pullAPIWrapper = new PullAPIWrapper(
                mQClientFactory,
                this.defaultLitePullConsumer.getConsumerGroup(), isUnitMode());
            this.pullAPIWrapper.registerFilterMessageHook(filterMessageHookList);
        }

        private void initOffsetStore()
        {
            if (this.defaultLitePullConsumer.getOffsetStore() != null)
            {
                this.offsetStore = this.defaultLitePullConsumer.getOffsetStore();
            }
            else
            {
                switch (this.defaultLitePullConsumer.getMessageModel())
                {
                    case MessageModel.BROADCASTING:
                        this.offsetStore = new LocalFileOffsetStore(this.mQClientFactory, this.defaultLitePullConsumer.getConsumerGroup());
                        break;
                    case MessageModel.CLUSTERING:
                        this.offsetStore = new RemoteBrokerOffsetStore(this.mQClientFactory, this.defaultLitePullConsumer.getConsumerGroup());
                        break;
                    default:
                        break;
                }
                this.defaultLitePullConsumer.setOffsetStore(this.offsetStore);
            }
            this.offsetStore.load();
        }

        private void startScheduleTask()
        {
            //ok
            //    scheduledExecutorService.scheduleAtFixedRate(
            //        new Runnable() {
            //                @Override
            //                public void run()
            //    {
            //        try
            //        {
            //            fetchTopicMessageQueuesAndCompare();
            //        }
            //        catch (Exception e)
            //        {
            //            log.Error("ScheduledTask fetchMessageQueuesAndCompare exception", e);
            //        }
            //    }
            //}, 1000 * 10, this.getDefaultLitePullConsumer().getTopicMetadataCheckIntervalMillis(), TimeUnit.MILLISECONDS);
            scheduledExecutorService.ScheduleAtFixedRate(() =>
            {
                try
                {
                    fetchTopicMessageQueuesAndCompare();
                }
                catch (Exception e)
                {
                    log.Error("ScheduledTask fetchMessageQueuesAndCompare exception", e.ToString());
                }
            }, 1000 * 10, this.getDefaultLitePullConsumer().getTopicMetadataCheckIntervalMillis());
        }

        private void operateAfterRunning()
        {
            // If subscribe function invoke before start function, then update topic subscribe info after initialization.
            if (subscriptionType == SubscriptionType.SUBSCRIBE)
            {
                updateTopicSubscribeInfoWhenSubscriptionChanged();
            }
            // If assign function invoke before start function, then update pull task after initialization.
            if (subscriptionType == SubscriptionType.ASSIGN)
            {
                updateAssignPullTask(assignedMessageQueue.messageQueues());
            }

            foreach (string topic in topicMessageQueueChangeListenerMap.Keys)
            {
                HashSet<MessageQueue> messageQueues = fetchMessageQueues(topic);
                messageQueuesForTopic.put(topic, messageQueues);
            }
            this.mQClientFactory.checkClientInBroker();
        }

        private void checkConfig()
        {
            // Check consumerGroup
            Validators.checkGroup(this.defaultLitePullConsumer.getConsumerGroup());

            // Check consumerGroup name is not equal default consumer group name.
            if (this.defaultLitePullConsumer.getConsumerGroup().Equals(MixAll.DEFAULT_CONSUMER_GROUP))
            {
                throw new MQClientException(
                    "consumerGroup can not equal "
                        + MixAll.DEFAULT_CONSUMER_GROUP
                        + ", please specify another one."
                        + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                    null);
            }

            // Check messageModel is not null.
            if (null == this.defaultLitePullConsumer.getMessageModel())
            {
                throw new MQClientException(
                    "messageModel is null"
                        + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                    null);
            }

            // Check allocateMessageQueueStrategy is not null
            if (null == this.defaultLitePullConsumer.getAllocateMessageQueueStrategy())
            {
                throw new MQClientException(
                    "allocateMessageQueueStrategy is null"
                        + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                    null);
            }

            if (this.defaultLitePullConsumer.getConsumerTimeoutMillisWhenSuspend() < this.defaultLitePullConsumer.getBrokerSuspendMaxTimeMillis())
            {
                throw new MQClientException(
                    "Long polling mode, the consumer consumerTimeoutMillisWhenSuspend must greater than brokerSuspendMaxTimeMillis"
                        + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                    null);
            }
        }

        public PullAPIWrapper getPullAPIWrapper()
        {
            return pullAPIWrapper;
        }

        private void startPullTask(ICollection<MessageQueue> mqSet)
        {
            foreach (MessageQueue messageQueue in mqSet)
            {
                if (!this.taskTable.ContainsKey(messageQueue))
                {
                    PullTaskImpl pullTask = new PullTaskImpl(messageQueue);
                    this.taskTable.put(messageQueue, pullTask);
                    this.scheduledThreadPoolExecutor.Schedule(pullTask, 0);
                }
            }
        }

        private void updateAssignPullTask(ICollection<MessageQueue> mqNewSet)
        {
            //Iterator<Map.Entry<MessageQueue, PullTaskImpl>> it = this.taskTable.entrySet().iterator();
            //while (it.hasNext())
            foreach(var entry in taskTable)
            {
                //Map.Entry<MessageQueue, PullTaskImpl> next = it.next();
                if (!mqNewSet.Contains(entry.Key))
                {
                    entry.Value.setCancelled(true);
                    //it.remove();
                    taskTable.TryRemove(entry.Key, out _); //???iter remove
                }
            }

            startPullTask(mqNewSet);
        }

        private void updateTopicSubscribeInfoWhenSubscriptionChanged()
        {
            var subTable = rebalanceImpl.getSubscriptionInner();
            if (subTable != null)
            {
                foreach (var entry in subTable)
                {
                    string topic = entry.Key;
                    this.mQClientFactory.updateTopicRouteInfoFromNameServer(topic);
                }
            }
        }

        /// <summary>
        /// synchronized
        /// </summary>
        /// <param name="topic"></param>
        /// <param name="subExpression"></param>
        [MethodImpl(MethodImplOptions.Synchronized)]
        public void subscribe(String topic, String subExpression)
        {
            try
            {
                if (topic == null || "".Equals(topic))
                {
                    throw new ArgumentException("Topic can not be null or empty.");
                }
                setSubscriptionType(SubscriptionType.SUBSCRIBE);
                SubscriptionData subscriptionData = FilterAPI.buildSubscriptionData(topic, subExpression);
                this.rebalanceImpl.getSubscriptionInner().put(topic, subscriptionData);
                this.defaultLitePullConsumer.setMessageQueueListener(new MessageQueueListenerImpl(this));
                assignedMessageQueue.setRebalanceImpl(this.rebalanceImpl);
                if (serviceState == ServiceState.RUNNING)
                {
                    this.mQClientFactory.sendHeartbeatToAllBrokerWithLock();
                    updateTopicSubscribeInfoWhenSubscriptionChanged();
                }
            }
            catch (Exception e)
            {
                throw new MQClientException("subscribe exception", e);
            }
        }

        /// <summary>
        /// synchronized
        /// </summary>
        /// <param name="topic"></param>
        /// <param name="messageSelector"></param>
        [MethodImpl(MethodImplOptions.Synchronized)]
        public void subscribe(String topic, MessageSelector messageSelector)
        {
            try
            {
                if (topic == null || "".Equals(topic))
                {
                    throw new ArgumentException("Topic can not be null or empty.");
                }
                setSubscriptionType(SubscriptionType.SUBSCRIBE);
                if (messageSelector == null)
                {
                    subscribe(topic, SubscriptionData.SUB_ALL);
                    return;
                }
                SubscriptionData subscriptionData = FilterAPI.build(topic,
                    messageSelector.getExpression(), messageSelector.getExpressionType());
                this.rebalanceImpl.getSubscriptionInner().put(topic, subscriptionData);
                this.defaultLitePullConsumer.setMessageQueueListener(new MessageQueueListenerImpl(this));
                assignedMessageQueue.setRebalanceImpl(this.rebalanceImpl);
                if (serviceState == ServiceState.RUNNING)
                {
                    this.mQClientFactory.sendHeartbeatToAllBrokerWithLock();
                    updateTopicSubscribeInfoWhenSubscriptionChanged();
                }
            }
            catch (Exception e)
            {
                throw new MQClientException("subscribe exception", e);
            }
        }

        /// <summary>
        /// synchronized
        /// </summary>
        /// <param name="String"></param>
        /// <param name=""></param>
        [MethodImpl(MethodImplOptions.Synchronized)]
        public void unsubscribe(String topic)
        {
            this.rebalanceImpl.getSubscriptionInner().remove(topic);
            removePullTaskCallback(topic);
            assignedMessageQueue.removeAssignedMessageQueue(topic);
        }




        /// <summary>
        /// synchronized
        /// </summary>
        /// <param name="messageQueues"></param>
        [MethodImpl(MethodImplOptions.Synchronized)]
        public void assign(ICollection<MessageQueue> messageQueues)
        {
            if (messageQueues == null || messageQueues.isEmpty())
            {
                throw new ArgumentException("Message queues can not be null or empty.");
            }
            setSubscriptionType(SubscriptionType.ASSIGN);
            assignedMessageQueue.updateAssignedMessageQueue(messageQueues);
            if (serviceState == ServiceState.RUNNING)
            {
                updateAssignPullTask(messageQueues);
            }
        }

        private void maybeAutoCommit()
        {
            long now = TimeUtils.CurrentTimeMillisUTC();
            if (now >= nextAutoCommitDeadline)
            {
                commitAll();
                nextAutoCommitDeadline = now + defaultLitePullConsumer.getAutoCommitIntervalMillis();
            }
        }

        /// <summary>
        /// synchronized
        /// </summary>
        /// <param name="timeout"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.Synchronized)]
        public List<MessageExt> poll(long timeout)
        {
            try
            {

                checkServiceState();
                if (timeout < 0)
                {
                    throw new ArgumentException("Timeout must not be negative");
                }

                if (defaultLitePullConsumer.isAutoCommit())
                {
                    maybeAutoCommit();
                }
                long endTime = Sys.currentTimeMillis() + timeout;

                ConsumeRequest consumeRequest = consumeRequestCache.poll(endTime - Sys.currentTimeMillis(), TimeUnit.MILLISECONDS);
                if (endTime - Sys.currentTimeMillis() > 0)
                {
                    while (consumeRequest != null && consumeRequest.getProcessQueue().isDropped())
                    {
                        consumeRequest = consumeRequestCache.poll(endTime - Sys.currentTimeMillis(), TimeUnit.MILLISECONDS);
                        if (endTime - Sys.currentTimeMillis() <= 0)
                        {
                            break;
                        }
                    }
                }

                if (consumeRequest != null && !consumeRequest.getProcessQueue().isDropped())
                {
                    List<MessageExt> messages = consumeRequest.getMessageExts();
                    long offset = consumeRequest.getProcessQueue().removeMessage(messages);
                    assignedMessageQueue.updateConsumeOffset(consumeRequest.getMessageQueue(), offset);
                    //If namespace not null , reset Topic without namespace.
                    this.resetTopic(messages);
                    return messages;
                }
            }
            catch (Exception ignore)
            {

            }

            return ContainerExt.Empty<MessageExt>();
            //return Array.Empty<MessageExt>().ToList();
            //return Collections.emptyList();
        }

        public void pause(ICollection<MessageQueue> messageQueues)
        {
            assignedMessageQueue.pause(messageQueues);
        }

        public void resume(ICollection<MessageQueue> messageQueues)
        {
            assignedMessageQueue.resume(messageQueues);
        }

        /// <summary>
        /// synchronized
        /// </summary>
        /// <param name="messageQueue"></param>
        /// <param name="offset"></param>
        [MethodImpl(MethodImplOptions.Synchronized)]
        public void seek(MessageQueue messageQueue, long offset)
        {
            if (!assignedMessageQueue.messageQueues().Contains(messageQueue))
            {
                if (subscriptionType == SubscriptionType.SUBSCRIBE)
                {
                    throw new MQClientException("The message queue is not in assigned list, may be rebalancing, message queue: " + messageQueue, null);
                }
                else
                {
                    throw new MQClientException("The message queue is not in assigned list, message queue: " + messageQueue, null);
                }
            }
            long minoffset = minOffset(messageQueue);
            long maxoffset = maxOffset(messageQueue);
            if (offset < minoffset || offset > maxoffset)
            {
                throw new MQClientException("Seek offset illegal, seek offset = " + offset + ", min offset = " + minoffset + ", max offset = " + maxoffset, null);
            }
            Object objLock = messageQueueLock.fetchLockObject(messageQueue);
            lock(objLock) 
            {
                clearMessageQueueInCache(messageQueue);

                PullTaskImpl oldPullTaskImpl = this.taskTable.get(messageQueue);
                if (oldPullTaskImpl != null)
                {
                    oldPullTaskImpl.tryInterrupt();
                    this.taskTable.remove(messageQueue);
                }
                assignedMessageQueue.setSeekOffset(messageQueue, offset);
                if (!this.taskTable.ContainsKey(messageQueue))
                {
                    PullTaskImpl pullTask = new PullTaskImpl(messageQueue);
                    this.taskTable.put(messageQueue, pullTask);
                    this.scheduledThreadPoolExecutor.Schedule(pullTask, 0);
                }
            }
        }

        public void seekToBegin(MessageQueue messageQueue)
        {
            long begin = minOffset(messageQueue);
            this.seek(messageQueue, begin);
        }

        public void seekToEnd(MessageQueue messageQueue)
        {
            long end = maxOffset(messageQueue);
            this.seek(messageQueue, end);
        }

        private long maxOffset(MessageQueue messageQueue)
        {
            checkServiceState();
            return this.mQClientFactory.getMQAdminImpl().maxOffset(messageQueue);
        }

        private long minOffset(MessageQueue messageQueue)
        {
            checkServiceState();
            return this.mQClientFactory.getMQAdminImpl().minOffset(messageQueue);
        }

        private void removePullTaskCallback(String topic)
        {
            removePullTask(topic);
        }

        private void removePullTask(String topic)
        {
            //Iterator<Map.Entry<MessageQueue, PullTaskImpl>> it = this.taskTable.entrySet().iterator();
            //while (it.hasNext())
            foreach(var entry in taskTable)
            {
                //Map.Entry<MessageQueue, PullTaskImpl> next = it.next();
                if (entry.Key.getTopic().Equals(topic))
                {
                    entry.Value.setCancelled(true);
                    //it.remove();
                    taskTable.TryRemove(entry.Key, out _);
                }
            }
        }

        /// <summary>
        /// synchronized
        /// </summary>
        [MethodImpl(MethodImplOptions.Synchronized)]
        public void commitAll()
        {
            try
            {
                foreach (MessageQueue messageQueue in assignedMessageQueue.messageQueues())
                {
                    long consumerOffset = assignedMessageQueue.getConsumerOffset(messageQueue);
                    if (consumerOffset != -1)
                    {
                        ProcessQueue processQueue = assignedMessageQueue.getProcessQueue(messageQueue);
                        if (processQueue != null && !processQueue.isDropped())
                        {
                            updateConsumeOffset(messageQueue, consumerOffset);
                        }
                    }
                }
                if (defaultLitePullConsumer.getMessageModel() == MessageModel.BROADCASTING)
                {
                    offsetStore.persistAll(assignedMessageQueue.messageQueues());
                }
            }
            catch (Exception e)
            {
                log.Error("An error occurred when update consume offset Automatically.");
            }
        }

        private void updatePullOffset(MessageQueue messageQueue, long nextPullOffset, ProcessQueue processQueue)
        {
            if (assignedMessageQueue.getSeekOffset(messageQueue) == -1)
            {
                assignedMessageQueue.updatePullOffset(messageQueue, nextPullOffset, processQueue);
            }
        }

        private void submitConsumeRequest(ConsumeRequest consumeRequest)
        {
            try
            {
                //consumeRequestCache.put(consumeRequest);
                consumeRequestCache.Enqueue(consumeRequest);
            }
            catch (Exception e)
            {
                log.Error("Submit consumeRequest error", e.ToString());
            }
        }

        private long fetchConsumeOffset(MessageQueue messageQueue)
        {
            checkServiceState();
            long offset = this.rebalanceImpl.computePullFromWhereWithException(messageQueue);
            return offset;
        }

        public long committed(MessageQueue messageQueue)
        {
            checkServiceState();
            long offset = this.offsetStore.readOffset(messageQueue, ReadOffsetType.MEMORY_FIRST_THEN_STORE);
            if (offset == -2)
            {
                throw new MQClientException("Fetch consume offset from broker exception", null);
            }
            return offset;
        }

        private void clearMessageQueueInCache(MessageQueue messageQueue)
        {
            ProcessQueue processQueue = assignedMessageQueue.getProcessQueue(messageQueue);
            if (processQueue != null)
            {
                processQueue.clear();
            }
            //Iterator<ConsumeRequest> iter = consumeRequestCache.iterator();
            //while (iter.hasNext())
            foreach(var entry in consumeRequestCache)
            {
                if (entry.getMessageQueue().Equals(messageQueue))
                {
                    //iter.remove(); //??? TODO
                }
            }
        }

        private long nextPullOffset(MessageQueue messageQueue)
        {
            long offset = -1;
            long seekOffset = assignedMessageQueue.getSeekOffset(messageQueue);
            if (seekOffset != -1)
            {
                offset = seekOffset;
                assignedMessageQueue.updateConsumeOffset(messageQueue, offset);
                assignedMessageQueue.setSeekOffset(messageQueue, -1);
            }
            else
            {
                offset = assignedMessageQueue.getPullOffset(messageQueue);
                if (offset == -1)
                {
                    offset = fetchConsumeOffset(messageQueue);
                }
            }
            return offset;
        }

        public long searchOffset(MessageQueue mq, long timestamp)
        {
            checkServiceState();
            return this.mQClientFactory.getMQAdminImpl().searchOffset(mq, timestamp);
        }


        class PullTaskImpl : IRunnable
        {

            static NLog.Logger log = NLog.LogManager.GetCurrentClassLogger();

            private DefaultLitePullConsumerImpl owner;

            public PullTaskImpl(DefaultLitePullConsumerImpl owner)
            {
                this.owner = owner;
            }


            private readonly MessageQueue messageQueue;
            private volatile bool cancelled = false;
            private Thread currentThread;

            public PullTaskImpl(MessageQueue messageQueue)
            {
                this.messageQueue = messageQueue;
            }

            public void tryInterrupt()
            {
                setCancelled(true);
                if (currentThread == null)
                {
                    return;
                }
                //if (!currentThread.isInterrupted())
                //{
                //    currentThread.interrupt();
                //}
                if (!currentThread.IsAlive)  //???
                {
                    currentThread.Interrupt();
                }
            }

            public void run()
            {

                if (!this.isCancelled())
                {

                    this.currentThread = Thread.CurrentThread;

                    if (owner.assignedMessageQueue.isPaused(messageQueue))
                    {
                        owner.scheduledThreadPoolExecutor.Schedule(this, PULL_TIME_DELAY_MILLS_WHEN_PAUSE);
                        log.Debug("Message Queue: {} has been paused!", messageQueue);
                        return;
                    }

                    ProcessQueue processQueue = owner.assignedMessageQueue.getProcessQueue(messageQueue);

                    if (null == processQueue || processQueue.isDropped())
                    {
                        log.Info("The message queue not be able to poll, because it's dropped. group={}, messageQueue={}", 
                            owner.defaultLitePullConsumer.getConsumerGroup(), this.messageQueue);
                        return;
                    }

                    if ((long)owner.consumeRequestCache.Count * owner.defaultLitePullConsumer.getPullBatchSize() > owner.defaultLitePullConsumer.getPullThresholdForAll())
                    {
                        owner.scheduledThreadPoolExecutor.Schedule(this, PULL_TIME_DELAY_MILLS_WHEN_FLOW_CONTROL);
                        if ((owner.consumeRequestFlowControlTimes++ % 1000) == 0)
                        {
                            log.Warn("The consume request count exceeds threshold {}, so do flow control, consume request count={}, flowControlTimes={}", 
                                owner.consumeRequestCache.Count, owner.consumeRequestFlowControlTimes);
                        }
                        return;
                    }

                    long cachedMessageCount = processQueue.getMsgCount().get();
                    long cachedMessageSizeInMiB = processQueue.getMsgSize().get() / (1024 * 1024);

                    if (cachedMessageCount > owner.defaultLitePullConsumer.getPullThresholdForQueue())
                    {
                        owner.scheduledThreadPoolExecutor.Schedule(this, PULL_TIME_DELAY_MILLS_WHEN_FLOW_CONTROL);
                        if ((owner.queueFlowControlTimes++ % 1000) == 0)
                        {
                            log.Warn(
                                "The cached message count exceeds the threshold {}, so do flow control, minOffset={}, maxOffset={}, count={}, size={} MiB, flowControlTimes={}",
                                owner.defaultLitePullConsumer.getPullThresholdForQueue(), processQueue.getMsgTreeMap().First().Key, processQueue.getMsgTreeMap().Last().Key, 
                                cachedMessageCount, cachedMessageSizeInMiB, owner.queueFlowControlTimes);
                        }
                        return;
                    }

                    if (cachedMessageSizeInMiB > owner.defaultLitePullConsumer.getPullThresholdSizeForQueue())
                    {
                        owner.scheduledThreadPoolExecutor.Schedule(this, PULL_TIME_DELAY_MILLS_WHEN_FLOW_CONTROL);
                        if ((owner.queueFlowControlTimes++ % 1000) == 0)
                        {
                            log.Warn(
                                "The cached message size exceeds the threshold {} MiB, so do flow control, minOffset={}, maxOffset={}, count={}, size={} MiB, flowControlTimes={}",
                                owner.defaultLitePullConsumer.getPullThresholdSizeForQueue(), processQueue.getMsgTreeMap().First().Key, processQueue.getMsgTreeMap().Last().Key, 
                                cachedMessageCount, cachedMessageSizeInMiB, owner.queueFlowControlTimes);
                        }
                        return;
                    }

                    if (processQueue.getMaxSpan() > owner.defaultLitePullConsumer.getConsumeMaxSpan())
                    {
                        owner.scheduledThreadPoolExecutor.Schedule(this, PULL_TIME_DELAY_MILLS_WHEN_FLOW_CONTROL);
                        if ((owner.queueMaxSpanFlowControlTimes++ % 1000) == 0)
                        {
                            log.Warn(
                                "The queue's messages, span too long, so do flow control, minOffset={}, maxOffset={}, maxSpan={}, flowControlTimes={}",
                                processQueue.getMsgTreeMap().First().Key, processQueue.getMsgTreeMap().Last().Key, processQueue.getMaxSpan(), owner.queueMaxSpanFlowControlTimes);
                        }
                        return;
                    }

                    long offset = 0L;
                    try
                    {
                        offset = owner.nextPullOffset(messageQueue);
                    }
                    catch (Exception e)
                    {
                        log.Error("Failed to get next pull offset", e.ToString());
                        owner.scheduledThreadPoolExecutor.Schedule(this, PULL_TIME_DELAY_MILLS_ON_EXCEPTION);
                        return;
                    }

                    if (this.isCancelled() || processQueue.isDropped())
                    {
                        return;
                    }
                    long pullDelayTimeMills = 0;
                    try
                    {
                        SubscriptionData subscriptionData;
                        String topic = this.messageQueue.getTopic();
                        if (owner.subscriptionType == SubscriptionType.SUBSCRIBE)
                        {
                            subscriptionData = owner.rebalanceImpl.getSubscriptionInner().get(topic);
                        }
                        else
                        {
                            subscriptionData = FilterAPI.buildSubscriptionData(topic, SubscriptionData.SUB_ALL);
                        }

                        PullResult pullResult = owner.pull(messageQueue, subscriptionData, offset, owner.defaultLitePullConsumer.getPullBatchSize());
                        if (this.isCancelled() || processQueue.isDropped())
                        {
                            return;
                        }
                        switch (pullResult.getPullStatus())
                        {
                            case PullStatus.FOUND:
                                object objLock = owner.messageQueueLock.fetchLockObject(messageQueue);
                                lock(objLock)
                                {
                                    if (pullResult.getMsgFoundList() != null && !pullResult.getMsgFoundList().isEmpty() && owner.assignedMessageQueue.getSeekOffset(messageQueue) == -1)
                                    {
                                        processQueue.putMessage(pullResult.getMsgFoundList());
                                        owner.submitConsumeRequest(new ConsumeRequest(pullResult.getMsgFoundList(), messageQueue, processQueue));
                                    }
                                }
                                break;
                            case PullStatus.OFFSET_ILLEGAL:
                                log.Warn("The pull request offset illegal, {}", pullResult.ToString());
                                break;
                            default:
                                break;
                        }
                        owner.updatePullOffset(messageQueue, pullResult.getNextBeginOffset(), processQueue);
                    }
                    catch (ThreadInterruptedException interruptedException)
                    {
                        log.Warn("Polling thread was interrupted.", interruptedException.ToString());
                    }
                    catch (Exception e)
                    {
                        pullDelayTimeMills = owner.pullTimeDelayMillsWhenException;
                        log.Error("An error occurred in pull message process.", e.ToString());
                    }

                    if (!this.isCancelled())
                    {
                        owner.scheduledThreadPoolExecutor.Schedule(this, pullDelayTimeMills);
                    }
                    else
                    {
                        log.Warn("The Pull Task is cancelled after doPullTask, {}", messageQueue);
                    }
                }
            }

            public bool isCancelled()
            {
                return cancelled;
            }

            public void setCancelled(bool cancelled)
            {
                this.cancelled = cancelled;
            }

            public MessageQueue getMessageQueue()
            {
                return messageQueue;
            }
        }


        private PullResult pull(MessageQueue mq, SubscriptionData subscriptionData, long offset, int maxNums)
        {
            return pull(mq, subscriptionData, offset, maxNums, this.defaultLitePullConsumer.getConsumerPullTimeoutMillis());
        }

        private PullResult pull(MessageQueue mq, SubscriptionData subscriptionData, long offset, int maxNums, long timeout)
        {
            return this.pullSyncImpl(mq, subscriptionData, offset, maxNums, true, timeout);
        }

        private PullResult pullSyncImpl(MessageQueue mq, SubscriptionData subscriptionData, long offset, int maxNums,
            bool block,
            long timeout)
        {

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

            int sysFlag = PullSysFlag.buildSysFlag(false, block, true, false, true);

            long timeoutMillis = block ? this.defaultLitePullConsumer.getConsumerTimeoutMillisWhenSuspend() : timeout;

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
                this.defaultLitePullConsumer.getBrokerSuspendMaxTimeMillis(),
                timeoutMillis,
                CommunicationMode.SYNC,
                null
            );
            this.pullAPIWrapper.processPullResult(mq, pullResult, subscriptionData);
            if (this.consumeMessageHookList.Count > 0)
            {
                ConsumeMessageContext consumeMessageContext = new ConsumeMessageContext();
                consumeMessageContext.setNamespace(defaultLitePullConsumer.getNamespace());
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

        private void resetTopic(List<MessageExt> msgList)
        {
            if (null == msgList || msgList.Count == 0)
            {
                return;
            }

            //If namespace not null , reset Topic without namespace.
            foreach (MessageExt messageExt in msgList)
            {
                if (null != this.defaultLitePullConsumer.getNamespace())
                {
                    messageExt.setTopic(NamespaceUtil.withoutNamespace(messageExt.getTopic(), this.defaultLitePullConsumer.getNamespace()));
                }
            }

        }

        public void updateConsumeOffset(MessageQueue mq, long offset)
        {
            checkServiceState();
            this.offsetStore.updateOffset(mq, offset, false);
        }

        public String groupName()
        {
            return this.defaultLitePullConsumer.getConsumerGroup();
        }

        public MessageModel messageModel()
        {
            return this.defaultLitePullConsumer.getMessageModel();
        }

        public ConsumeType consumeType()
        {
            return ConsumeType.CONSUME_ACTIVELY;
        }

        public ConsumeFromWhere consumeFromWhere()
        {
            return ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET;
        }

        public HashSet<SubscriptionData> subscriptions()
        {
            HashSet<SubscriptionData> subSet = new HashSet<SubscriptionData>();
            subSet.addAll(this.rebalanceImpl.getSubscriptionInner().Values);
            return subSet;
        }

        public void doRebalance()
        {
            if (this.rebalanceImpl != null)
            {
                this.rebalanceImpl.doRebalance(false);
            }
        }

        public void persistConsumerOffset()
        {
            try
            {
                checkServiceState();
                HashSet<MessageQueue> mqs = null;//new HashSet<MessageQueue>();
                if (this.subscriptionType == SubscriptionType.SUBSCRIBE)
                {
                    //HashSet<MessageQueue> allocateMq = this.rebalanceImpl.getProcessQueueTable().Keys.ToHashSet();
                    //mqs.addAll(allocateMq);
                    mqs = this.rebalanceImpl.getProcessQueueTable().Keys.ToHashSet();
                }
                else if (this.subscriptionType == SubscriptionType.ASSIGN)
                {
                    mqs = new HashSet<MessageQueue>();
                    HashSet<MessageQueue> assignedMessageQueue = this.assignedMessageQueue.getAssignedMessageQueues();
                    mqs.addAll(assignedMessageQueue);
                }
                this.offsetStore.persistAll(mqs);
            }
            catch (Exception e)
            {
                log.Error("Persist consumer offset error for group: {} ", this.defaultLitePullConsumer.getConsumerGroup(), e);
            }
        }

        public void updateTopicSubscribeInfo(String topic, HashSet<MessageQueue> info)
        {
            var subTable = this.rebalanceImpl.getSubscriptionInner();
            if (subTable != null)
            {
                if (subTable.ContainsKey(topic))
                {
                    this.rebalanceImpl.getTopicSubscribeInfoTable().put(topic, info);
                }
            }
        }

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

        public bool isUnitMode()
        {
            return this.defaultLitePullConsumer.isUnitMode();
        }

        public ConsumerRunningInfo consumerRunningInfo()
        {
            ConsumerRunningInfo info = new ConsumerRunningInfo();

            Properties prop = MixAll.object2Properties(this.defaultLitePullConsumer);
            prop.put(ConsumerRunningInfo.PROP_CONSUMER_START_TIMESTAMP, this.consumerStartTimestamp.ToString());
            info.properties = prop;
            info.subscriptionSet.addAll(this.subscriptions());
            return info;
        }

        private void updateConsumeOffsetToBroker(MessageQueue mq, long offset, bool isOneway)
        {
            this.offsetStore.updateConsumeOffsetToBroker(mq, offset, isOneway);
        }

        public OffsetStore getOffsetStore()
        {
            return offsetStore;
        }

        public DefaultLitePullConsumer getDefaultLitePullConsumer()
        {
            return defaultLitePullConsumer;
        }

        public HashSet<MessageQueue> fetchMessageQueues(String topic)
        {
            checkServiceState();
            HashSet<MessageQueue> result = this.mQClientFactory.getMQAdminImpl().fetchSubscribeMessageQueues(topic);
            return parseMessageQueues(result);
        }

        /// <summary>
        /// synchronized
        /// </summary>
        [MethodImpl(MethodImplOptions.Synchronized)]
        private void fetchTopicMessageQueuesAndCompare()
        {
            foreach (var entry in topicMessageQueueChangeListenerMap)
            {
                String topic = entry.Key;
                TopicMessageQueueChangeListener topicMessageQueueChangeListener = entry.Value;
                //HashSet<MessageQueue> oldMessageQueues = messageQueuesForTopic.get(topic)
                messageQueuesForTopic.TryGetValue(topic, out HashSet<MessageQueue> oldMessageQueues);
                HashSet<MessageQueue> newMessageQueues = fetchMessageQueues(topic);
                bool isChanged = !isSetEqual(newMessageQueues, oldMessageQueues);
                if (isChanged)
                {
                    //messageQueuesForTopic.put(topic, newMessageQueues);
                    messageQueuesForTopic[topic] = newMessageQueues;
                    if (topicMessageQueueChangeListener != null)
                    {
                        topicMessageQueueChangeListener.onChanged(topic, newMessageQueues);
                    }
                }
            }
        }

        private bool isSetEqual(HashSet<MessageQueue> set1, HashSet<MessageQueue> set2)
        {
            if (set1 == null && set2 == null)
            {
                return true;
            }

            if (set1 == null || set2 == null || set1.Count != set2.Count
                || set1.Count == 0 || set2.Count == 0)
            {
                return false;
            }

            //Iterator iter = set2.iterator();
            //bool isEqual = true;
            //while (iter.hasNext())
            //{
            //    if (!set1.contains(iter.next()))
            //    {
            //        isEqual = false;
            //    }
            //}

            bool isEqual = true;
            foreach (var item in set2)
            {
                if (!set1.Contains(item))
                {
                    isEqual = false;
                }
            }
            return isEqual;
        }

        /// <summary>
        /// synchronized
        /// </summary>
        [MethodImpl(MethodImplOptions.Synchronized)]
        public void registerTopicMessageQueueChangeListener(String topic, TopicMessageQueueChangeListener listener)
        {
            if (topic == null || listener == null)
            {
                throw new MQClientException("Topic or listener is null", null);
            }
            if (topicMessageQueueChangeListenerMap.ContainsKey(topic))
            {
                log.Warn("Topic {} had been registered, new listener will overwrite the old one", topic);
            }
            //topicMessageQueueChangeListenerMap.put(topic, listener);
            topicMessageQueueChangeListenerMap[topic] = listener;
            if (this.serviceState == ServiceState.RUNNING)
            {
                HashSet<MessageQueue> messageQueues = fetchMessageQueues(topic);
                //messageQueuesForTopic.put(topic, messageQueues);
                messageQueuesForTopic[topic] = messageQueues; //小心put不能直接用Add替换
            }
        }

        private HashSet<MessageQueue> parseMessageQueues(HashSet<MessageQueue> queueSet)
        {
            HashSet<MessageQueue> resultQueues = new HashSet<MessageQueue>();
            foreach (MessageQueue messageQueue in queueSet)
            {
                string userTopic = NamespaceUtil.withoutNamespace(messageQueue.getTopic(), this.defaultLitePullConsumer.getNamespace());
                resultQueues.Add(new MessageQueue(userTopic, messageQueue.getBrokerName(), messageQueue.getQueueId()));
            }
            return resultQueues;
        }


        public void setPullTimeDelayMillsWhenException(long pullTimeDelayMillsWhenException)
        {
            this.pullTimeDelayMillsWhenException = pullTimeDelayMillsWhenException;
        }




        public class ConsumeRequest
        {
            private readonly List<MessageExt> messageExts;
            private readonly MessageQueue messageQueue;
            private readonly ProcessQueue processQueue;

            public ConsumeRequest(List<MessageExt> messageExts, MessageQueue messageQueue, ProcessQueue processQueue)
            {
                this.messageExts = messageExts;
                this.messageQueue = messageQueue;
                this.processQueue = processQueue;
            }

            public List<MessageExt> getMessageExts()
            {
                return messageExts;
            }

            public MessageQueue getMessageQueue()
            {
                return messageQueue;
            }

            public ProcessQueue getProcessQueue()
            {
                return processQueue;
            }

        }

    }
}
