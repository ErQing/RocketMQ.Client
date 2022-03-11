using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace RocketMQ.Client
{
    public class MQClientInstance
    {
        private readonly static long LOCK_TIMEOUT_MILLIS = 3000;
        //private readonly InternalLogger log = ClientLogger.getLog();
        static NLog.Logger log = NLog.LogManager.GetCurrentClassLogger();
        private readonly ClientConfig clientConfig;
        private readonly int instanceIndex;
        private readonly string clientId;
        private readonly long bootTimestamp = TimeUtils.CurrentTimeMillisUTC();
        private readonly ConcurrentDictionary<string/* group */, MQProducerInner> producerTable = new ConcurrentDictionary<string, MQProducerInner>();
        private readonly ConcurrentDictionary<string/* group */, MQConsumerInner> consumerTable = new ConcurrentDictionary<string, MQConsumerInner>();
        private readonly ConcurrentDictionary<string/* group */, MQAdminExtInner> adminExtTable = new ConcurrentDictionary<string, MQAdminExtInner>();
        private readonly NettyClientConfig nettyClientConfig;
        private readonly MQClientAPIImpl mQClientAPIImpl;
        private readonly MQAdminImpl mQAdminImpl;
        private readonly ConcurrentDictionary<String/* Topic */, TopicRouteData> topicRouteTable = new ConcurrentDictionary<String, TopicRouteData>();
        //private readonly Lock lockNamesrv = new ReentrantLock();
        //private readonly Lock lockHeartbeat = new ReentrantLock();
        private readonly object lockNamesrv = new object();
        private readonly object lockHeartbeat = new object();
        private readonly ConcurrentDictionary<String/* Broker Name */, Dictionary<long/* brokerId */, String/* address */>> brokerAddrTable =
            new ConcurrentDictionary<String, Dictionary<long, String>>();
        private readonly ConcurrentDictionary<String/* Broker Name */, Dictionary<String/* address */, int>> brokerVersionTable =
            new ConcurrentDictionary<String, Dictionary<String, int>>();
        private readonly ScheduledExecutorService scheduledExecutorService;
        //private readonly ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactory()
        //{
        //    @Override
        //    public Thread newThread(Runnable r)
        //    {
        //        return new Thread(r, "MQClientFactoryScheduledThread");
        //    }
        //});
        private readonly ClientRemotingProcessor clientRemotingProcessor;
        private readonly PullMessageService pullMessageService;
        private readonly RebalanceService rebalanceService;
        private readonly DefaultMQProducer defaultMQProducer;
        private readonly ConsumerStatsManager consumerStatsManager;
        private readonly AtomicLong sendHeartbeatTimesTotal = new AtomicLong(0);
        private ServiceState serviceState = ServiceState.CREATE_JUST;
        private Random random = new Random();

        public MQClientInstance(ClientConfig clientConfig, int instanceIndex, string clientId)
            : this(clientConfig, instanceIndex, clientId, null)
        {

        }

        public MQClientInstance(ClientConfig clientConfig, int instanceIndex, string clientId, RPCHook rpcHook)
        {
            this.clientConfig = clientConfig;
            this.instanceIndex = instanceIndex;
            this.nettyClientConfig = new NettyClientConfig();
            this.nettyClientConfig.setClientCallbackExecutorThreads(clientConfig.getClientCallbackExecutorThreads());
            this.nettyClientConfig.setUseTLS(clientConfig.isUseTLS());
            this.clientRemotingProcessor = new ClientRemotingProcessor(this);
            this.mQClientAPIImpl = new MQClientAPIImpl(this.nettyClientConfig, this.clientRemotingProcessor, rpcHook, clientConfig);

            if (this.clientConfig.getNamesrvAddr() != null)
            {
                this.mQClientAPIImpl.updateNameServerAddressList(this.clientConfig.getNamesrvAddr());
                log.Info("user specified name server address: {}", this.clientConfig.getNamesrvAddr());
            }

            this.clientId = clientId;

            this.mQAdminImpl = new MQAdminImpl(this);

            this.pullMessageService = new PullMessageService(this);

            this.rebalanceService = new RebalanceService(this);

            this.defaultMQProducer = new DefaultMQProducer(MixAll.CLIENT_INNER_PRODUCER_GROUP);
            this.defaultMQProducer.resetClientConfig(clientConfig);

            this.consumerStatsManager = new ConsumerStatsManager(this.scheduledExecutorService);

            log.Info("Created a new client Instance, InstanceIndex:{}, ClientID:{}, ClientConfig:{}, ClientVersion:{}, SerializerType:{}",
                this.instanceIndex,
                this.clientId,
                this.clientConfig,
                MQVersion.getVersionDesc(MQVersion.CURRENT_VERSION), RemotingCommand.getSerializeTypeConfigInThisServer());
        }

        public static TopicPublishInfo topicRouteData2TopicPublishInfo(String topic, TopicRouteData route)
        {
            TopicPublishInfo info = new TopicPublishInfo();
            info.setTopicRouteData(route);
            if (route.orderTopicConf != null && route.orderTopicConf.Length > 0)
            {
                String[] brokers = route.orderTopicConf.Split(";");
                foreach (String broker in brokers)
                {
                    String[] item = broker.Split(":");
                    int nums = int.Parse(item[1]);
                    for (int i = 0; i < nums; i++)
                    {
                        MessageQueue mq = new MessageQueue(topic, item[0], i);
                        info.getMessageQueueList().Add(mq);
                    }
                }
                info.setOrderTopic(true);
            }
            else
            {
                List<QueueData> qds = route.queueDatas;
                //Collections.sort(qds);
                qds.Sort();
                foreach (QueueData qd in qds)
                {
                    if (PermName.isWriteable(qd.perm))
                    {
                        BrokerData brokerData = null;
                        foreach (BrokerData bd in route.brokerDatas)
                        {
                            if (bd.brokerName.Equals(qd.brokerName))
                            {
                                brokerData = bd;
                                break;
                            }
                        }

                        if (null == brokerData)
                        {
                            continue;
                        }

                        if (!brokerData.brokerAddrs.ContainsKey(MixAll.MASTER_ID))
                        {
                            continue;
                        }

                        for (int i = 0; i < qd.writeQueueNums; i++)
                        {
                            MessageQueue mq = new MessageQueue(topic, qd.brokerName, i);
                            info.getMessageQueueList().Add(mq);
                        }
                    }
                }

                info.setOrderTopic(false);
            }

            return info;
        }

        public static HashSet<MessageQueue> topicRouteData2TopicSubscribeInfo(String topic, TopicRouteData route)
        {
            HashSet<MessageQueue> mqList = new HashSet<MessageQueue>();
            List<QueueData> qds = route.queueDatas;
            foreach (QueueData qd in qds)
            {
                if (PermName.isReadable(qd.perm))
                {
                    for (int i = 0; i < qd.readQueueNums; i++)
                    {
                        MessageQueue mq = new MessageQueue(topic, qd.brokerName, i);
                        mqList.Add(mq);
                    }
                }
            }

            return mqList;
        }


        ///<exception cref="MQClientException"/>
        public void start()
        {

            lock (this)
            {
                switch (this.serviceState)
                {
                    case ServiceState.CREATE_JUST:
                        this.serviceState = ServiceState.START_FAILED;
                        // If not specified,looking address from name server
                        if (null == this.clientConfig.getNamesrvAddr())
                        {
                            this.mQClientAPIImpl.fetchNameServerAddr();
                        }
                        // Start request-response channel
                        this.mQClientAPIImpl.start();
                        // Start various schedule tasks
                        this.startScheduledTask();
                        // Start pull service
                        this.pullMessageService.start();
                        // Start rebalance service
                        this.rebalanceService.start();
                        // Start push service
                        this.defaultMQProducer.getDefaultMQProducerImpl().start(false);
                        log.Info("the client factory [{}] start OK", this.clientId);
                        this.serviceState = ServiceState.RUNNING;
                        break;
                    case ServiceState.START_FAILED:
                        throw new MQClientException("The Factory object[" + this.getClientId() + "] has been created before, and failed.", null);
                    default:
                        break;
                }
            }
        }

        private void startScheduledTask()
        {
            //    if (null == this.clientConfig.getNamesrvAddr())
            //    {
            //        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {

            //        @Override
            //        public void run()
            //        {
            //            try
            //            {
            //                MQClientInstance.this.mQClientAPIImpl.fetchNameServerAddr();
            //            }
            //            catch (Exception e)
            //            {
            //                log.Error("ScheduledTask fetchNameServerAddr exception", e);
            //            }
            //        }
            //    }, 1000 * 10, 1000 * 60 * 2, TimeUnit.MILLISECONDS);
            //}
            if (null == this.clientConfig.getNamesrvAddr())
            {
                scheduledExecutorService.ScheduleAtFixedRate(() =>
                {
                    try
                    {
                        mQClientAPIImpl.fetchNameServerAddr();
                    }
                    catch (Exception e)
                    {
                        log.Error("ScheduledTask fetchNameServerAddr exception", e.ToString());
                    }
                }, 1000 * 10, 1000 * 60 * 2);
            }



            //this.scheduledExecutorService.scheduleAtFixedRate(new Runnable()
            //{

            //    @Override
            //    public void run()
            //    {
            //        try
            //        {
            //            MQClientInstance.this.updateTopicRouteInfoFromNameServer();
            //        }
            //        catch (Exception e)
            //        {
            //            log.Error("ScheduledTask updateTopicRouteInfoFromNameServer exception", e);
            //        }
            //    }
            //}, 10, this.clientConfig.getPollNameServerInterval(), TimeUnit.MILLISECONDS);


            scheduledExecutorService.ScheduleAtFixedRate(() =>
            {
                try
                {
                    updateTopicRouteInfoFromNameServer();
                }
                catch (Exception e)
                {
                    log.Error("ScheduledTask updateTopicRouteInfoFromNameServer exception", e.ToString());
                }
            }, 10, clientConfig.getPollNameServerInterval());


            //this.scheduledExecutorService.scheduleAtFixedRate(new Runnable()
            //{

            //    @Override
            //    public void run()
            //    {
            //        try
            //        {
            //            MQClientInstance.this.cleanOfflineBroker();
            //            MQClientInstance.this.sendHeartbeatToAllBrokerWithLock();
            //        }
            //        catch (Exception e)
            //        {
            //            log.Error("ScheduledTask sendHeartbeatToAllBroker exception", e);
            //        }
            //    }
            //}, 1000, this.clientConfig.getHeartbeatBrokerInterval(), TimeUnit.MILLISECONDS);

            scheduledExecutorService.ScheduleAtFixedRate(() =>
            {
                try
                {
                    cleanOfflineBroker();
                    sendHeartbeatToAllBrokerWithLock();
                }
                catch (Exception e)
                {
                    log.Error("ScheduledTask updateTopicRouteInfoFromNameServer exception", e.ToString());
                }
            }, 1000, clientConfig.getHeartbeatBrokerInterval());


            //this.scheduledExecutorService.scheduleAtFixedRate(new Runnable()
            //{

            //    @Override
            //    public void run()
            //    {
            //        try
            //        {
            //            MQClientInstance.this.persistAllConsumerOffset();
            //        }
            //        catch (Exception e)
            //        {
            //            log.Error("ScheduledTask persistAllConsumerOffset exception", e);
            //        }
            //    }
            //}, 1000 * 10, this.clientConfig.getPersistConsumerOffsetInterval(), TimeUnit.MILLISECONDS);

            scheduledExecutorService.ScheduleAtFixedRate(() =>
            {
                try
                {
                    persistAllConsumerOffset();
                }
                catch (Exception e)
                {
                    log.Error("ScheduledTask persistAllConsumerOffset exception", e.ToString());
                }
            }, 1000 * 10, clientConfig.getPersistConsumerOffsetInterval());

            //this.scheduledExecutorService.scheduleAtFixedRate(new Runnable()
            //{

            //    @Override
            //    public void run()
            //    {
            //        try
            //        {
            //            MQClientInstance.this.adjustThreadPool();
            //        }
            //        catch (Exception e)
            //        {
            //            log.Error("ScheduledTask adjustThreadPool exception", e);
            //        }
            //    }
            //}, 1, 1, TimeUnit.MINUTES);

            scheduledExecutorService.ScheduleAtFixedRate(() =>
            {
                try
                {
                    adjustThreadPool();
                }
                catch (Exception e)
                {
                    log.Error("ScheduledTask adjustThreadPool exception", e.ToString());
                }
            }, TimeSpan.FromMinutes(1), TimeSpan.FromMinutes(1));
        }

        public string getClientId()
        {
            return clientId;
        }

        public void updateTopicRouteInfoFromNameServer()
        {
            HashSet<String> topicList = new HashSet<String>();

            // Consumer
            {
                //Iterator<Entry<String, MQConsumerInner>> it = this.consumerTable.entrySet().iterator();
                //while (it.hasNext())
                //{
                //    Entry<String, MQConsumerInner> entry = it.next();
                //    MQConsumerInner impl = entry.getValue();
                //    if (impl != null)
                //    {
                //        Set<SubscriptionData> subList = impl.subscriptions();
                //        if (subList != null)
                //        {
                //            for (SubscriptionData subData : subList)
                //            {
                //                topicList.add(subData.getTopic());
                //            }
                //        }
                //    }
                //}

                foreach (var item in consumerTable)
                {
                    MQConsumerInner impl = item.Value;
                    if (impl != null)
                    {
                        HashSet<SubscriptionData> subList = impl.subscriptions();
                        if (subList != null)
                        {
                            foreach (SubscriptionData subData in subList)
                            {
                                topicList.Add(subData.topic);
                            }
                        }
                    }
                }
            }

            // Producer
            {
                //Iterator<Entry<String, MQProducerInner>> it = this.producerTable.entrySet().iterator();
                //while (it.hasNext())
                //{
                //    Entry<String, MQProducerInner> entry = it.next();
                //    MQProducerInner impl = entry.getValue();
                //    if (impl != null)
                //    {
                //        Set<String> lst = impl.getPublishTopicList();
                //        topicList.addAll(lst);
                //    }
                //}

                foreach (var item in producerTable)
                {
                    MQProducerInner impl = item.Value;
                    if (impl != null)
                    {
                        HashSet<String> lst = impl.getPublishTopicList();
                        //topicList.addAll(lst);
                        topicList.AddAll(lst);
                    }
                }

            }

            foreach (String topic in topicList)
            {
                this.updateTopicRouteInfoFromNameServer(topic);
            }
        }

        /**
         * @param offsetTable
         * @param namespace
         * @return newOffsetTable
         */
        public Dictionary<MessageQueue, long> parseOffsetTableFromBroker(Dictionary<MessageQueue, long> offsetTable, string nameSpace)
        {
            Dictionary<MessageQueue, long> newOffsetTable = new Dictionary<MessageQueue, long>();
            if (UtilAll.isNotEmpty(nameSpace))
            {
                foreach (var entry in offsetTable)
                {
                    MessageQueue queue = entry.Key;
                    queue.setTopic(NamespaceUtil.withoutNamespace(queue.getTopic(), nameSpace));
                    newOffsetTable.Add(queue, entry.Value);
                }
            }
            else
            {
                //newOffsetTable.putAll(offsetTable);
                newOffsetTable.PutAll(offsetTable);
            }

            return newOffsetTable;
        }

        /**
         * Remove offline broker
         */
        private void cleanOfflineBroker()
        {
            try
            {
                //if (this.lockNamesrv.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS))
                if (Monitor.TryEnter(lockNamesrv, TimeSpan.FromMilliseconds(LOCK_TIMEOUT_MILLIS)))
                    try
                    {
                        ConcurrentDictionary<String, Dictionary<long, String>> updatedTable = new ConcurrentDictionary<String, Dictionary<long, String>>();

                        //Iterator<Entry<String, HashMap<Long, String>>> itBrokerTable = this.brokerAddrTable.entrySet().iterator();
                        //while (itBrokerTable.hasNext())
                        //{
                        //    Entry<String, HashMap<Long, String>> entry = itBrokerTable.next();
                        //    string brokerName = entry.getKey();
                        //    HashMap<Long, String> oneTable = entry.getValue();

                        //    HashMap<Long, String> cloneAddrTable = new HashMap<Long, String>();
                        //    cloneAddrTable.putAll(oneTable);

                        //    Iterator<Entry<Long, String>> it = cloneAddrTable.entrySet().iterator();
                        //    while (it.hasNext())
                        //    {
                        //        Entry<Long, String> ee = it.next();
                        //        string addr = ee.getValue();
                        //        if (!this.isBrokerAddrExistInTopicRouteTable(addr))
                        //        {
                        //            it.remove();
                        //            log.info("the broker addr[{} {}] is offline, remove it", brokerName, addr);
                        //        }
                        //    }

                        //    if (cloneAddrTable.isEmpty())
                        //    {
                        //        itBrokerTable.remove();
                        //        log.info("the broker[{}] name's host is offline, remove it", brokerName);
                        //    }
                        //    else
                        //    {
                        //        updatedTable.put(brokerName, cloneAddrTable);
                        //    }
                        //}

                        foreach (var entry in brokerAddrTable)
                        {
                            string brokerName = entry.Key;
                            Dictionary<long, string> oneTable = entry.Value;
                            Dictionary<long, string> cloneAddrTable = new Dictionary<long, string>();
                            cloneAddrTable.PutAll(oneTable);
                            foreach (var item in cloneAddrTable)
                            {
                                string addr = item.Value;
                                if (!this.isBrokerAddrExistInTopicRouteTable(addr))
                                {
                                    cloneAddrTable.Remove(item.Key);
                                    log.Info("the broker addr[{} {}] is offline, remove it", brokerName, addr);
                                }
                            }
                            if (cloneAddrTable.IsEmpty())
                            {
                                //itBrokerTable.remove();
                                brokerAddrTable.TryRemove(entry.Key, out _);
                                log.Info("the broker[{}] name's host is offline, remove it", brokerName);
                            }
                            else
                            {
                                updatedTable.Put(brokerName, cloneAddrTable);
                            }
                        }

                        if (updatedTable.Count > 0)
                        {
                            this.brokerAddrTable.PutAll(updatedTable);
                        }
                    }
                    finally
                    {
                        //this.lockNamesrv.unlock();
                        Monitor.Exit(lockNamesrv);
                    }
            }
            catch (ThreadInterruptedException e)
            {
                log.Warn("cleanOfflineBroker Exception", e);
            }
        }

        /// <exception cref="MQClientException"/>
        public void checkClientInBroker()
        {
            //Iterator<Entry<String, MQConsumerInner>> it = this.consumerTable.entrySet().iterator();
            foreach (var entry in consumerTable)
            {
                HashSet<SubscriptionData> subscriptionInner = entry.Value.subscriptions();
                if (subscriptionInner == null || subscriptionInner.IsEmpty())
                {
                    return;
                }

                foreach (SubscriptionData subscriptionData in subscriptionInner)
                {
                    if (ExpressionType.isTagType(subscriptionData.expressionType))
                    {
                        continue;
                    }
                    // may need to check one broker every cluster...
                    // assume that the configs of every broker in cluster are the the same.
                    string addr = findBrokerAddrByTopic(subscriptionData.topic);

                    if (addr != null)
                    {
                        try
                        {
                            this.getMQClientAPIImpl().checkClientInBroker(
                                addr, entry.Key, this.clientId, subscriptionData, clientConfig.getMqClientApiTimeout()
                            );
                        }
                        catch (Exception e)
                        {
                            if (e is MQClientException exception)
                            {
                                throw exception;
                            }
                            else
                            {
                                throw new MQClientException("Check client in broker error, maybe because you use "
                                    + subscriptionData.expressionType + " to filter message, but server has not been upgraded to support!"
                                    + "This error would not affect the launch of consumer, but may has impact on message receiving if you " +
                                    "have use the new features which are not supported by server, please check the log!", e);
                            }
                        }
                    }
                }
            }
        }

        public void sendHeartbeatToAllBrokerWithLock()
        {
            if (Monitor.TryEnter(lockHeartbeat))
            {
                try
                {
                    this.sendHeartbeatToAllBroker();
                    this.uploadFilterClassSource();
                }
                catch (Exception e)
                {
                    log.Error("sendHeartbeatToAllBroker exception", e.ToString());
                }
                finally
                {
                    //this.lockHeartbeat.unlock();
                    Monitor.Exit(lockHeartbeat);
                }
            }
            else
            {
                log.Warn("lock heartBeat, but failed. [{}]", this.clientId);
            }
        }

        private void persistAllConsumerOffset()
        {
            //Iterator<Entry<String, MQConsumerInner>> it = this.consumerTable.entrySet().iterator();
            //while (it.hasNext())
            //{
            //    Entry<String, MQConsumerInner> entry = it.next();
            //    MQConsumerInner impl = entry.getValue();
            //    impl.persistConsumerOffset();
            //}
            foreach (var entry in consumerTable)
            {
                MQConsumerInner impl = entry.Value;
                impl.persistConsumerOffset();
            }
        }

        public void adjustThreadPool()
        {
            //Iterator<Entry<String, MQConsumerInner>> it = this.consumerTable.entrySet().iterator();
            foreach (var entry in consumerTable)
            {
                MQConsumerInner impl = entry.Value;
                if (impl != null)
                {
                    try
                    {
                        if (impl is DefaultMQPushConsumerImpl)
                        {
                            DefaultMQPushConsumerImpl dmq = (DefaultMQPushConsumerImpl)impl;
                            dmq.adjustThreadPool();
                        }
                    }
                    catch (Exception e)
                    {
                    }
                }
            }
        }

        public bool updateTopicRouteInfoFromNameServer(String topic)
        {
            return updateTopicRouteInfoFromNameServer(topic, false, null);
        }

        private bool isBrokerAddrExistInTopicRouteTable(String addr)
        {
            //Iterator<Entry<String, TopicRouteData>> it = this.topicRouteTable.entrySet().iterator();
            foreach (var entry in topicRouteTable)
            {
                TopicRouteData topicRouteData = entry.Value;
                List<BrokerData> bds = topicRouteData.brokerDatas;
                foreach (BrokerData bd in bds)
                {
                    if (bd.brokerAddrs != null)
                    {
                        bool exist = bd.brokerAddrs.ContainsValue(addr);
                        if (exist)
                            return true;
                    }
                }
            }

            return false;
        }

        private void sendHeartbeatToAllBroker()
        {
            HeartbeatData heartbeatData = this.prepareHeartbeatData();
            bool producerEmpty = heartbeatData.producerDataSet.IsEmpty();
            bool consumerEmpty = heartbeatData.consumerDataSet.IsEmpty();
            if (producerEmpty && consumerEmpty)
            {
                log.Warn("sending heartbeat, but no consumer and no producer. [{}]", this.clientId);
                return;
            }

            if (!this.brokerAddrTable.IsEmpty())
            {
                long times = this.sendHeartbeatTimesTotal.GetAndIncrement();
                //Iterator<Entry<String, HashMap<Long, String>>> it = this.brokerAddrTable.entrySet().iterator();
                foreach (var entry in brokerAddrTable)
                {
                    string brokerName = entry.Key;
                    Dictionary<long, String> oneTable = entry.Value;
                    if (oneTable != null)
                    {
                        foreach (var entry1 in oneTable)
                        {
                            long id = entry1.Key;
                            string addr = entry1.Value;
                            if (addr != null)
                            {
                                if (consumerEmpty)
                                {
                                    if (id != MixAll.MASTER_ID)
                                        continue;
                                }

                                try
                                {
                                    int version = this.mQClientAPIImpl.sendHearbeat(addr, heartbeatData, clientConfig.getMqClientApiTimeout());
                                    if (!this.brokerVersionTable.ContainsKey(brokerName))
                                    {
                                        //this.brokerVersionTable.put(brokerName, new Dictionary<String, int>(4));
                                        this.brokerVersionTable[brokerName] = new Dictionary<String, int>(4);
                                    }
                                    //this.brokerVersionTable.get(brokerName).put(addr, version);
                                    this.brokerVersionTable[brokerName][addr] = version;
                                    if (times % 20 == 0)
                                    {
                                        log.Info("send heart beat to broker[{} {} {}] success", brokerName, id, addr);
                                        log.Info(heartbeatData.ToString());
                                    }
                                }
                                catch (Exception e)
                                {
                                    if (this.isBrokerInNameServer(addr))
                                    {
                                        log.Info("send heart beat to broker[{} {} {}] failed", brokerName, id, addr, e);
                                    }
                                    else
                                    {
                                        log.Info("send heart beat to broker[{} {} {}] exception, because the broker not up, forget it", brokerName,
                                            id, addr, e);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        private void uploadFilterClassSource()
        {
            //Iterator<Entry<String, MQConsumerInner>> it = this.consumerTable.entrySet().iterator();
            foreach (var entry in consumerTable)
            {
                MQConsumerInner consumer = entry.Value;
                if (ConsumeType.CONSUME_PASSIVELY == consumer.consumeType())
                {
                    HashSet<SubscriptionData> subscriptions = consumer.subscriptions();
                    foreach (SubscriptionData sub in subscriptions)
                    {
                        if (sub.classFilterMode && sub.filterClassSource != null)
                        {
                            string consumerGroup = consumer.groupName();
                            string className = sub.subString;
                            string topic = sub.topic;
                            string filterClassSource = sub.filterClassSource;
                            try
                            {
                                this.uploadFilterClassToAllFilterServer(consumerGroup, className, topic, filterClassSource);
                            }
                            catch (Exception e)
                            {
                                log.Error("uploadFilterClassToAllFilterServer Exception", e.ToString());
                            }
                        }
                    }
                }
            }
        }

        public bool updateTopicRouteInfoFromNameServer(String topic, bool isDefault,
            DefaultMQProducer defaultMQProducer)
        {
            try
            {
                //if (this.lockNamesrv.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS))
                if (Monitor.TryEnter(lockNamesrv, TimeSpan.FromMilliseconds(LOCK_TIMEOUT_MILLIS)))
                {
                    try
                    {
                        TopicRouteData topicRouteData;
                        if (isDefault && defaultMQProducer != null)
                        {
                            topicRouteData = this.mQClientAPIImpl.getDefaultTopicRouteInfoFromNameServer(defaultMQProducer.getCreateTopicKey(),
                                clientConfig.getMqClientApiTimeout());
                            if (topicRouteData != null)
                            {
                                foreach (QueueData data in topicRouteData.queueDatas)
                                {
                                    int queueNums = Math.Min(defaultMQProducer.getDefaultTopicQueueNums(), data.readQueueNums);
                                    data.readQueueNums = queueNums;
                                    data.writeQueueNums = queueNums;
                                }
                            }
                        }
                        else
                        {
                            topicRouteData = this.mQClientAPIImpl.getTopicRouteInfoFromNameServer(topic, clientConfig.getMqClientApiTimeout());
                        }
                        if (topicRouteData != null)
                        {
                            TopicRouteData old = this.topicRouteTable.Get(topic);
                            bool changed = topicRouteDataIsChange(old, topicRouteData);
                            if (!changed)
                            {
                                changed = this.isNeedUpdateTopicRouteInfo(topic);
                            }
                            else
                            {
                                log.Info("the topic[{}] route info changed, old[{}] ,new[{}]", topic, old, topicRouteData);
                            }

                            if (changed)
                            {
                                TopicRouteData cloneTopicRouteData = topicRouteData.cloneTopicRouteData();

                                foreach (BrokerData bd in topicRouteData.brokerDatas)
                                {
                                    //this.brokerAddrTable.put(bd.getBrokerName(), bd.getBrokerAddrs());
                                    this.brokerAddrTable[bd.brokerName] = bd.brokerAddrs;
                                }

                                // Update Pub info
                                if (!producerTable.IsEmpty())
                                {
                                    TopicPublishInfo publishInfo = topicRouteData2TopicPublishInfo(topic, topicRouteData);
                                    publishInfo.setHaveTopicRouterInfo(true);
                                    //Iterator<Entry<String, MQProducerInner>> it = this.producerTable.entrySet().iterator();
                                    foreach (var entry in producerTable)
                                    {
                                        MQProducerInner impl = entry.Value;
                                        if (impl != null)
                                        {
                                            impl.updateTopicPublishInfo(topic, publishInfo);
                                        }
                                    }
                                }

                                // Update sub info
                                if (!consumerTable.IsEmpty())
                                {
                                    HashSet<MessageQueue> subscribeInfo = topicRouteData2TopicSubscribeInfo(topic, topicRouteData);
                                    //Iterator<Entry<String, MQConsumerInner>> it = this.consumerTable.entrySet().iterator();
                                    foreach (var entry in consumerTable)
                                    {
                                        MQConsumerInner impl = entry.Value;
                                        if (impl != null)
                                        {
                                            impl.updateTopicSubscribeInfo(topic, subscribeInfo);
                                        }
                                    }
                                }
                                log.Info("topicRouteTable.put. Topic = {}, TopicRouteData[{}]", topic, cloneTopicRouteData);
                                //this.topicRouteTable.put(topic, cloneTopicRouteData);
                                this.topicRouteTable[topic] = cloneTopicRouteData;
                                return true;
                            }
                        }
                        else
                        {
                            log.Warn("updateTopicRouteInfoFromNameServer, getTopicRouteInfoFromNameServer return null, Topic: {}. [{}]", topic, this.clientId);
                        }
                    }
                    catch (MQClientException e)
                    {
                        if (!topic.StartsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX) && !topic.Equals(TopicValidator.AUTO_CREATE_TOPIC_KEY_TOPIC))
                        {
                            log.Warn("updateTopicRouteInfoFromNameServer Exception", e);
                        }
                    }
                    catch (RemotingException e)
                    {
                        log.Error("updateTopicRouteInfoFromNameServer Exception", e);
                        //throw new IllegalStateException(e);
                        throw new InvalidOperationException();
                    }
                    finally
                    {
                        //this.lockNamesrv.unlock();
                        Monitor.Exit(lockNamesrv);
                    }
                }
                else
                {
                    log.Warn("updateTopicRouteInfoFromNameServer tryLock timeout {}ms. [{}]", LOCK_TIMEOUT_MILLIS, this.clientId);
                }
            }
            catch (ThreadInterruptedException e)
            {
                log.Warn("updateTopicRouteInfoFromNameServer Exception", e);
            }

            return false;
        }

        private HeartbeatData prepareHeartbeatData()
        {
            HeartbeatData heartbeatData = new HeartbeatData();

            // clientID
            heartbeatData.clientID = this.clientId;

            // Consumer
            foreach (var entry in consumerTable)
            {
                MQConsumerInner impl = entry.Value;
                if (impl != null)
                {
                    ConsumerData consumerData = new ConsumerData();
                    consumerData.groupName = impl.groupName();
                    consumerData.consumeType = impl.consumeType();
                    consumerData.messageModel = impl.messageModel();
                    consumerData.consumeFromWhere = impl.consumeFromWhere();
                    //consumerData.getSubscriptionDataSet().addAll(impl.subscriptions());
                    consumerData.subscriptionDataSet.AddAll(impl.subscriptions());
                    consumerData.unitMode = impl.isUnitMode();

                    heartbeatData.consumerDataSet.Add(consumerData);
                }
            }

            // Producer
            foreach (var entry in producerTable)
            {
                MQProducerInner impl = entry.Value;
                if (impl != null)
                {
                    ProducerData producerData = new ProducerData();
                    producerData.groupName = entry.Key;

                    heartbeatData.producerDataSet.Add(producerData);
                }
            }

            return heartbeatData;
        }

        private bool isBrokerInNameServer(String brokerAddr)
        {
            //Iterator<Entry<String, TopicRouteData>> it = this.topicRouteTable.entrySet().iterator();
            foreach (var entry in topicRouteTable)
            {
                List<BrokerData> brokerDatas = entry.Value.brokerDatas;
                foreach (BrokerData bd in brokerDatas)
                {
                    bool contain = bd.brokerAddrs.ContainsValue(brokerAddr);
                    if (contain)
                        return true;
                }
            }

            return false;
        }

        /**
         * This method will be removed in the version 5.0.0,because filterServer was removed,and method
         * <code>subscribe(final string topic, final MessageSelector messageSelector)</code> is recommended.
         */
        ///<exception cref="UnsupportedEncodingException"/>
        [Obsolete]//@Deprecated
        private void uploadFilterClassToAllFilterServer(String consumerGroup, string fullClassName, string topic, string filterClassSource)
        {
            byte[] classBody = null;
            int classCRC = 0;
            try
            {
                classBody = filterClassSource.getBytes(MixAll.DEFAULT_CHARSET);
                classCRC = UtilAll.crc32(classBody);
            }
            catch (Exception e1)
            {
                log.Warn("uploadFilterClassToAllFilterServer Exception, ClassName: {} {}",
                    fullClassName,
                    RemotingHelper.exceptionSimpleDesc(e1));
            }

            //TopicRouteData topicRouteData = this.topicRouteTable.get(topic);
            topicRouteTable.TryGetValue(topic, out TopicRouteData topicRouteData);
            if (topicRouteData != null
                && topicRouteData.filterServerTable != null && !topicRouteData.filterServerTable.IsEmpty())
            {
                //Iterator<Entry<String, List<String>>> it = topicRouteData.getFilterServerTable().entrySet().iterator();
                var filterServerTable = topicRouteData.filterServerTable;
                foreach (var entry in filterServerTable)
                {
                    List<String> value = entry.Value;
                    foreach (String fsAddr in value)
                    {
                        try
                        {
                            this.mQClientAPIImpl.registerMessageFilterClass(fsAddr, consumerGroup, topic, fullClassName, classCRC, classBody, 5000);
                            log.Info("register message class filter to {} OK, ConsumerGroup: {} Topic: {} ClassName: {}", fsAddr, consumerGroup,
                                topic, fullClassName);
                        }
                        catch (Exception e)
                        {
                            log.Error("uploadFilterClassToAllFilterServer Exception", e);
                        }
                    }
                }
            }
            else
            {
                log.Warn("register message class filter failed, because no filter server, ConsumerGroup: {} Topic: {} ClassName: {}",
                    consumerGroup, topic, fullClassName);
            }
        }

        private bool topicRouteDataIsChange(TopicRouteData olddata, TopicRouteData nowdata)
        {
            if (olddata == null || nowdata == null)
                return true;
            TopicRouteData old = olddata.cloneTopicRouteData();
            TopicRouteData now = nowdata.cloneTopicRouteData();
            //Collections.sort(old.getQueueDatas());
            //Collections.sort(old.getBrokerDatas());
            //Collections.sort(now.getQueueDatas());
            //Collections.sort(now.getBrokerDatas());

            old.queueDatas.Sort();
            old.brokerDatas.Sort();
            now.queueDatas.Sort();
            now.brokerDatas.Sort();
            return !old.Equals(now);
        }

        private bool isNeedUpdateTopicRouteInfo(String topic)
        {
            bool result = false;
            {
                //Iterator<Entry<String, MQProducerInner>> it = this.producerTable.entrySet().iterator();
                //while (it.hasNext() && !result)
                //{
                //    Entry<String, MQProducerInner> entry = it.next();
                //    MQProducerInner impl = entry.getValue();
                //    if (impl != null)
                //    {
                //        result = impl.isPublishTopicNeedUpdate(topic);
                //    }
                //}
                foreach (var entry in producerTable)
                {
                    MQProducerInner impl = entry.Value;
                    if (impl != null)
                    {
                        result = impl.isPublishTopicNeedUpdate(topic);
                        if (result) break;
                    }
                }
            }

            {
                //Iterator<Entry<String, MQConsumerInner>> it = this.consumerTable.entrySet().iterator();
                //while (it.hasNext() && !result)
                //{
                //    Entry<String, MQConsumerInner> entry = it.next();
                //    MQConsumerInner impl = entry.getValue();
                //    if (impl != null)
                //    {
                //        result = impl.isSubscribeTopicNeedUpdate(topic);
                //    }
                //}
                foreach (var entry in consumerTable)
                {
                    MQConsumerInner impl = entry.Value;
                    if (impl != null)
                    {
                        result = impl.isSubscribeTopicNeedUpdate(topic);
                        if (result) break;
                    }
                }
            }

            return result;
        }

        public void shutdown()
        {
            // Consumer
            if (!this.consumerTable.IsEmpty())
                return;

            // AdminExt
            if (!this.adminExtTable.IsEmpty())
                return;

            // Producer
            if (this.producerTable.Count > 1)
                return;

            lock (this)
            {
                switch (this.serviceState)
                {
                    case ServiceState.CREATE_JUST:
                        break;
                    case ServiceState.RUNNING:
                        this.defaultMQProducer.getDefaultMQProducerImpl().shutdown(false);

                        this.serviceState = ServiceState.SHUTDOWN_ALREADY;
                        this.pullMessageService.shutdown(true);
                        this.scheduledExecutorService.Shutdown();
                        this.mQClientAPIImpl.shutdown();
                        this.rebalanceService.shutdown();

                        MQClientManager.getInstance().removeClientFactory(this.clientId);
                        log.Info("the client factory [{}] shutdown OK", this.clientId);
                        break;
                    case ServiceState.SHUTDOWN_ALREADY:
                        break;
                    default:
                        break;
                }
            }
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public bool registerConsumer(String group, MQConsumerInner consumer)
        {
            if (null == group || null == consumer)
            {
                return false;
            }

            //java:putIfAbsent null = 之前不存在
            //return:the previous value associated with the specified key, or null if there was no mapping for the key.
            //(A null return can also indicate that the map previously associated null with the key, if the implementation supports null values.)
            //MQConsumerInner prev = this.consumerTable.putIfAbsent(group, consumer);
            var flag = this.consumerTable.TryAdd(group, consumer);
            if (!flag)
            {
                log.Warn("the consumer group[" + group + "] exist already.");
                return false;
            }

            return true;
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public void unregisterConsumer(String group)
        {
            this.consumerTable.TryRemove(group, out _);
            this.unregisterClient(null, group);
        }

        private void unregisterClient(String producerGroup, string consumerGroup)
        {
            //Iterator<Entry<String, HashMap<Long, String>>> it = this.brokerAddrTable.entrySet().iterator();
            //while (it.hasNext())
            foreach (var entry in brokerAddrTable)
            {
                //Entry<String, HashMap<Long, String>> entry = it.next();
                string brokerName = entry.Key;
                Dictionary<long, String> oneTable = entry.Value;
                if (oneTable != null)
                {
                    foreach (var entry1 in oneTable)
                    {
                        string addr = entry1.Value;
                        if (addr != null)
                        {
                            try
                            {
                                this.mQClientAPIImpl.unregisterClient(addr, this.clientId, producerGroup, consumerGroup, clientConfig.getMqClientApiTimeout());
                                log.Info("unregister client[Producer: {} Consumer: {}] from broker[{} {} {}] success", producerGroup, consumerGroup, brokerName, entry1.Key, addr);
                            }
                            catch (RemotingException e)
                            {
                                log.Warn("unregister client RemotingException from broker: {}, {}", addr, e.Message);
                            }
                            catch (ThreadInterruptedException e)
                            {
                                log.Warn("unregister client InterruptedException from broker: {}, {}", addr, e.Message);
                            }
                            catch (MQBrokerException e)
                            {
                                log.Warn("unregister client MQBrokerException from broker: {}, {}", addr, e.Message);
                            }
                        }
                    }
                }
            }
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public bool registerProducer(String group, DefaultMQProducerImpl producer)
        {
            if (null == group || null == producer)
            {
                return false;
            }

            //MQProducerInner prev = this.producerTable.putIfAbsent(group, producer);
            var flag = this.producerTable.TryAdd(group, producer);
            if (!flag)
            {
                log.Warn("the producer group[{}] exist already.", group);
                return false;
            }

            return true;
        }


        [MethodImpl(MethodImplOptions.Synchronized)]
        public void unregisterProducer(String group)
        {
            this.producerTable.TryRemove(group, out _);
            this.unregisterClient(group, null);
        }

        public bool registerAdminExt(String group, MQAdminExtInner admin)
        {
            if (null == group || null == admin)
            {
                return false;
            }

            //MQAdminExtInner prev = this.adminExtTable.putIfAbsent(group, admin);
            var flag = this.adminExtTable.TryAdd(group, admin);
            if (!flag)
            {
                log.Warn("the admin group[{}] exist already.", group);
                return false;
            }

            return true;
        }

        public void unregisterAdminExt(String group)
        {
            //this.adminExtTable.remove(group);
            this.adminExtTable.TryRemove(group, out _);
        }

        public void rebalanceImmediately()
        {
            this.rebalanceService.wakeup();
        }

        public void doRebalance()
        {
            foreach (var entry in this.consumerTable)
            {
                MQConsumerInner impl = entry.Value;
                if (impl != null)
                {
                    try
                    {
                        impl.doRebalance();
                    }
                    catch (Exception e)
                    {
                        log.Error("doRebalance exception", e.ToString());
                    }
                }
            }
        }

        public MQProducerInner selectProducer(String group)
        {
            //return this.producerTable.get(group);
            producerTable.TryGetValue(group, out MQProducerInner res);
            return res;
        }

        public MQConsumerInner selectConsumer(String group)
        {
            //return this.consumerTable.get(group);
            consumerTable.TryGetValue(group, out MQConsumerInner res);
            return res;
        }

        public string findBrokerAddressInPublish(String brokerName)
        {
            //HashMap<Long/* brokerId */, String/* address */> map = this.brokerAddrTable.get(brokerName);
            brokerAddrTable.TryGetValue(brokerName, out var map);
            if (map != null && !map.IsEmpty())
            {
                //return map.get(MixAll.MASTER_ID);
                map.TryGetValue(MixAll.MASTER_ID, out var res);
                return res;
            }
            return null;
        }

        public FindBrokerResult findBrokerAddressInSubscribe(String brokerName, long brokerId, bool onlyThisBroker)
        {
            string brokerAddr = null;
            bool slave = false;
            bool found = false;

            //HashMap<Long/* brokerId */, String/* address */> map = this.brokerAddrTable.get(brokerName);
            this.brokerAddrTable.TryGetValue(brokerName, out var map);
            if (map != null && !map.IsEmpty())
            {
                //brokerAddr = map.get(brokerId);
                map.TryGetValue(brokerId, out brokerAddr);
                slave = brokerId != MixAll.MASTER_ID;
                found = brokerAddr != null;

                if (!found && slave)
                {
                    //brokerAddr = map.get(brokerId + 1);
                    map.TryGetValue(brokerId + 1, out brokerAddr);
                    found = brokerAddr != null;
                }

                if (!found && !onlyThisBroker)
                {
                    //Entry<Long, String> entry = map.entrySet().iterator().next();
                    var it = map.GetEnumerator();
                    it.MoveNext();
                    var entry = it.Current;
                    brokerAddr = entry.Value;
                    slave = entry.Key != MixAll.MASTER_ID;
                    found = true;
                }
            }

            if (found)
            {
                return new FindBrokerResult(brokerAddr, slave, findBrokerVersion(brokerName, brokerAddr));
            }

            return null;
        }

        public int findBrokerVersion(String brokerName, string brokerAddr)
        {
            if (this.brokerVersionTable.ContainsKey(brokerName))
            {
                if (this.brokerVersionTable[brokerName].ContainsKey(brokerAddr))
                {
                    return this.brokerVersionTable[brokerName][brokerAddr];
                }
            }
            //To do need to fresh the version
            return 0;
        }

        public List<String> findConsumerIdList(String topic, string group)
        {
            string brokerAddr = this.findBrokerAddrByTopic(topic);
            if (null == brokerAddr)
            {
                this.updateTopicRouteInfoFromNameServer(topic);
                brokerAddr = this.findBrokerAddrByTopic(topic);
            }

            if (null != brokerAddr)
            {
                try
                {
                    return this.mQClientAPIImpl.getConsumerIdListByGroup(brokerAddr, group, clientConfig.getMqClientApiTimeout());
                }
                catch (Exception e)
                {
                    log.Warn("getConsumerIdListByGroup exception, " + brokerAddr + " " + group, e.ToString());
                }
            }

            return null;
        }

        public string findBrokerAddrByTopic(String topic)
        {
            TopicRouteData topicRouteData = this.topicRouteTable.Get(topic);
            if (topicRouteData != null)
            {
                List<BrokerData> brokers = topicRouteData.brokerDatas;
                if (!brokers.IsEmpty())
                {
                    //int index = random.nextInt(brokers.Count);
                    int index = random.Next(brokers.Count);
                    BrokerData bd = brokers[index % brokers.Count];
                    return bd.selectBrokerAddr();
                }
            }

            return null;
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public void resetOffset(String topic, string group, Dictionary<MessageQueue, long> offsetTable)
        {
            DefaultMQPushConsumerImpl consumer = null;
            try
            {
                MQConsumerInner impl = this.consumerTable.Get(group);
                if (impl != null && impl is DefaultMQPushConsumerImpl)
                {
                    consumer = (DefaultMQPushConsumerImpl)impl;
                }
                else
                {
                    log.Info("[reset-offset] consumer dose not exist. group={}", group);
                    return;
                }
                consumer.suspend();

                ConcurrentDictionary<MessageQueue, ProcessQueue> processQueueTable = consumer.getRebalanceImpl().getProcessQueueTable();
                foreach (var entry in processQueueTable)
                {
                    MessageQueue mq = entry.Key;
                    if (topic.Equals(mq.getTopic()) && offsetTable.ContainsKey(mq))
                    {
                        ProcessQueue pq = entry.Value;
                        pq.setDropped(true);
                        pq.clear();
                    }
                }

                try
                {
                    //TimeUnit.SECONDS.sleep(10);
                    Thread.Sleep(TimeSpan.FromSeconds(10));
                }
                catch (ThreadInterruptedException e)
                {

                }

                //Iterator<MessageQueue> iterator = processQueueTable.keySet().iterator();
                //while (iterator.hasNext())
                foreach (var entry in processQueueTable)
                {
                    //MessageQueue mq = iterator.next();
                    //long offset = offsetTable.get(entry.Key);
                    MessageQueue mq = entry.Key;
                    bool flag = offsetTable.TryGetValue(mq, out long offset);
                    //if (topic.Equals(mq.getTopic()) && offset != null)
                    if (topic.Equals(mq.getTopic()) && flag)
                    {
                        try
                        {
                            consumer.updateConsumeOffset(mq, offset);
                            consumer.getRebalanceImpl().removeUnnecessaryMessageQueue(mq, processQueueTable[mq]);
                            //iterator.remove();
                            processQueueTable.TryRemove(mq, out _);
                        }
                        catch (Exception e)
                        {
                            log.Warn("reset offset failed. group={}, {}", group, mq, e);
                        }
                    }
                }
            }
            finally
            {
                if (consumer != null)
                {
                    consumer.resume();
                }
            }
        }

        public Dictionary<MessageQueue, long> getConsumerStatus(String topic, string group)
        {
            //MQConsumerInner impl = this.consumerTable.get(group);
            consumerTable.TryGetValue(group, out MQConsumerInner impl);
            if (impl != null && impl is DefaultMQPushConsumerImpl)
            {
                DefaultMQPushConsumerImpl consumer = (DefaultMQPushConsumerImpl)impl;
                return consumer.getOffsetStore().cloneOffsetTable(topic);
            }
            else if (impl != null && impl is DefaultMQPullConsumerImpl)
            {
                DefaultMQPullConsumerImpl consumer = (DefaultMQPullConsumerImpl)impl;
                return consumer.getOffsetStore().cloneOffsetTable(topic);
            }
            else
            {
                return new Dictionary<MessageQueue, long>(0);
                //return (Dictionary<MessageQueue, long>)Enumerable.Empty<Dictionary<MessageQueue, long>>();
                //return Collections.EMPTY_MAP;
            }
        }

        public TopicRouteData getAnExistTopicRouteData(String topic)
        {
            topicRouteTable.TryGetValue(topic, out TopicRouteData res);
            return res;
            //return this.topicRouteTable.get(topic);
        }

        public MQClientAPIImpl getMQClientAPIImpl()
        {
            return mQClientAPIImpl;
        }

        public MQAdminImpl getMQAdminImpl()
        {
            return mQAdminImpl;
        }

        public long getBootTimestamp()
        {
            return bootTimestamp;
        }

        public ScheduledExecutorService getScheduledExecutorService()
        {
            return scheduledExecutorService;
        }

        public PullMessageService getPullMessageService()
        {
            return pullMessageService;
        }

        public DefaultMQProducer getDefaultMQProducer()
        {
            return defaultMQProducer;
        }

        public ConcurrentDictionary<String, TopicRouteData> getTopicRouteTable()
        {
            return topicRouteTable;
        }

        public ConsumeMessageDirectlyResult consumeMessageDirectly(MessageExt msg, string consumerGroup, string brokerName)
        {
            //MQConsumerInner mqConsumerInner = this.consumerTable.get(consumerGroup);
            consumerTable.TryGetValue(consumerGroup, out MQConsumerInner mqConsumerInner);
            if (null != mqConsumerInner)
            {
                DefaultMQPushConsumerImpl consumer = (DefaultMQPushConsumerImpl)mqConsumerInner;

                ConsumeMessageDirectlyResult result = consumer.getConsumeMessageService().consumeMessageDirectly(msg, brokerName);
                return result;
            }

            return null;
        }

        public ConsumerRunningInfo consumerRunningInfo(String consumerGroup)
        {
            //MQConsumerInner mqConsumerInner = this.consumerTable.get(consumerGroup);
            consumerTable.TryGetValue(consumerGroup, out MQConsumerInner mqConsumerInner);
            if (mqConsumerInner == null)
            {
                return null;
            }

            ConsumerRunningInfo consumerRunningInfo = mqConsumerInner.consumerRunningInfo();

            List<String> nsList = this.mQClientAPIImpl.getRemotingClient().getNameServerAddressList();

            StringBuilder strBuilder = new StringBuilder();
            if (nsList != null)
            {
                foreach (String addr in nsList)
                {
                    strBuilder.Append(addr).Append(";");
                }
            }

            string nsAddr = strBuilder.ToString();
            consumerRunningInfo.properties.Put(ConsumerRunningInfo.PROP_NAMESERVER_ADDR, nsAddr);
            consumerRunningInfo.properties.Put(ConsumerRunningInfo.PROP_CONSUME_TYPE, mqConsumerInner.consumeType().name());
            consumerRunningInfo.properties.Put(ConsumerRunningInfo.PROP_CLIENT_VERSION,
                MQVersion.getVersionDesc(MQVersion.CURRENT_VERSION));

            return consumerRunningInfo;
        }

        public ConsumerStatsManager getConsumerStatsManager()
        {
            return consumerStatsManager;
        }

        public NettyClientConfig getNettyClientConfig()
        {
            return nettyClientConfig;
        }

        public ClientConfig getClientConfig()
        {
            return clientConfig;
        }

    }

}
