using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;

namespace RocketMQ.Client
{
    public class AsyncTraceDispatcher : TraceDispatcher
    {
        //private final static InternalLogger log = ClientLogger.getLog();
        static NLog.Logger log = NLog.LogManager.GetCurrentClassLogger();
        private readonly static AtomicInteger COUNTER = new AtomicInteger();
        private readonly int queueSize;
        private readonly int traceQueueSize;
        private readonly int batchSize;
        private readonly int maxMsgSize;
        private readonly DefaultMQProducer traceProducer;
        //private readonly ThreadPoolExecutor traceExecutor;
        private readonly ExecutorService traceExecutor;
        // The last discard number of log
        private AtomicLong discardCount;
        private Thread worker;
        //private readonly ArrayBlockingQueue<TraceContext> traceContextQueue;
        private readonly BlockingQueue<TraceContext> traceContextQueue;
        //private ArrayBlockingQueue<Runnable> appenderQueue;
        //private ConcurrentQueue<Runnable> appenderQueue;
        private volatile Thread shutDownHook;
        private volatile bool stopped = false;
        private DefaultMQProducerImpl hostProducer;
        private DefaultMQPushConsumerImpl hostConsumer;
        private volatile ThreadLocalIndex sendWhichQueue = new ThreadLocalIndex();
        private string dispatcherId = UUID.randomUUID().ToString();
        private string traceTopicName;
        private AtomicBoolean isStarted = new AtomicBoolean(false);
        private AccessChannel accessChannel = AccessChannel.LOCAL;
        private string group;
        private TraceDispatcher.Type type;

        public AsyncTraceDispatcher(String group, TraceDispatcher.Type type, string traceTopicName, RPCHook rpcHook)
        {
            // queueSize is greater than or equal to the n power of 2 of value
            this.queueSize = 2048;
            this.traceQueueSize = 1024;
            this.batchSize = 100;
            this.maxMsgSize = 128000;
            this.discardCount = new AtomicLong(0L);
            //this.traceContextQueue = new ArrayBlockingQueue<TraceContext>(1024);
            this.traceContextQueue = BlockingQueue<TraceContext>.Create();
            this.group = group;
            this.type = type;

            //this.appenderQueue = new ArrayBlockingQueue<Runnable>(queueSize);
            this.traceContextQueue = BlockingQueue<TraceContext>.Create();
            if (!UtilAll.isBlank(traceTopicName))
            {
                this.traceTopicName = traceTopicName;
            }
            else
            {
                this.traceTopicName = TopicValidator.RMQ_SYS_TRACE_TOPIC;
            }
            //ok
            //this.traceExecutor = new ThreadPoolExecutor(//
            //        10, //
            //        20, //
            //        1000 * 60, //
            //        TimeUnit.MILLISECONDS, //
            //        this.appenderQueue, //
            //        new ThreadFactoryImpl("MQTraceSendThread_"));
            //traceExecutor = new ExecutorService(10, 20, queueSize);
            traceExecutor = new ExecutorService(20, queueSize);
            traceProducer = getAndCreateTraceProducer(rpcHook);
        }

        public AccessChannel getAccessChannel()
        {
            return accessChannel;
        }

        public void setAccessChannel(AccessChannel accessChannel)
        {
            this.accessChannel = accessChannel;
        }

        public string getTraceTopicName()
        {
            return traceTopicName;
        }

        public void setTraceTopicName(String traceTopicName)
        {
            this.traceTopicName = traceTopicName;
        }

        public DefaultMQProducer getTraceProducer()
        {
            return traceProducer;
        }

        public DefaultMQProducerImpl getHostProducer()
        {
            return hostProducer;
        }

        public void setHostProducer(DefaultMQProducerImpl hostProducer)
        {
            this.hostProducer = hostProducer;
        }

        public DefaultMQPushConsumerImpl getHostConsumer()
        {
            return hostConsumer;
        }

        public void setHostConsumer(DefaultMQPushConsumerImpl hostConsumer)
        {
            this.hostConsumer = hostConsumer;
        }

        public void start(String nameSrvAddr, AccessChannel accessChannel)
        {
            if (isStarted.CompareAndSet(false, true))
            {
                traceProducer.setNamesrvAddr(nameSrvAddr);
                traceProducer.setInstanceName(TraceConstants.TRACE_INSTANCE_NAME + "_" + nameSrvAddr);
                traceProducer.start();
            }
            this.accessChannel = accessChannel;
            //this.worker = new Thread(new AsyncRunnable(), "MQ-AsyncTraceDispatcher-Thread-" + dispatcherId);
            this.worker = new Thread(new ThreadStart(new AsyncRunnable(this).run));
            worker.Name = "MQ-AsyncTraceDispatcher-Thread-" + dispatcherId;
            this.worker.IsBackground = true;  //如果所有前台线程都已经终止，不会等待此线程完成
            //this.worker.setDaemon(true); //???
            this.worker.Start();
            this.registerShutDownHook();
        }

        private DefaultMQProducer getAndCreateTraceProducer(RPCHook rpcHook)
        {
            DefaultMQProducer traceProducerInstance = this.traceProducer;
            if (traceProducerInstance == null)
            {
                traceProducerInstance = new DefaultMQProducer(rpcHook);
                traceProducerInstance.setProducerGroup(genGroupNameForTrace());
                traceProducerInstance.setSendMsgTimeout(5000);
                traceProducerInstance.setVipChannelEnabled(false);
                // The max size of message is 128K
                traceProducerInstance.setMaxMessageSize(maxMsgSize - 10 * 1000);
            }
            return traceProducerInstance;
        }

        private string genGroupNameForTrace()
        {
            return TraceConstants.GROUP_NAME_PREFIX + "-" + this.group + "-" + this.type + "-" + COUNTER.incrementAndGet();
        }

        //@Override
        public bool append(object ctx)
        {
            //bool result = traceContextQueue.offer((TraceContext)ctx);
            //if (!result)
            //{
            //    log.Info("buffer full" + discardCount.incrementAndGet() + " ,context is " + ctx);
            //}
            //return result;
            if (traceContextQueue.Count >= traceQueueSize)  //???
            {
                log.Info("buffer full" + discardCount.IncrementAndGet() + " ,context is " + ctx);
                return false;
            }
            else
            {
                traceContextQueue.Add((TraceContext)ctx);
                return true;
            }
        }

        //@Override
        public void flush()
        {
            // The maximum waiting time for refresh,avoid being written all the time, resulting in failure to return.
            long end = Sys.currentTimeMillis() + 500;
            while (Sys.currentTimeMillis() <= end)
            {
                lock (traceContextQueue)
                {
                    //if (traceContextQueue.Count == 0 && appenderQueue.Count == 0)
                    if (traceContextQueue.Count == 0 && traceExecutor.GetWaitingCount() == 0)
                    {
                        break;
                    }
                }
                try
                {
                    Thread.Sleep(1);
                }
                catch (ThreadInterruptedException e)
                {
                    break;
                }
            }
            //log.Info("------end trace send " + traceContextQueue.Count + "   " + appenderQueue.Count);
            log.Info("------end trace send " + traceContextQueue.Count + "   " + traceExecutor.GetWaitingCount());
        }

        //@Override
        public void shutdown()
        {
            this.stopped = true;
            flush();
            this.traceExecutor.Shutdown();
            if (isStarted.get())
            {
                traceProducer.shutdown();
            }
            this.removeShutdownHook();
        }

        class RegisterShutDownHookRunnable : Runnable
        {
            public volatile bool hasShutdown = false;
        }
        public void registerShutDownHook()
        {
            if (shutDownHook == null)
            {
                //            shutDownHook = new Thread(new RegisterShutDownHookRunnable()
                //            {
                //                        hasShutdown = false,

                //                        //public void run()
                //                        Run=()=>
                //    {
                //        lock(this) {
                //            if (!this.hasShutdown)
                //            {
                //                flush();
                //            }
                //        }
                //    }
                //}, "ShutdownHookMQTrace");

                var hook = new RegisterShutDownHookRunnable();
                hook.hasShutdown = false;
                hook.Run = () =>
                {
                    lock (this)
                    {
                        if (!hook.hasShutdown)
                        {
                            flush();
                        }
                    }
                };
                shutDownHook = new Thread(new ThreadStart(hook.Run));
                shutDownHook.Name = "ShutdownHookMQTrace";
                Runtime.getRuntime().addShutdownHook(shutDownHook);
            }
        }

        public void removeShutdownHook()
        {
            if (shutDownHook != null)
            {
                try
                {
                    Runtime.getRuntime().removeShutdownHook(shutDownHook);
                }
                catch (InvalidOperationException e)
                {
                    // ignore - VM is already shutting down
                }
            }
        }

        class AsyncRunnable : Runnable
        {
            private AsyncTraceDispatcher owner;
            public AsyncRunnable(AsyncTraceDispatcher owner)
            {
                this.owner = owner;
            }

            private bool stopped;

            public void run()
            {
                while (!stopped)
                {
                    List<TraceContext> contexts = new List<TraceContext>(owner.batchSize);
                    lock (owner.traceContextQueue)
                    {
                        for (int i = 0; i < owner.batchSize; i++)
                        {
                            TraceContext context = null;
                            try
                            {
                                //get trace data element from blocking Queue - traceContextQueue
                                context = owner.traceContextQueue.Poll(5, TimeUnit.MILLISECONDS); //???阻塞5毫秒？
                            }
                            catch (Exception e)
                            {
                            }
                            if (context != null)
                            {
                                contexts.Add(context);
                            }
                            else
                            {
                                break;
                            }
                        }
                        if (contexts.Count > 0)
                        {
                            AsyncAppenderRequest request = new AsyncAppenderRequest(contexts);
                            owner.traceExecutor.Submit(request);
                        }
                        else if (owner.stopped)
                        {
                            stopped = true;
                        }
                    }
                }

            }
        }

        class AsyncAppenderRequest : IRunnable
        {
            private AsyncTraceDispatcher owner;
            public AsyncAppenderRequest(AsyncTraceDispatcher owner)
            {
                this.owner = owner;
            }

            List<TraceContext> contextList;

            public AsyncAppenderRequest(List<TraceContext> contextList)
            {
                if (contextList != null)
                {
                    this.contextList = contextList;
                }
                else
                {
                    this.contextList = new ArrayList<TraceContext>(1);
                }
            }

            public void run()
            {
                sendTraceData(contextList);
            }

            public void sendTraceData(List<TraceContext> contextList)
            {
                var transBeanMap = new HashMap<String, List<TraceTransferBean>>();
                foreach (TraceContext context in contextList)
                {
                    if (context.getTraceBeans().IsEmpty())
                    {
                        continue;
                    }
                    // Topic value corresponding to original message entity content
                    string topic = context.getTraceBeans().Get(0).getTopic();
                    string regionId = context.getRegionId();
                    // Use  original message entity's topic as key
                    string key = topic;
                    if (!Str.isBlank(regionId))
                    {
                        key = key + TraceConstants.CONTENT_SPLITOR + regionId;
                    }
                    List<TraceTransferBean> transBeanList = transBeanMap.Get(key);
                    if (transBeanList == null)
                    {
                        transBeanList = new ArrayList<TraceTransferBean>();
                        transBeanMap.Put(key, transBeanList);
                    }
                    TraceTransferBean traceData = TraceDataEncoder.encoderFromContextBean(context);
                    transBeanList.Add(traceData);
                }
                foreach (var entry in transBeanMap)
                {
                    String[] key = entry.Key.Split(Str.valueOf(TraceConstants.CONTENT_SPLITOR));
                    string dataTopic = entry.Key;
                    string regionId = null;
                    if (key.Length > 1)
                    {
                        dataTopic = key[0];
                        regionId = key[1];
                    }
                    flushData(entry.Value, dataTopic, regionId);
                }
            }

            /**
             * Batch sending data actually
             */
            private void flushData(List<TraceTransferBean> transBeanList, string dataTopic, string regionId)
            {
                if (transBeanList.Count == 0)
                {
                    return;
                }
                // Temporary buffer
                StringBuilder buffer = new StringBuilder(1024);
                int count = 0;
                HashSet<String> keySet = new HashSet<String>();

                foreach (TraceTransferBean bean in transBeanList)
                {
                    // Keyset of message trace includes msgId of or original message
                    keySet.AddAll(bean.getTransKey());
                    buffer.Append(bean.getTransData());
                    count++;
                    // Ensure that the size of the package should not exceed the upper limit.
                    if (buffer.Length >= owner.traceProducer.getMaxMessageSize())
                    {
                        sendTraceDataByMQ(keySet, buffer.ToString(), dataTopic, regionId);
                        // Clear temporary buffer after finishing
                        //buffer.delete(0, buffer.Length);
                        buffer.Remove(0, buffer.Length); //???
                                                         //keySet.clear();
                        keySet.Clear();
                        count = 0;
                    }
                }
                if (count > 0)
                {
                    sendTraceDataByMQ(keySet, buffer.ToString(), dataTopic, regionId);
                }
                transBeanList.Clear();
            }

            /**
             * Send message trace data
             *
             * @param keySet the keyset in this batch(including msgId in original message not offsetMsgId)
             * @param data   the message trace data in this batch
             */
            private void sendTraceDataByMQ(HashSet<String> keySet, string data, string dataTopic, string regionId)
            {
                string traceTopic = owner.traceTopicName;
                if (AccessChannel.CLOUD == owner.accessChannel)
                {
                    traceTopic = TraceConstants.TRACE_TOPIC_PREFIX + regionId;
                }
                Message message = new Message(traceTopic, data.getBytes()); //???charset
                                                                            // Keyset of message trace includes msgId of or original message
                message.setKeys(keySet);
                try
                {
                    HashSet<String> traceBrokerSet = tryGetMessageQueueBrokerSet(owner.traceProducer.getDefaultMQProducerImpl(), traceTopic);
                    //SendCallback callback = new SendCallback();
                    SendCallback callback = new SendCallback()
                    {
                        //@Override
                        //public void onSuccess(SendResult sendResult)
                        OnSuccess = (sendResult) =>
                        {

                        },

                        //public void onException(Throwable e)
                        OnException = (e) =>
                        {
                            log.Error("send trace data failed, the traceData is {}", data, e);
                        }
                    };
                    if (traceBrokerSet.Count <= 0)
                    {
                        // No cross set
                        owner.traceProducer.send(message, callback, 5000);
                    }
                    else
                    {
                        owner.traceProducer.send(message, new MessageQueueSelector()
                        {
                            //@Override
                            //public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg)
                            Select = (mqs, msg, arg) =>
                            {
                                HashSet<String> brokerSet = (HashSet<String>)arg;
                                List<MessageQueue> filterMqs = new ArrayList<MessageQueue>();
                                foreach (MessageQueue queue in mqs)
                                {
                                    if (brokerSet.Contains(queue.getBrokerName()))
                                    {
                                        filterMqs.Add(queue);
                                    }
                                }
                                int index = owner.sendWhichQueue.incrementAndGet();
                                int pos = Math.Abs(index) % filterMqs.Count;
                                if (pos < 0)
                                {
                                    pos = 0;
                                }
                                return filterMqs.Get(pos);
                            }
                        }, traceBrokerSet, callback);
                    }

                }
                catch (Exception e)
                {
                    log.Error("send trace data failed, the traceData is {}", data, e);
                }
            }

            private HashSet<String> tryGetMessageQueueBrokerSet(DefaultMQProducerImpl producer, string topic)
            {
                HashSet<String> brokerSet = new HashSet<String>();
                TopicPublishInfo topicPublishInfo = producer.getTopicPublishInfoTable().Get(topic);
                if (null == topicPublishInfo || !topicPublishInfo.ok())
                {
                    producer.getTopicPublishInfoTable().PutIfAbsent(topic, new TopicPublishInfo());
                    producer.getmQClientFactory().updateTopicRouteInfoFromNameServer(topic);
                    topicPublishInfo = producer.getTopicPublishInfoTable().Get(topic);
                }
                if (topicPublishInfo.isHaveTopicRouterInfo() || topicPublishInfo.ok())
                {
                    foreach (MessageQueue queue in topicPublishInfo.getMessageQueueList())
                    {
                        brokerSet.Add(queue.getBrokerName());
                    }
                }
                return brokerSet;
            }
        }
    }
}
