using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;

namespace RocketMQ.Client
{
    public class DefaultMQProducerImpl : MQProducerInner
    {
        //private final InternalLogger log = ClientLogger.getLog();
        static NLog.Logger log = NLog.LogManager.GetCurrentClassLogger();
        private readonly Random random = new Random();
        private readonly DefaultMQProducer defaultMQProducer;
        private readonly ConcurrentDictionary<String/* topic */, TopicPublishInfo> topicPublishInfoTable =
            new ConcurrentDictionary<String, TopicPublishInfo>();
        private readonly List<SendMessageHook> sendMessageHookList = new List<SendMessageHook>();
        private readonly List<EndTransactionHook> endTransactionHookList = new List<EndTransactionHook>();
        private readonly RPCHook rpcHook;
        //private readonly BlockingQueue<Runnable> asyncSenderThreadPoolQueue;
        private readonly ExecutorService defaultAsyncSenderExecutor;
        //protected BlockingQueue<Runnable> checkRequestQueue;
        protected ExecutorService checkExecutor;
        private ServiceState serviceState = ServiceState.CREATE_JUST;
        private MQClientInstance mQClientFactory;
        private List<CheckForbiddenHook> checkForbiddenHookList = new List<CheckForbiddenHook>();
        private int zipCompressLevel = int.Parse(Sys.getProperty(MixAll.MESSAGE_COMPRESS_LEVEL, "5"));
        private MQFaultStrategy mqFaultStrategy = new MQFaultStrategy();
        private ExecutorService asyncSenderExecutor;

        public DefaultMQProducerImpl(DefaultMQProducer defaultMQProducer) : this(defaultMQProducer, null)
        {

        }

        public DefaultMQProducerImpl(DefaultMQProducer defaultMQProducer, RPCHook rpcHook)
        {
            this.defaultMQProducer = defaultMQProducer;
            this.rpcHook = rpcHook;

            //ok
            //this.asyncSenderThreadPoolQueue = new LinkedBlockingQueue<Runnable>(50000);
            //        this.defaultAsyncSenderExecutor = new ThreadPoolExecutor(
            //            Runtime.getRuntime().availableProcessors(),
            //            Runtime.getRuntime().availableProcessors(),
            //            1000 * 60,
            //            TimeUnit.MILLISECONDS,
            //            this.asyncSenderThreadPoolQueue,
            //            new ThreadFactory()
            //            {
            //                private AtomicInteger threadIndex = new AtomicInteger(0);

            //    @Override
            //            public Thread newThread(Runnable r)
            //    {
            //        return new Thread(r, "AsyncSenderExecutor_" + this.threadIndex.incrementAndGet());
            //    }
            //});
            asyncSenderExecutor = new ExecutorService(UtilAll.availableProcessors(), 50000);
        }

        public void registerCheckForbiddenHook(CheckForbiddenHook checkForbiddenHook)
        {
            this.checkForbiddenHookList.Add(checkForbiddenHook);
            log.Info("register a new checkForbiddenHook. hookName={}, allHookSize={}", checkForbiddenHook.hookName(),
                checkForbiddenHookList.Count);
        }

        public void initTransactionEnv()
        {
            TransactionMQProducer producer = (TransactionMQProducer)this.defaultMQProducer;
            if (producer.getExecutorService() != null)
            {
                this.checkExecutor = producer.getExecutorService();
            }
            else
            {
                //ok
                //this.checkRequestQueue = new LinkedBlockingQueue<Runnable>(producer.getCheckRequestHoldMax());
                //this.checkExecutor = new ThreadPoolExecutor(
                //    producer.getCheckThreadPoolMinSize(),
                //    producer.getCheckThreadPoolMaxSize(),
                //    1000 * 60,
                //    TimeUnit.MILLISECONDS,
                //    this.checkRequestQueue);
                checkExecutor = new ExecutorService(producer.getCheckThreadPoolMinSize(), producer.getCheckRequestHoldMax());
            }
        }

        public void destroyTransactionEnv()
        {
            if (this.checkExecutor != null)
            {
                this.checkExecutor.Shutdown();
            }
        }

        public void registerSendMessageHook(SendMessageHook hook)
        {
            this.sendMessageHookList.Add(hook);
            log.Info("register sendMessage Hook, {}", hook.hookName());
        }

        public void registerEndTransactionHook(EndTransactionHook hook)
        {
            this.endTransactionHookList.Add(hook);
            log.Info("register endTransaction Hook, {}", hook.hookName());
        }

        ///<exception cref="MQClientException"/>
        public void start()
        {
            this.start(true);
        }

        ///<exception cref="MQClientException"/>
        public void start(bool startFactory)
        {
            switch (this.serviceState)
            {
                case ServiceState.CREATE_JUST:
                    this.serviceState = ServiceState.START_FAILED;

                    this.checkConfig();

                    if (!this.defaultMQProducer.getProducerGroup().Equals(MixAll.CLIENT_INNER_PRODUCER_GROUP))
                    {
                        this.defaultMQProducer.changeInstanceNameToPID();
                    }

                    this.mQClientFactory = MQClientManager.getInstance().getOrCreateMQClientInstance(this.defaultMQProducer, rpcHook);

                    bool registerOK = mQClientFactory.registerProducer(this.defaultMQProducer.getProducerGroup(), this);
                    if (!registerOK)
                    {
                        this.serviceState = ServiceState.CREATE_JUST;
                        throw new MQClientException("The producer group[" + this.defaultMQProducer.getProducerGroup()
                            + "] has been created before, specify another name please." + FAQUrl.suggestTodo(FAQUrl.GROUP_NAME_DUPLICATE_URL),
                            null);
                    }

                    //this.topicPublishInfoTable.put(this.defaultMQProducer.getCreateTopicKey(), new TopicPublishInfo());
                    this.topicPublishInfoTable[this.defaultMQProducer.getCreateTopicKey()] = new TopicPublishInfo();

                    if (startFactory)
                    {
                        mQClientFactory.start();
                    }

                    log.Info("the producer [{}] start OK. sendMessageWithVIPChannel={}", this.defaultMQProducer.getProducerGroup(),
                        this.defaultMQProducer.isSendMessageWithVIPChannel());
                    this.serviceState = ServiceState.RUNNING;
                    break;
                case ServiceState.RUNNING:
                case ServiceState.START_FAILED:
                case ServiceState.SHUTDOWN_ALREADY:
                    throw new MQClientException("The producer service state not OK, maybe started once, "
                        + this.serviceState
                        + FAQUrl.suggestTodo(FAQUrl.CLIENT_SERVICE_NOT_OK),
                        null);
                default:
                    break;
            }

            this.mQClientFactory.sendHeartbeatToAllBrokerWithLock();

            RequestFutureHolder.getInstance().startScheduledTask(this);

        }

        private void checkConfig()
        {
            Validators.checkGroup(this.defaultMQProducer.getProducerGroup());

            if (null == this.defaultMQProducer.getProducerGroup())
            {
                throw new MQClientException("producerGroup is null", null);
            }

            if (this.defaultMQProducer.getProducerGroup().Equals(MixAll.DEFAULT_PRODUCER_GROUP))
            {
                throw new MQClientException("producerGroup can not equal " + MixAll.DEFAULT_PRODUCER_GROUP + ", please specify another one.",
                    null);
            }
        }

        public void shutdown()
        {
            this.shutdown(true);
        }

        public void shutdown(bool shutdownFactory)
        {
            switch (this.serviceState)
            {
                case ServiceState.CREATE_JUST:
                    break;
                case ServiceState.RUNNING:
                    this.mQClientFactory.unregisterProducer(this.defaultMQProducer.getProducerGroup());
                    this.defaultAsyncSenderExecutor.Shutdown();
                    if (shutdownFactory)
                    {
                        this.mQClientFactory.shutdown();
                    }
                    RequestFutureHolder.getInstance().shutdown(this);
                    log.Info("the producer [{}] shutdown OK", this.defaultMQProducer.getProducerGroup());
                    this.serviceState = ServiceState.SHUTDOWN_ALREADY;
                    break;
                case ServiceState.SHUTDOWN_ALREADY:
                    break;
                default:
                    break;
            }
        }

        public HashSet<String> getPublishTopicList()
        {
            //ok
            //HashSet<String> topicList = new HashSet<String>();
            //foreach (String key in this.topicPublishInfoTable.keySet())
            //{
            //    topicList.Add(key);
            //}
            //return topicList;
            return topicPublishInfoTable.Keys.ToHashSet();
        }

        public bool isPublishTopicNeedUpdate(String topic)
        {
            //TopicPublishInfo prev = this.topicPublishInfoTable.get(topic);
            topicPublishInfoTable.TryGetValue(topic, out TopicPublishInfo prev);
            return null == prev || !prev.ok();
        }

        /**
         * @deprecated This method will be removed in the version 5.0.0 and {@link DefaultMQProducerImpl#getCheckListener} is recommended.
         */
        //@Override
        [Obsolete]//@Deprecated
        public TransactionCheckListener checkListener()
        {
            if (this.defaultMQProducer is TransactionMQProducer)
            {
                TransactionMQProducer producer = (TransactionMQProducer)defaultMQProducer;
                return producer.getTransactionCheckListener();
            }

            return null;
        }

        public TransactionListener getCheckListener()
        {
            if (this.defaultMQProducer is TransactionMQProducer)
            {
                TransactionMQProducer producer = (TransactionMQProducer)defaultMQProducer;
                return producer.getTransactionListener();
            }
            return null;
        }


        class CheckTransactionStateImpl : Runnable { public Action<LocalTransactionState, string, Exception> processTransactionState { get; set; } }

        public void checkTransactionState(String addr, MessageExt msg, CheckTransactionStateRequestHeader header)
        {

            string brokerAddr = addr;
            MessageExt message = msg;
            CheckTransactionStateRequestHeader checkRequestHeader = header;
            string group = defaultMQProducer.getProducerGroup();

            //Runnable request = new Runnable()
            CheckTransactionStateImpl request = new CheckTransactionStateImpl();
            //@Override
            //public void run()
            request.Run = () =>
            {
                TransactionCheckListener transactionCheckListener = /*DefaultMQProducerImpl.this.*/checkListener();
                TransactionListener transactionListener = getCheckListener();
                if (transactionCheckListener != null || transactionListener != null)
                {
                    LocalTransactionState localTransactionState = LocalTransactionState.UNKNOW;
                    Exception exception = null;
                    try
                    {
                        if (transactionCheckListener != null)
                        {
                            localTransactionState = transactionCheckListener.checkLocalTransactionState(message);
                        }
                        else if (transactionListener != null)
                        {
                            log.Debug("Used new check API in transaction message");
                            localTransactionState = transactionListener.checkLocalTransaction(message);
                        }
                        else
                        {
                            log.Warn("CheckTransactionState, pick transactionListener by group[{}] failed", group);
                        }
                    }
                    catch (Exception e)
                    {
                        log.Error("Broker call checkTransactionState, but checkLocalTransactionState exception", e.ToString());
                        exception = e;
                    }

                    /*this.*/
                    request.processTransactionState(localTransactionState, group, exception);
                }
                else
                {
                    log.Warn("CheckTransactionState, pick transactionCheckListener by group[{}] failed", group);
                }
            };

            //private void processTransactionState(
            //    LocalTransactionState localTransactionState,
            //    String producerGroup,
            //    Exception exception)
            request.processTransactionState = (localTransactionState, producerGroup, exception) =>
            {
                EndTransactionRequestHeader thisHeader = new EndTransactionRequestHeader();
                thisHeader.commitLogOffset = checkRequestHeader.commitLogOffset;
                thisHeader.producerGroup = producerGroup;
                thisHeader.tranStateTableOffset = checkRequestHeader.tranStateTableOffset;
                thisHeader.fromTransactionCheck = true;

                String uniqueKey = message.getProperties().get(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX);
                if (uniqueKey == null)
                {
                    uniqueKey = message.getMsgId();
                }
                thisHeader.msgId = uniqueKey;
                thisHeader.transactionId = checkRequestHeader.transactionId;
                switch (localTransactionState)
                {
                    case LocalTransactionState.COMMIT_MESSAGE:
                        thisHeader.commitOrRollback = MessageSysFlag.TRANSACTION_COMMIT_TYPE;
                        break;
                    case LocalTransactionState.ROLLBACK_MESSAGE:
                        thisHeader.commitOrRollback = (MessageSysFlag.TRANSACTION_ROLLBACK_TYPE);
                        log.Warn("when broker check, client rollback this transaction, {}", thisHeader);
                        break;
                    case LocalTransactionState.UNKNOW:
                        thisHeader.commitOrRollback = (MessageSysFlag.TRANSACTION_NOT_TYPE);
                        log.Warn("when broker check, client does not know this transaction state, {}", thisHeader);
                        break;
                    default:
                        break;
                }

                String remark = null;
                if (exception != null)
                {
                    remark = "checkLocalTransactionState Exception: " + RemotingHelper.exceptionSimpleDesc(exception);
                }
                doExecuteEndTransactionHook(msg, uniqueKey, brokerAddr, localTransactionState, true);

                try
                {
                    /*DefaultMQProducerImpl.this.*/
                    mQClientFactory.getMQClientAPIImpl().endTransactionOneway(brokerAddr, thisHeader, remark, 3000);
                }
                catch (Exception e)
                {
                    log.Error("endTransactionOneway exception", e.ToString());
                }
            };
            this.checkExecutor.Submit(request);
        }

        public void updateTopicPublishInfo(String topic, TopicPublishInfo info)
        {
            if (info != null && topic != null)
            {
                TopicPublishInfo prev = this.topicPublishInfoTable.put(topic, info);
                if (prev != null)
                {
                    log.Info("updateTopicPublishInfo prev is not null, " + prev.ToString());
                }
            }
        }

        //@Override
        public bool isUnitMode()
        {
            return this.defaultMQProducer.isUnitMode();
        }

        ///<exception cref="MQClientException"/>
        public void createTopic(String key, String newTopic, int queueNum)
        {
            createTopic(key, newTopic, queueNum, 0);
        }

        ///<exception cref="MQClientException"/>
        public void createTopic(String key, String newTopic, int queueNum, int topicSysFlag)
        {
            this.makeSureStateOK();
            Validators.checkTopic(newTopic);
            Validators.isSystemTopic(newTopic);

            this.mQClientFactory.getMQAdminImpl().createTopic(key, newTopic, queueNum, topicSysFlag);
        }

        ///<exception cref="MQClientException"/>
        private void makeSureStateOK()
        {
            if (this.serviceState != ServiceState.RUNNING)
            {
                throw new MQClientException("The producer service state not OK, "
                    + this.serviceState
                    + FAQUrl.suggestTodo(FAQUrl.CLIENT_SERVICE_NOT_OK),
                    null);
            }
        }

        ///<exception cref="MQClientException"/>
        public List<MessageQueue> fetchPublishMessageQueues(String topic)
        {
            this.makeSureStateOK();
            return this.mQClientFactory.getMQAdminImpl().fetchPublishMessageQueues(topic);
        }


        ///<exception cref="MQClientException"/>
        public long searchOffset(MessageQueue mq, long timestamp)
        {
            this.makeSureStateOK();
            return this.mQClientFactory.getMQAdminImpl().searchOffset(mq, timestamp);
        }

        ///<exception cref="MQClientException"/>
        public long maxOffset(MessageQueue mq)
        {
            this.makeSureStateOK();
            return this.mQClientFactory.getMQAdminImpl().maxOffset(mq);
        }


        ///<exception cref="MQClientException"/>
        public long minOffset(MessageQueue mq)
        {
            this.makeSureStateOK();
            return this.mQClientFactory.getMQAdminImpl().minOffset(mq);
        }

        ///<exception cref="MQClientException"/>
        public long earliestMsgStoreTime(MessageQueue mq)
        {
            this.makeSureStateOK();
            return this.mQClientFactory.getMQAdminImpl().earliestMsgStoreTime(mq);
        }

        ///<exception cref="MQClientException"/>
        ///<exception cref="RemotingException"/>
        ///<exception cref="InterruptedException"/>
        ///<exception cref="MQBrokerException"/>
        public MessageExt viewMessage(
            String msgId)
        {
            this.makeSureStateOK();

            return this.mQClientFactory.getMQAdminImpl().viewMessage(msgId);
        }

        ///<exception cref="MQClientException"/>
        ///<exception cref="InterruptedException"/>
        public QueryResult queryMessage(String topic, String key, int maxNum, long begin, long end)
        {
            this.makeSureStateOK();
            return this.mQClientFactory.getMQAdminImpl().queryMessage(topic, key, maxNum, begin, end);
        }

        ///<exception cref="MQClientException"/>
        ///<exception cref="InterruptedException"/>
        public MessageExt queryMessageByUniqKey(String topic, String uniqKey)
        {
            this.makeSureStateOK();
            return this.mQClientFactory.getMQAdminImpl().queryMessageByUniqKey(topic, uniqKey);
        }

        /**
         * DEFAULT ASYNC -------------------------------------------------------
         */
        ///<exception cref="MQClientException"/>
        ///<exception cref="RemotingException"/>
        ///<exception cref="InterruptedException"/>
        public void send(Message msg,
            SendCallback sendCallback)
        {
            send(msg, sendCallback, this.defaultMQProducer.getSendMsgTimeout());
        }

        /**
         * @deprecated
         * It will be removed at 4.4.0 cause for exception handling and the wrong Semantics of timeout. A new one will be
         * provided in next version
         *
         * @param msg
         * @param sendCallback
         * @param timeout      the <code>sendCallback</code> will be invoked at most time
         * @throws RejectedExecutionException
         */
        [Obsolete]//@Deprecated
        ///<exception cref="MQClientException"/>
        ///<exception cref="RemotingException"/>
        ///<exception cref="InterruptedException"/>
        public void send(Message msg, SendCallback sendCallback, long timeout)
        {
            long beginStartTime = Sys.currentTimeMillis();
            ExecutorService executor = this.getAsyncSenderExecutor();
            try
            {
                executor.Submit(new Runnable()
                {
                    //public void run()
                    Run = () =>
                    {
                        long costTime = Sys.currentTimeMillis() - beginStartTime;
                        if (timeout > costTime)
                        {
                            try
                            {
                                sendDefaultImpl(msg, CommunicationMode.ASYNC, sendCallback, timeout - costTime);
                            }
                            catch (Exception e)
                            {
                                sendCallback.OnException(e);
                            }
                        }
                        else
                        {
                            sendCallback.OnException(
                                new RemotingTooMuchRequestException("DEFAULT ASYNC send call timeout"));
                        }
                    }

                });
            }
            catch (Exception e)
            {
                throw new MQClientException("executor rejected ", e);
            }

        }

        public MessageQueue selectOneMessageQueue(TopicPublishInfo tpInfo, String lastBrokerName)
        {
            return this.mqFaultStrategy.selectOneMessageQueue(tpInfo, lastBrokerName);
        }

        public void updateFaultItem(String brokerName, long currentLatency, bool isolation)
        {
            this.mqFaultStrategy.updateFaultItem(brokerName, currentLatency, isolation);
        }

        ///<exception cref="MQClientException"/>
        private void validateNameServerSetting()
        {
            List<String> nsList = this.getmQClientFactory().getMQClientAPIImpl().getNameServerAddressList();
            if (null == nsList || nsList.isEmpty())
            {
                throw new MQClientException(
                    "No name server address, please set it." + FAQUrl.suggestTodo(FAQUrl.NAME_SERVER_ADDR_NOT_EXIST_URL), null).setResponseCode(ClientErrorCode.NO_NAME_SERVER_EXCEPTION);
            }

        }


        ///<exception cref="MQClientException"/>
        ///<exception cref="RemotingException"/>
        ///<exception cref="InterruptedException"/>
        ///<exception cref="MQBrokerException"/>
        private SendResult sendDefaultImpl(Message msg,CommunicationMode communicationMode,SendCallback sendCallback,long timeout)
        {
            this.makeSureStateOK();
            Validators.checkMessage(msg, this.defaultMQProducer);
            long invokeID = random.nextLong(); 
            long beginTimestampFirst = Sys.currentTimeMillis();
            long beginTimestampPrev = beginTimestampFirst;
            long endTimestamp = beginTimestampFirst;
            TopicPublishInfo topicPublishInfo = this.tryToFindTopicPublishInfo(msg.getTopic());
            if (topicPublishInfo != null && topicPublishInfo.ok())
            {
                bool callTimeout = false;
                MessageQueue mq = null;
                Exception exception = null;
                SendResult sendResult = null;
                int timesTotal = communicationMode == CommunicationMode.SYNC ? 1 + this.defaultMQProducer.getRetryTimesWhenSendFailed() : 1;
                int times = 0;
                String[] brokersSent = new String[timesTotal];
                for (; times < timesTotal; times++)
                {
                    String lastBrokerName = null == mq ? null : mq.getBrokerName();
                    MessageQueue mqSelected = this.selectOneMessageQueue(topicPublishInfo, lastBrokerName);
                    if (mqSelected != null)
                    {
                        mq = mqSelected;
                        brokersSent[times] = mq.getBrokerName();
                        try
                        {
                            beginTimestampPrev = Sys.currentTimeMillis();
                            if (times > 0)
                            {
                                //Reset topic with namespace during resend.
                                msg.setTopic(this.defaultMQProducer.withNamespace(msg.getTopic()));
                            }
                            long costTime = beginTimestampPrev - beginTimestampFirst;
                            if (timeout < costTime)
                            {
                                callTimeout = true;
                                break;
                            }

                            sendResult = this.sendKernelImpl(msg, mq, communicationMode, sendCallback, topicPublishInfo, timeout - costTime);
                            endTimestamp = Sys.currentTimeMillis();
                            this.updateFaultItem(mq.getBrokerName(), endTimestamp - beginTimestampPrev, false);
                            switch (communicationMode)
                            {
                                case CommunicationMode.ASYNC:
                                    return null;
                                case CommunicationMode.ONEWAY:
                                    return null;
                                case CommunicationMode.SYNC:
                                    if (sendResult.getSendStatus() != SendStatus.SEND_OK)
                                    {
                                        if (this.defaultMQProducer.isRetryAnotherBrokerWhenNotStoreOK())
                                        {
                                            continue;
                                        }
                                    }

                                    return sendResult;
                                default:
                                    break;
                            }
                        }
                        catch (RemotingException e)
                        {
                            endTimestamp = Sys.currentTimeMillis();
                            this.updateFaultItem(mq.getBrokerName(), endTimestamp - beginTimestampPrev, true);
                            log.Warn(String.Format("sendKernelImpl exception, resend at once, InvokeID: %s, RT: %sms, Broker: %s", invokeID, endTimestamp - beginTimestampPrev, mq), e);
                            log.Warn(msg.ToString());
                            exception = e;
                            continue;
                        }
                        catch (MQClientException e)
                        {
                            endTimestamp = Sys.currentTimeMillis();
                            this.updateFaultItem(mq.getBrokerName(), endTimestamp - beginTimestampPrev, true);
                            log.Warn(String.Format("sendKernelImpl exception, resend at once, InvokeID: %s, RT: %sms, Broker: %s", invokeID, endTimestamp - beginTimestampPrev, mq), e);
                            log.Warn(msg.ToString());
                            exception = e;
                            continue;
                        }
                        catch (MQBrokerException e)
                        {
                            endTimestamp = Sys.currentTimeMillis();
                            this.updateFaultItem(mq.getBrokerName(), endTimestamp - beginTimestampPrev, true);
                            log.Warn(String.Format("sendKernelImpl exception, resend at once, InvokeID: %s, RT: %sms, Broker: %s", invokeID, endTimestamp - beginTimestampPrev, mq), e);
                            log.Warn(msg.ToString());
                            exception = e;
                            if (this.defaultMQProducer.getRetryResponseCodes().Contains(e.getResponseCode()))
                            {
                                continue;
                            }
                            else
                            {
                                if (sendResult != null)
                                {
                                    return sendResult;
                                }

                                throw e;
                            }
                        }
                        catch (ThreadInterruptedException e)
                        {
                            endTimestamp = Sys.currentTimeMillis();
                            this.updateFaultItem(mq.getBrokerName(), endTimestamp - beginTimestampPrev, false);
                            log.Warn(String.Format("sendKernelImpl exception, throw exception, InvokeID: %s, RT: %sms, Broker: %s", invokeID, endTimestamp - beginTimestampPrev, mq), e);
                            log.Warn(msg.ToString());

                            log.Warn("sendKernelImpl exception", e);
                            log.Warn(msg.ToString());
                            throw e;
                        }
                    }
                    else
                    {
                        break;
                    }
                }

                if (sendResult != null)
                {
                    return sendResult;
                }

                String info = String.Format("Send [%d] times, still failed, cost [%d]ms, Topic: %s, BrokersSent: %s",
                    times,
                    Sys.currentTimeMillis() - beginTimestampFirst,
                    msg.getTopic(),
                    UtilAll.Array2String(brokersSent)); //Arrays.toString(brokersSent)

                info += FAQUrl.suggestTodo(FAQUrl.SEND_MSG_FAILED);

                MQClientException mqClientException = new MQClientException(info, exception);
                if (callTimeout)
                {
                    throw new RemotingTooMuchRequestException("sendDefaultImpl call timeout");
                }

                if (exception is MQBrokerException)
                {
                    mqClientException.setResponseCode(((MQBrokerException)exception).getResponseCode());
                }
                else if (exception is RemotingConnectException)
                {
                    mqClientException.setResponseCode(ClientErrorCode.CONNECT_BROKER_EXCEPTION);
                }
                else if (exception is RemotingTimeoutException)
                {
                    mqClientException.setResponseCode(ClientErrorCode.ACCESS_BROKER_TIMEOUT);
                }
                else if (exception is MQClientException)
                {
                    mqClientException.setResponseCode(ClientErrorCode.BROKER_NOT_EXIST_EXCEPTION);
                }

                throw mqClientException;
            }

            validateNameServerSetting();

            throw new MQClientException("No route info of this topic: " + msg.getTopic() + FAQUrl.suggestTodo(FAQUrl.NO_TOPIC_ROUTE_INFO),
                null).setResponseCode(ClientErrorCode.NOT_FOUND_TOPIC_EXCEPTION);
        }

        private TopicPublishInfo tryToFindTopicPublishInfo(String topic)
        {
            TopicPublishInfo topicPublishInfo = this.topicPublishInfoTable.get(topic);
            if (null == topicPublishInfo || !topicPublishInfo.ok())
            {
                this.topicPublishInfoTable.putIfAbsent(topic, new TopicPublishInfo());
                this.mQClientFactory.updateTopicRouteInfoFromNameServer(topic);
                topicPublishInfo = this.topicPublishInfoTable.get(topic);
            }

            if (topicPublishInfo.isHaveTopicRouterInfo() || topicPublishInfo.ok())
            {
                return topicPublishInfo;
            }
            else
            {
                this.mQClientFactory.updateTopicRouteInfoFromNameServer(topic, true, this.defaultMQProducer);
                //topicPublishInfo = this.topicPublishInfoTable.get(topic);
                this.topicPublishInfoTable.TryGetValue(topic, out topicPublishInfo);
                return topicPublishInfo;
            }
        }

        ///<exception cref="MQClientException"/>
        ///<exception cref="RemotingException"/>
        ///<exception cref="MQBrokerException"/>
        ///<exception cref="InterruptedException"/>
        private SendResult sendKernelImpl(Message msg, MessageQueue mq, CommunicationMode communicationMode, 
            SendCallback sendCallback,TopicPublishInfo topicPublishInfo,long timeout)
        {
            long beginStartTime = Sys.currentTimeMillis();
            String brokerAddr = this.mQClientFactory.findBrokerAddressInPublish(mq.getBrokerName());
            if (null == brokerAddr)
            {
                tryToFindTopicPublishInfo(mq.getTopic());
                brokerAddr = this.mQClientFactory.findBrokerAddressInPublish(mq.getBrokerName());
            }

            SendMessageContext context = null;
            if (brokerAddr != null)
            {
                brokerAddr = MixAll.brokerVIPChannel(this.defaultMQProducer.isSendMessageWithVIPChannel(), brokerAddr);

                byte[] prevBody = msg.getBody();
                try
                {
                    //for MessageBatch,ID has been set in the generating process
                    if (!(msg is MessageBatch))
                    {
                        MessageClientIDSetter.setUniqID(msg);
                    }

                    bool topicWithNamespace = false;
                    if (null != this.mQClientFactory.getClientConfig().getNamespace())
                    {
                        msg.setInstanceId(this.mQClientFactory.getClientConfig().getNamespace());
                        topicWithNamespace = true;
                    }

                    int sysFlag = 0;
                    bool msgBodyCompressed = false;
                    if (this.tryToCompressMessage(msg))
                    {
                        sysFlag |= MessageSysFlag.COMPRESSED_FLAG;
                        msgBodyCompressed = true;
                    }

                    String tranMsg = msg.getProperty(MessageConst.PROPERTY_TRANSACTION_PREPARED);
                    if (bool.Parse(tranMsg))
                    {
                        sysFlag |= MessageSysFlag.TRANSACTION_PREPARED_TYPE;
                    }

                    if (hasCheckForbiddenHook())
                    {
                        CheckForbiddenContext checkForbiddenContext = new CheckForbiddenContext();
                        checkForbiddenContext.setNameSrvAddr(this.defaultMQProducer.getNamesrvAddr());
                        checkForbiddenContext.setGroup(this.defaultMQProducer.getProducerGroup());
                        checkForbiddenContext.setCommunicationMode(communicationMode);
                        checkForbiddenContext.setBrokerAddr(brokerAddr);
                        checkForbiddenContext.setMessage(msg);
                        checkForbiddenContext.setMq(mq);
                        checkForbiddenContext.setUnitMode(this.isUnitMode());
                        this.executeCheckForbiddenHook(checkForbiddenContext);
                    }

                    if (this.hasSendMessageHook())
                    {
                        context = new SendMessageContext();
                        context.setProducer(this);
                        context.setProducerGroup(this.defaultMQProducer.getProducerGroup());
                        context.setCommunicationMode(communicationMode);
                        context.setBornHost(this.defaultMQProducer.getClientIP());
                        context.setBrokerAddr(brokerAddr);
                        context.setMessage(msg);
                        context.setMq(mq);
                        context.setNamespace(this.defaultMQProducer.getNamespace());
                        String isTrans = msg.getProperty(MessageConst.PROPERTY_TRANSACTION_PREPARED);
                        if (isTrans != null && isTrans.Equals("true"))
                        {
                            context.setMsgType(MessageType.Trans_Msg_Half);
                        }

                        if (msg.getProperty("__STARTDELIVERTIME") != null || msg.getProperty(MessageConst.PROPERTY_DELAY_TIME_LEVEL) != null)
                        {
                            context.setMsgType(MessageType.Delay_Msg);
                        }
                        this.executeSendMessageHookBefore(context);
                    }

                    SendMessageRequestHeader requestHeader = new SendMessageRequestHeader();
                    requestHeader.producerGroup = this.defaultMQProducer.getProducerGroup();
                    requestHeader.topic = msg.getTopic();
                    requestHeader.defaultTopic = this.defaultMQProducer.getCreateTopicKey();
                    requestHeader.defaultTopicQueueNums = this.defaultMQProducer.getDefaultTopicQueueNums();
                    requestHeader.queueId = mq.getQueueId();
                    requestHeader.sysFlag = sysFlag;
                    requestHeader.bornTimestamp = Sys.currentTimeMillis();
                    requestHeader.flag = msg.getFlag();
                    requestHeader.properties = MessageDecoder.messageProperties2String(msg.getProperties());
                    requestHeader.reconsumeTimes = 0;
                    requestHeader.unitMode = this.isUnitMode();
                    requestHeader.batch = msg is MessageBatch;
                    if (requestHeader.topic.StartsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX))
                    {
                        String reconsumeTimes = MessageAccessor.getReconsumeTime(msg);
                        if (reconsumeTimes != null)
                        {
                            //requestHeader.setReconsumeTimes(Integer.valueOf(reconsumeTimes));
                            requestHeader.reconsumeTimes = int.Parse(reconsumeTimes);
                            MessageAccessor.clearProperty(msg, MessageConst.PROPERTY_RECONSUME_TIME);
                        }

                        String maxReconsumeTimes = MessageAccessor.getMaxReconsumeTimes(msg);
                        if (maxReconsumeTimes != null)
                        {
                            //requestHeader.setMaxReconsumeTimes(Integer.valueOf(maxReconsumeTimes));
                            requestHeader.maxReconsumeTimes = int.Parse(maxReconsumeTimes);
                            MessageAccessor.clearProperty(msg, MessageConst.PROPERTY_MAX_RECONSUME_TIMES);
                        }
                    }

                    SendResult sendResult = null;
                    switch (communicationMode)
                    {
                        case CommunicationMode.ASYNC:
                            Message tmpMessage = msg;
                            bool messageCloned = false;
                            if (msgBodyCompressed)
                            {
                                //If msg body was compressed, msgbody should be reset using prevBody.
                                //Clone new message using commpressed message body and recover origin massage.
                                //Fix bug:https://github.com/apache/rocketmq-externals/issues/66
                                tmpMessage = MessageAccessor.cloneMessage(msg);
                                messageCloned = true;
                                msg.setBody(prevBody);
                            }

                            if (topicWithNamespace)
                            {
                                if (!messageCloned)
                                {
                                    tmpMessage = MessageAccessor.cloneMessage(msg);
                                    messageCloned = true;
                                }
                                msg.setTopic(NamespaceUtil.withoutNamespace(msg.getTopic(), this.defaultMQProducer.getNamespace()));
                            }

                            long costTimeAsync = Sys.currentTimeMillis() - beginStartTime;
                            if (timeout < costTimeAsync)
                            {
                                throw new RemotingTooMuchRequestException("sendKernelImpl call timeout");
                            }
                            sendResult = this.mQClientFactory.getMQClientAPIImpl().sendMessage(
                                brokerAddr,
                                mq.getBrokerName(),
                                tmpMessage,
                                requestHeader,
                                timeout - costTimeAsync,
                                communicationMode,
                                sendCallback,
                                topicPublishInfo,
                                this.mQClientFactory,
                                this.defaultMQProducer.getRetryTimesWhenSendAsyncFailed(),
                                context,
                                this);
                            break;
                        case CommunicationMode.ONEWAY:
                        case CommunicationMode.SYNC:
                            long costTimeSync = Sys.currentTimeMillis() - beginStartTime;
                            if (timeout < costTimeSync)
                            {
                                throw new RemotingTooMuchRequestException("sendKernelImpl call timeout");
                            }
                            sendResult = this.mQClientFactory.getMQClientAPIImpl().sendMessage(
                                brokerAddr,
                                mq.getBrokerName(),
                                msg,
                                requestHeader,
                                timeout - costTimeSync,
                                communicationMode,
                                context,
                                this);
                            break;
                        default:
                            //assert false;
                            Debug.Assert(false);
                            break;
                    }

                    if (this.hasSendMessageHook())
                    {
                        context.setSendResult(sendResult);
                        this.executeSendMessageHookAfter(context);
                    }

                    return sendResult;
                }
                catch (RemotingException e)
                {
                    if (this.hasSendMessageHook())
                    {
                        context.setException(e);
                        this.executeSendMessageHookAfter(context);
                    }
                    throw e;
                }
                catch (MQBrokerException e)
                {
                    if (this.hasSendMessageHook())
                    {
                        context.setException(e);
                        this.executeSendMessageHookAfter(context);
                    }
                    throw e;
                }
                catch (ThreadInterruptedException e)
                {
                    if (this.hasSendMessageHook())
                    {
                        context.setException(e);
                        this.executeSendMessageHookAfter(context);
                    }
                    throw e;
                }
                finally
                {
                    msg.setBody(prevBody);
                    msg.setTopic(NamespaceUtil.withoutNamespace(msg.getTopic(), this.defaultMQProducer.getNamespace()));
                }
            }

            throw new MQClientException("The broker[" + mq.getBrokerName() + "] not exist", null);
        }

        public MQClientInstance getmQClientFactory()
        {
            return mQClientFactory;
        }

        private bool tryToCompressMessage(Message msg)
        {
            if (msg is MessageBatch)
            {
                //batch dose not support compressing right now
                return false;
            }
            byte[] body = msg.getBody();
            if (body != null)
            {
                if (body.Length >= this.defaultMQProducer.getCompressMsgBodyOverHowmuch())
                {
                    try
                    {
                        byte[] data = UtilAll.compress(body, zipCompressLevel);
                        if (data != null)
                        {
                            msg.setBody(data);
                            return true;
                        }
                    }
                    catch (IOException e)
                    {
                        log.Error("tryToCompressMessage exception", e);
                        log.Warn(msg.ToString());
                    }
                }
            }

            return false;
        }

        public bool hasCheckForbiddenHook()
        {
            return !checkForbiddenHookList.isEmpty();
        }

        ///<exception cref="MQClientException"/>
        public void executeCheckForbiddenHook(CheckForbiddenContext context)
        {
            if (hasCheckForbiddenHook())
            {
                foreach (CheckForbiddenHook hook in checkForbiddenHookList)
                {
                    hook.checkForbidden(context);
                }
            }
        }

        public bool hasSendMessageHook()
        {
            return !this.sendMessageHookList.isEmpty();
        }

        public void executeSendMessageHookBefore(SendMessageContext context)
        {
            if (!this.sendMessageHookList.isEmpty())
            {
                foreach (SendMessageHook hook in this.sendMessageHookList)
                {
                    try
                    {
                        hook.sendMessageBefore(context);
                    }
                    catch (Exception e)
                    {
                        log.Warn("failed to executeSendMessageHookBefore", e.ToString());
                    }
                }
            }
        }

        public void executeSendMessageHookAfter(SendMessageContext context)
        {
            if (!this.sendMessageHookList.isEmpty())
            {
                foreach (SendMessageHook hook in this.sendMessageHookList)
                {
                    try
                    {
                        hook.sendMessageAfter(context);
                    }
                    catch (Exception e)
                    {
                        log.Warn("failed to executeSendMessageHookAfter", e.ToString());
                    }
                }
            }
        }

        public bool hasEndTransactionHook()
        {
            return !this.endTransactionHookList.isEmpty();
        }

        public void executeEndTransactionHook(EndTransactionContext context)
        {
            if (!this.endTransactionHookList.isEmpty())
            {
                foreach (EndTransactionHook hook in this.endTransactionHookList)
                {
                    try
                    {
                        hook.endTransaction(context);
                    }
                    catch (Exception e)
                    {
                        log.Warn("failed to executeEndTransactionHook", e.ToString());
                    }
                }
            }
        }

        public void doExecuteEndTransactionHook(Message msg, String msgId, String brokerAddr, LocalTransactionState state,
            bool fromTransactionCheck)
        {
            if (hasEndTransactionHook())
            {
                EndTransactionContext context = new EndTransactionContext();
                context.setProducerGroup(defaultMQProducer.getProducerGroup());
                context.setBrokerAddr(brokerAddr);
                context.setMessage(msg);
                context.setMsgId(msgId);
                context.setTransactionId(msg.getTransactionId());
                context.setTransactionState(state);
                context.setFromTransactionCheck(fromTransactionCheck);
                executeEndTransactionHook(context);
            }
        }

        /**
         * DEFAULT ONEWAY -------------------------------------------------------
         */
        ///<exception cref="MQClientException"/>
        ///<exception cref="RemotingException"/>
        ///<exception cref="InterruptedException"/>
        public void sendOneway(Message msg)
        {
            try
            {
                this.sendDefaultImpl(msg, CommunicationMode.ONEWAY, null, this.defaultMQProducer.getSendMsgTimeout());
            }
            catch (MQBrokerException e)
            {
                throw new MQClientException("unknown exception", e);
            }
        }

        /**
         * KERNEL SYNC -------------------------------------------------------
         */
        ///<exception cref="MQClientException"/>
        ///<exception cref="RemotingException"/>
        ///<exception cref="InterruptedException"/>
        ///<exception cref="MQBrokerException"/>
        public SendResult send(Message msg, MessageQueue mq)
        {
            return send(msg, mq, this.defaultMQProducer.getSendMsgTimeout());
        }

        ///<exception cref="MQClientException"/>
        ///<exception cref="RemotingException"/>
        ///<exception cref="InterruptedException"/>
        ///<exception cref="MQBrokerException"/>
        public SendResult send(Message msg, MessageQueue mq, long timeout)
        {
            long beginStartTime = Sys.currentTimeMillis();
            this.makeSureStateOK();
            Validators.checkMessage(msg, this.defaultMQProducer);

            if (!msg.getTopic().Equals(mq.getTopic()))
            {
                throw new MQClientException("message's topic not equal mq's topic", null);
            }

            long costTime = Sys.currentTimeMillis() - beginStartTime;
            if (timeout < costTime)
            {
                throw new RemotingTooMuchRequestException("call timeout");
            }

            return this.sendKernelImpl(msg, mq, CommunicationMode.SYNC, null, null, timeout);
        }

        /**
         * KERNEL ASYNC -------------------------------------------------------
         */
        ///<exception cref="MQClientException"/>
        ///<exception cref="RemotingException"/>
        ///<exception cref="InterruptedException"/>
        public void send(Message msg, MessageQueue mq, SendCallback sendCallback)
        {
            send(msg, mq, sendCallback, this.defaultMQProducer.getSendMsgTimeout());
        }

        /**
         * @deprecated
         * It will be removed at 4.4.0 cause for exception handling and the wrong Semantics of timeout. A new one will be
         * provided in next version
         *
         * @param msg
         * @param mq
         * @param sendCallback
         * @param timeout      the <code>sendCallback</code> will be invoked at most time
         * @throws MQClientException
         * @throws RemotingException
         * @throws InterruptedException
         */
        [Obsolete]//@Deprecated
        ///<exception cref="MQClientException"/>
        ///<exception cref="RemotingException"/>
        ///<exception cref="InterruptedException"/>
        public void send(Message msg, MessageQueue mq, SendCallback sendCallback, long timeout)
        {
            long beginStartTime = Sys.currentTimeMillis();
            ExecutorService executor = this.getAsyncSenderExecutor();
            try
            {
                executor.Submit(new Runnable()
                {
                    //public void run()
                    Run = () =>
                    {
                        try
                        {
                            makeSureStateOK();
                            Validators.checkMessage(msg, defaultMQProducer);

                            if (!msg.getTopic().Equals(mq.getTopic()))
                            {
                                throw new MQClientException("message's topic not equal mq's topic", null);
                            }
                            long costTime = Sys.currentTimeMillis() - beginStartTime;
                            if (timeout > costTime)
                            {
                                try
                                {
                                    sendKernelImpl(msg, mq, CommunicationMode.ASYNC, sendCallback, null,
                                        timeout - costTime);
                                }
                                catch (MQBrokerException e)
                                {
                                    throw new MQClientException("unknown exception", e);
                                }
                            }
                            else
                            {
                                sendCallback.OnException(new RemotingTooMuchRequestException("call timeout"));
                            }
                        }
                        catch (Exception e)
                        {
                            sendCallback.OnException(e);
                        }
                    }
                });
            }
            catch (Exception e)
            {
                throw new MQClientException("executor rejected ", e);
            }

        }

        /**
         * KERNEL ONEWAY -------------------------------------------------------
         */
        ///<exception cref="MQClientException"/>
        ///<exception cref="RemotingException"/>
        ///<exception cref="InterruptedException"/>
        public void sendOneway(Message msg,
        MessageQueue mq)
        {
            this.makeSureStateOK();
            Validators.checkMessage(msg, this.defaultMQProducer);

            try
            {
                this.sendKernelImpl(msg, mq, CommunicationMode.ONEWAY, null, null, this.defaultMQProducer.getSendMsgTimeout());
            }
            catch (MQBrokerException e)
            {
                throw new MQClientException("unknown exception", e);
            }
        }

        /**
         * SELECT SYNC -------------------------------------------------------
         */
        ///<exception cref="MQClientException"/>
        ///<exception cref="RemotingException"/>
        ///<exception cref="InterruptedException"/>
        ///<exception cref="MQBrokerException"/>
        public SendResult send(Message msg, MessageQueueSelector selector, Object arg)
        {
            return send(msg, selector, arg, this.defaultMQProducer.getSendMsgTimeout());
        }

        ///<exception cref="MQClientException"/>
        ///<exception cref="RemotingException"/>
        ///<exception cref="InterruptedException"/>
        ///<exception cref="MQBrokerException"/>
        public SendResult send(Message msg, MessageQueueSelector selector, Object arg, long timeout)
        {
            return this.sendSelectImpl(msg, selector, arg, CommunicationMode.SYNC, null, timeout);
        }

        ///<exception cref="MQClientException"/>
        ///<exception cref="RemotingException"/>
        ///<exception cref="InterruptedException"/>
        ///<exception cref="MQBrokerException"/>
        private SendResult sendSelectImpl(
        Message msg,
        MessageQueueSelector selector,
        Object arg,
        CommunicationMode communicationMode,
        SendCallback sendCallback, long timeout
    )
        {
            long beginStartTime = Sys.currentTimeMillis();
            this.makeSureStateOK();
            Validators.checkMessage(msg, this.defaultMQProducer);

            TopicPublishInfo topicPublishInfo = this.tryToFindTopicPublishInfo(msg.getTopic());
            if (topicPublishInfo != null && topicPublishInfo.ok())
            {
                MessageQueue mq = null;
                try
                {
                    List<MessageQueue> messageQueueList =
                        mQClientFactory.getMQAdminImpl().parsePublishMessageQueues(topicPublishInfo.getMessageQueueList());
                    Message userMessage = MessageAccessor.cloneMessage(msg);
                    String userTopic = NamespaceUtil.withoutNamespace(userMessage.getTopic(), mQClientFactory.getClientConfig().getNamespace());
                    userMessage.setTopic(userTopic);

                    mq = mQClientFactory.getClientConfig().queueWithNamespace(selector.Select(messageQueueList, userMessage, arg));
                }
                catch (Exception e)
                {
                    throw new MQClientException("select message queue threw exception.", e);
                }

                long costTime = Sys.currentTimeMillis() - beginStartTime;
                if (timeout < costTime)
                {
                    throw new RemotingTooMuchRequestException("sendSelectImpl call timeout");
                }
                if (mq != null)
                {
                    return this.sendKernelImpl(msg, mq, communicationMode, sendCallback, null, timeout - costTime);
                }
                else
                {
                    throw new MQClientException("select message queue return null.", null);
                }
            }

            validateNameServerSetting();
            throw new MQClientException("No route info for this topic, " + msg.getTopic(), null);
        }

        /**
         * SELECT ASYNC -------------------------------------------------------
         */
        ///<exception cref="MQClientException"/>
        ///<exception cref="RemotingException"/>
        ///<exception cref="InterruptedException"/>
        public void send(Message msg, MessageQueueSelector selector, Object arg, SendCallback sendCallback)
        {
            send(msg, selector, arg, sendCallback, this.defaultMQProducer.getSendMsgTimeout());
        }

        /**
         * It will be removed at 4.4.0 cause for exception handling and the wrong Semantics of timeout. A new one will be
         * provided in next version
         *
         * @param msg
         * @param selector
         * @param arg
         * @param sendCallback
         * @param timeout      the <code>sendCallback</code> will be invoked at most time
         * @throws MQClientException
         * @throws RemotingException
         * @throws InterruptedException
         */
        ///<exception cref="MQClientException"/>
        ///<exception cref="RemotingException"/>
        ///<exception cref="InterruptedException"/>
        [Obsolete]//@Deprecated
        public void send(Message msg, MessageQueueSelector selector, Object arg,
        SendCallback sendCallback, long timeout)
        {
            long beginStartTime = Sys.currentTimeMillis();
            ExecutorService executor = this.getAsyncSenderExecutor();
            try
            {
                executor.Submit(new Runnable()
                {
                    //public void run()
                    Run = () =>
                    {
                        long costTime = Sys.currentTimeMillis() - beginStartTime;
                        if (timeout > costTime)
                        {
                            try
                            {
                                try
                                {
                                    sendSelectImpl(msg, selector, arg, CommunicationMode.ASYNC, sendCallback,
                                        timeout - costTime);
                                }
                                catch (MQBrokerException e)
                                {
                                    throw new MQClientException("unknown exception", e);
                                }
                            }
                            catch (Exception e)
                            {
                                //sendCallback.onException(e);
                                sendCallback.OnException(e);
                            }
                        }
                        else
                        {
                            //sendCallback.onException(new RemotingTooMuchRequestException("call timeout"));
                            sendCallback.OnException(new RemotingTooMuchRequestException("call timeout"));
                        }
                    }

                });
            }
            //catch (RejectedExecutionException e)
            catch(Exception e)
            {
                throw new MQClientException("executor rejected ", e);
            }
        }

        /**
         * SELECT ONEWAY -------------------------------------------------------
         */
        ///<exception cref="MQClientException"/>
        ///<exception cref="RemotingException"/>
        ///<exception cref="InterruptedException"/>
        public void sendOneway(Message msg, MessageQueueSelector selector, Object arg)
        {
            try
            {
                this.sendSelectImpl(msg, selector, arg, CommunicationMode.ONEWAY, null, this.defaultMQProducer.getSendMsgTimeout());
            }
            catch (MQBrokerException e)
            {
                throw new MQClientException("unknown exception", e);
            }
        }

        ///<exception cref="MQClientException"/>
        public TransactionSendResult sendMessageInTransaction(Message msg,
        LocalTransactionExecuter localTransactionExecuter, Object arg)
        {
            TransactionListener transactionListener = getCheckListener();
            if (null == localTransactionExecuter && null == transactionListener)
            {
                throw new MQClientException("tranExecutor is null", null);
            }

            // ignore DelayTimeLevel parameter
            if (msg.getDelayTimeLevel() != 0)
            {
                MessageAccessor.clearProperty(msg, MessageConst.PROPERTY_DELAY_TIME_LEVEL);
            }

            Validators.checkMessage(msg, this.defaultMQProducer);

            SendResult sendResult = null;
            MessageAccessor.putProperty(msg, MessageConst.PROPERTY_TRANSACTION_PREPARED, "true");
            MessageAccessor.putProperty(msg, MessageConst.PROPERTY_PRODUCER_GROUP, this.defaultMQProducer.getProducerGroup());
            try
            {
                sendResult = this.send(msg);
            }
            catch (Exception e)
            {
                throw new MQClientException("send message Exception", e);
            }

            LocalTransactionState localTransactionState = LocalTransactionState.UNKNOW;
            Exception localException = null;
            switch (sendResult.getSendStatus())
            {
                case SendStatus.SEND_OK:
                    {
                        try
                        {
                            if (sendResult.getTransactionId() != null)
                            {
                                msg.putUserProperty("__transactionId__", sendResult.getTransactionId());
                            }
                            String transactionId = msg.getProperty(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX);
                            if (null != transactionId && !"".Equals(transactionId))
                            {
                                msg.setTransactionId(transactionId);
                            }
                            if (null != localTransactionExecuter)
                            {
                                localTransactionState = localTransactionExecuter.executeLocalTransactionBranch(msg, arg);
                            }
                            else if (transactionListener != null)
                            {
                                log.Debug("Used new transaction API");
                                localTransactionState = transactionListener.executeLocalTransaction(msg, arg);
                            }
                            if (null == localTransactionState)//???
                            {
                                localTransactionState = LocalTransactionState.UNKNOW;
                            }

                            if (localTransactionState != LocalTransactionState.COMMIT_MESSAGE)
                            {
                                log.Info("executeLocalTransactionBranch return {}", localTransactionState);
                                log.Info(msg.ToString());
                            }
                        }
                        catch (Exception e)
                        {
                            log.Info("executeLocalTransactionBranch exception", e.ToString());
                            log.Info(msg.ToString());
                            localException = e;
                        }
                    }
                    break;
                case SendStatus.FLUSH_DISK_TIMEOUT:
                case SendStatus.FLUSH_SLAVE_TIMEOUT:
                case SendStatus.SLAVE_NOT_AVAILABLE:
                    localTransactionState = LocalTransactionState.ROLLBACK_MESSAGE;
                    break;
                default:
                    break;
            }

            try
            {
                this.endTransaction(msg, sendResult, localTransactionState, localException);
            }
            catch (Exception e)
            {
                log.Warn("local transaction execute " + localTransactionState + ", but end broker transaction failed", e.ToString());
            }

            TransactionSendResult transactionSendResult = new TransactionSendResult();
            transactionSendResult.setSendStatus(sendResult.getSendStatus());
            transactionSendResult.setMessageQueue(sendResult.getMessageQueue());
            transactionSendResult.setMsgId(sendResult.getMsgId());
            transactionSendResult.setQueueOffset(sendResult.getQueueOffset());
            transactionSendResult.setTransactionId(sendResult.getTransactionId());
            transactionSendResult.setLocalTransactionState(localTransactionState);
            return transactionSendResult;
        }

        /**
         * DEFAULT SYNC -------------------------------------------------------
         */
        ///<exception cref="MQClientException"/>
        ///<exception cref="RemotingException"/>
        ///<exception cref="MQBrokerException"/>
        ///<exception cref="InterruptedException"/>
        public SendResult send(Message msg)
        {
            return send(msg, this.defaultMQProducer.getSendMsgTimeout());
        }

        ///<exception cref="UnknownHostException"/>
        ///<exception cref="RemotingException"/>
        ///<exception cref="MQBrokerException"/>
        ///<exception cref="InterruptedException"/>
        public void endTransaction(
        Message msg,
        SendResult sendResult,
        LocalTransactionState localTransactionState,
        Exception localException)
        {
            MessageId id;
            if (sendResult.getOffsetMsgId() != null)
            {
                id = MessageDecoder.decodeMessageId(sendResult.getOffsetMsgId());
            }
            else
            {
                id = MessageDecoder.decodeMessageId(sendResult.getMsgId());
            }
            String transactionId = sendResult.getTransactionId();
            String brokerAddr = this.mQClientFactory.findBrokerAddressInPublish(sendResult.getMessageQueue().getBrokerName());
            EndTransactionRequestHeader requestHeader = new EndTransactionRequestHeader();
            requestHeader.transactionId = transactionId;
            requestHeader.commitLogOffset = id.getOffset();
            switch (localTransactionState)
            {
                case LocalTransactionState.COMMIT_MESSAGE:
                    requestHeader.commitOrRollback = MessageSysFlag.TRANSACTION_COMMIT_TYPE;
                    break;
                case LocalTransactionState.ROLLBACK_MESSAGE:
                    requestHeader.commitOrRollback = MessageSysFlag.TRANSACTION_ROLLBACK_TYPE;
                    break;
                case LocalTransactionState.UNKNOW:
                    requestHeader.commitOrRollback = MessageSysFlag.TRANSACTION_NOT_TYPE;
                    break;
                default:
                    break;
            }

            doExecuteEndTransactionHook(msg, sendResult.getMsgId(), brokerAddr, localTransactionState, false);
            requestHeader.producerGroup = this.defaultMQProducer.getProducerGroup();
            requestHeader.tranStateTableOffset = sendResult.getQueueOffset();
            requestHeader.msgId = sendResult.getMsgId();
            String remark = localException != null ? ("executeLocalTransactionBranch exception: " + localException.ToString()) : null;
            this.mQClientFactory.getMQClientAPIImpl().endTransactionOneway(brokerAddr, requestHeader, remark,
                this.defaultMQProducer.getSendMsgTimeout());
        }

        public void setCallbackExecutor(ExecutorService callbackExecutor)
        {
            this.mQClientFactory.getMQClientAPIImpl().getRemotingClient().setCallbackExecutor(callbackExecutor);
        }

        public ExecutorService getAsyncSenderExecutor()
        {
            return null == asyncSenderExecutor ? defaultAsyncSenderExecutor : asyncSenderExecutor;
        }

        public void setAsyncSenderExecutor(ExecutorService asyncSenderExecutor)
        {
            this.asyncSenderExecutor = asyncSenderExecutor;
        }

        ///<exception cref="MQClientException"/>
        ///<exception cref="RemotingException"/>
        ///<exception cref="MQBrokerException"/>
        ///<exception cref="InterruptedException"/>
        public SendResult send(Message msg,
    long timeout)
        {
            return this.sendDefaultImpl(msg, CommunicationMode.SYNC, null, timeout);
        }

        ///<exception cref="MQClientException"/>
        ///<exception cref="RemotingException"/>
        ///<exception cref="MQBrokerException"/>
        ///<exception cref="InterruptedException"/>
        ///<exception cref="RequestTimeoutException"/>
        public Message request(Message msg,
        long timeout)
        {
            long beginTimestamp = Sys.currentTimeMillis();
            prepareSendRequest(msg, timeout);
            String correlationId = msg.getProperty(MessageConst.PROPERTY_CORRELATION_ID);

            try
            {
                RequestResponseFuture requestResponseFuture = new RequestResponseFuture(correlationId, timeout, null);
                RequestFutureHolder.getInstance().getRequestFutureTable().put(correlationId, requestResponseFuture);

                long cost = Sys.currentTimeMillis() - beginTimestamp;
                this.sendDefaultImpl(msg, CommunicationMode.ASYNC, new SendCallback()
                {
                    //public void onSuccess(SendResult sendResult)
                    OnSuccess = (sendResult) =>
                    {
                        requestResponseFuture.setSendRequestOk(true);
                    },

                    //public void onException(Throwable e)
                    OnException = (e) =>
                    {
                        requestResponseFuture.setSendRequestOk(false);
                        requestResponseFuture.putResponseMessage(null);
                        requestResponseFuture.setCause(e);
                    }
                }, timeout - cost);

                return waitResponse(msg, timeout, requestResponseFuture, cost);
            }
            finally
            {
                RequestFutureHolder.getInstance().getRequestFutureTable().remove(correlationId);
            }
        }

        ///<exception cref="MQClientException"/>
        ///<exception cref="RemotingException"/>
        ///<exception cref="MQBrokerException"/>
        ///<exception cref="InterruptedException"/>
        public void request(Message msg, RequestCallback requestCallback, long timeout)
        {
            long beginTimestamp = Sys.currentTimeMillis();
            prepareSendRequest(msg, timeout);
            String correlationId = msg.getProperty(MessageConst.PROPERTY_CORRELATION_ID);

            RequestResponseFuture requestResponseFuture = new RequestResponseFuture(correlationId, timeout, requestCallback);
            RequestFutureHolder.getInstance().getRequestFutureTable().put(correlationId, requestResponseFuture);

            long cost = Sys.currentTimeMillis() - beginTimestamp;
            this.sendDefaultImpl(msg, CommunicationMode.ASYNC, new SendCallback()
            {
                //public void onSuccess(SendResult sendResult)
                OnSuccess = (sendResult) =>
                {
                    requestResponseFuture.setSendRequestOk(true);
                },

                //public void onException(Throwable e)
                OnException = (e) =>
                {
                    requestResponseFuture.setCause(e);
                    requestFail(correlationId);
                }
            }, timeout - cost);
        }

        ///<exception cref="MQClientException"/>
        ///<exception cref="RemotingException"/>
        ///<exception cref="MQBrokerException"/>
        ///<exception cref="InterruptedException"/>
        ///<exception cref="RequestTimeoutException"/>
        public Message request(Message msg, MessageQueueSelector selector, Object arg,
        long timeout)
        {
            long beginTimestamp = Sys.currentTimeMillis();
            prepareSendRequest(msg, timeout);
            String correlationId = msg.getProperty(MessageConst.PROPERTY_CORRELATION_ID);

            try
            {
                RequestResponseFuture requestResponseFuture = new RequestResponseFuture(correlationId, timeout, null);
                RequestFutureHolder.getInstance().getRequestFutureTable().put(correlationId, requestResponseFuture);

                long cost = Sys.currentTimeMillis() - beginTimestamp;
                this.sendSelectImpl(msg, selector, arg, CommunicationMode.ASYNC, new SendCallback()
                {
                    //public void onSuccess(SendResult sendResult)
                    OnSuccess = (sendResult) =>
                    {
                        requestResponseFuture.setSendRequestOk(true);
                    },

                    //public void onException(Throwable e)
                    OnException = (e) =>
                    {
                        requestResponseFuture.setSendRequestOk(false);
                        requestResponseFuture.putResponseMessage(null);
                        requestResponseFuture.setCause(e);
                    }
                }, timeout - cost);

                return waitResponse(msg, timeout, requestResponseFuture, cost);
            }
            finally
            {
                RequestFutureHolder.getInstance().getRequestFutureTable().remove(correlationId);
            }
        }

        ///<exception cref="MQClientException"/>
        ///<exception cref="RemotingException"/>
        ///<exception cref="MQBrokerException"/>
        ///<exception cref="InterruptedException"/>
        public void request(Message msg, MessageQueueSelector selector, Object arg,
        RequestCallback requestCallback, long timeout)
        {
            long beginTimestamp = Sys.currentTimeMillis();
            prepareSendRequest(msg, timeout);
            String correlationId = msg.getProperty(MessageConst.PROPERTY_CORRELATION_ID);

            RequestResponseFuture requestResponseFuture = new RequestResponseFuture(correlationId, timeout, requestCallback);
            RequestFutureHolder.getInstance().getRequestFutureTable().put(correlationId, requestResponseFuture);

            long cost = Sys.currentTimeMillis() - beginTimestamp;
            this.sendSelectImpl(msg, selector, arg, CommunicationMode.ASYNC, new SendCallback()
            {
                //public void onSuccess(SendResult sendResult)
                OnSuccess = (sendResult) =>
                {
                    requestResponseFuture.setSendRequestOk(true);
                },

                //public void onException(Throwable e)
                OnException = (e) =>
                        {
                            requestResponseFuture.setCause(e);
                            requestFail(correlationId);
                        }
            }, timeout - cost);

        }

        ///<exception cref="MQClientException"/>
        ///<exception cref="RemotingException"/>
        ///<exception cref="MQBrokerException"/>
        ///<exception cref="InterruptedException"/>
        ///<exception cref="RequestTimeoutException"/>
        public Message request(Message msg, MessageQueue mq, long timeout)
        {
            long beginTimestamp = Sys.currentTimeMillis();
            prepareSendRequest(msg, timeout);
            String correlationId = msg.getProperty(MessageConst.PROPERTY_CORRELATION_ID);

            try
            {
                RequestResponseFuture requestResponseFuture = new RequestResponseFuture(correlationId, timeout, null);
                RequestFutureHolder.getInstance().getRequestFutureTable().put(correlationId, requestResponseFuture);

                long cost = Sys.currentTimeMillis() - beginTimestamp;
                this.sendKernelImpl(msg, mq, CommunicationMode.ASYNC, new SendCallback()
                {
                    //public void onSuccess(SendResult sendResult)
                    OnSuccess = (sendResult) =>
                    {
                        requestResponseFuture.setSendRequestOk(true);
                    },

                    //public void onException(Throwable e)
                    OnException = (e) =>
                    {
                        requestResponseFuture.setSendRequestOk(false);
                        requestResponseFuture.putResponseMessage(null);
                        requestResponseFuture.setCause(e);
                    }
                }, null, timeout - cost);

                return waitResponse(msg, timeout, requestResponseFuture, cost);
            }
            finally
            {
                RequestFutureHolder.getInstance().getRequestFutureTable().remove(correlationId);
            }
        }

        ///<exception cref="MQClientException"/>
        ///<exception cref="InterruptedException"/>
        ///<exception cref="RequestTimeoutException"/>
        private Message waitResponse(Message msg, long timeout, RequestResponseFuture requestResponseFuture, long cost)
        {
            Message responseMessage = requestResponseFuture.waitResponseMessage(timeout - cost).Result; //???
            if (responseMessage == null)
            {
                if (requestResponseFuture.isSendRequestOk())
                {
                    throw new RequestTimeoutException(ClientErrorCode.REQUEST_TIMEOUT_EXCEPTION,
                        "send request message to <" + msg.getTopic() + "> OK, but wait reply message timeout, " + timeout + " ms.");
                }
                else
                {
                    throw new MQClientException("send request message to <" + msg.getTopic() + "> fail", requestResponseFuture.getCause());
                }
            }
            return responseMessage;
        }

        ///<exception cref="MQClientException"/>
        ///<exception cref="RemotingException"/>
        ///<exception cref="MQBrokerException"/>
        ///<exception cref="InterruptedException"/>
        public void request(Message msg, MessageQueue mq, RequestCallback requestCallback, long timeout)
        {
            long beginTimestamp = Sys.currentTimeMillis();
            prepareSendRequest(msg, timeout);
            String correlationId = msg.getProperty(MessageConst.PROPERTY_CORRELATION_ID);

            RequestResponseFuture requestResponseFuture = new RequestResponseFuture(correlationId, timeout, requestCallback);
            RequestFutureHolder.getInstance().getRequestFutureTable().put(correlationId, requestResponseFuture);

            long cost = Sys.currentTimeMillis() - beginTimestamp;
            this.sendKernelImpl(msg, mq, CommunicationMode.ASYNC, new SendCallback()
            {
                //public void onSuccess(SendResult sendResult)
                OnSuccess = (sendResult) =>
                {
                    requestResponseFuture.setSendRequestOk(true);
                },

                //public void onException(Throwable e)
                OnException = (e) =>
                {
                    requestResponseFuture.setCause(e);
                    requestFail(correlationId);
                }
            }, null, timeout - cost);
        }

        private void requestFail(String correlationId)
        {
            RequestResponseFuture responseFuture = RequestFutureHolder.getInstance().getRequestFutureTable().remove(correlationId);
            if (responseFuture != null)
            {
                responseFuture.setSendRequestOk(false);
                responseFuture.putResponseMessage(null);
                try
                {
                    responseFuture.executeRequestCallback();
                }
                catch (Exception e)
                {
                    log.Warn("execute requestCallback in requestFail, and callback throw", e.ToString());
                }
            }
        }

        private void prepareSendRequest(Message msg, long timeout)
        {
            String correlationId = CorrelationIdUtil.createCorrelationId();
            String requestClientId = this.getmQClientFactory().getClientId();
            MessageAccessor.putProperty(msg, MessageConst.PROPERTY_CORRELATION_ID, correlationId);
            MessageAccessor.putProperty(msg, MessageConst.PROPERTY_MESSAGE_REPLY_TO_CLIENT, requestClientId);
            MessageAccessor.putProperty(msg, MessageConst.PROPERTY_MESSAGE_TTL, timeout.ToString());

            bool hasRouteData = this.getmQClientFactory().getTopicRouteTable().ContainsKey(msg.getTopic());
            if (!hasRouteData)
            {
                long beginTimestamp = Sys.currentTimeMillis();
                this.tryToFindTopicPublishInfo(msg.getTopic());
                this.getmQClientFactory().sendHeartbeatToAllBrokerWithLock();
                long cost = Sys.currentTimeMillis() - beginTimestamp;
                if (cost > 500)
                {
                    log.Warn("prepare send request for <{}> cost {} ms", msg.getTopic(), cost);
                }
            }
        }

        public ConcurrentDictionary<String, TopicPublishInfo> getTopicPublishInfoTable()
        {
            return topicPublishInfoTable;
        }

        public int getZipCompressLevel()
        {
            return zipCompressLevel;
        }

        public void setZipCompressLevel(int zipCompressLevel)
        {
            this.zipCompressLevel = zipCompressLevel;
        }

        public ServiceState getServiceState()
        {
            return serviceState;
        }

        public void setServiceState(ServiceState serviceState)
        {
            this.serviceState = serviceState;
        }

        public long[] getNotAvailableDuration()
        {
            return this.mqFaultStrategy.getNotAvailableDuration();
        }

        public void setNotAvailableDuration(long[] notAvailableDuration)
        {
            this.mqFaultStrategy.setNotAvailableDuration(notAvailableDuration);
        }

        public long[] getLatencyMax()
        {
            return this.mqFaultStrategy.getLatencyMax();
        }

        public void setLatencyMax(long[] latencyMax)
        {
            this.mqFaultStrategy.setLatencyMax(latencyMax);
        }

        public bool isSendLatencyFaultEnable()
        {
            return this.mqFaultStrategy.isSendLatencyFaultEnable();
        }

        public void setSendLatencyFaultEnable(bool sendLatencyFaultEnable)
        {
            this.mqFaultStrategy.setSendLatencyFaultEnable(sendLatencyFaultEnable);
        }

        public DefaultMQProducer getDefaultMQProducer()
        {
            return defaultMQProducer;
        }
    }
}
