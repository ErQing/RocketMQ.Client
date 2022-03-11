using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;

namespace RocketMQ.Client
{
    public class ConsumeMessageOrderlyService : ConsumeMessageService
    {
        //private static readonly InternalLogger log = ClientLogger.getLog();
        static NLog.Logger log = NLog.LogManager.GetCurrentClassLogger();
        private readonly static long MAX_TIME_CONSUME_CONTINUOUSLY =
        long.Parse(Sys.getProperty("rocketmq.client.maxTimeConsumeContinuously", "60000"));
        private readonly DefaultMQPushConsumerImpl defaultMQPushConsumerImpl;
        private readonly DefaultMQPushConsumer defaultMQPushConsumer;
        private readonly MessageListenerOrderly messageListener;
        private readonly BlockingQueue<Runnable> consumeRequestQueue;
        //private readonly ThreadPoolExecutor consumeExecutor;
        private readonly ExecutorService consumeExecutor;
        private readonly string consumerGroup;
        private readonly MessageQueueLock messageQueueLock = new MessageQueueLock();
        private readonly ScheduledExecutorService scheduledExecutorService;
        private volatile bool stopped = false;

        public ConsumeMessageOrderlyService(DefaultMQPushConsumerImpl defaultMQPushConsumerImpl,
            MessageListenerOrderly messageListener)
        {
            this.defaultMQPushConsumerImpl = defaultMQPushConsumerImpl;
            this.messageListener = messageListener;

            this.defaultMQPushConsumer = this.defaultMQPushConsumerImpl.getDefaultMQPushConsumer();
            this.consumerGroup = this.defaultMQPushConsumer.getConsumerGroup();
            //this.consumeRequestQueue = new LinkedBlockingQueue<Runnable>();
            this.consumeRequestQueue = BlockingQueue<Runnable>.Create();

            string consumeThreadPrefix = null;
            if (consumerGroup.length() > 100)
            {
                consumeThreadPrefix = new StringBuilder("ConsumeMessageThread_").Append(consumerGroup.JavaSubstring(0, 100)).Append("_").ToString();
            }
            else
            {
                consumeThreadPrefix = new StringBuilder("ConsumeMessageThread_").Append(consumerGroup).Append("_").ToString();
            }
            //this.consumeExecutor = new ThreadPoolExecutor(
            //    this.defaultMQPushConsumer.getConsumeThreadMin(),
            //    this.defaultMQPushConsumer.getConsumeThreadMax(),
            //    1000 * 60,
            //    TimeUnit.MILLISECONDS,
            //    this.consumeRequestQueue,
            //    new ThreadFactoryImpl(consumeThreadPrefix));
            //this.scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl("ConsumeMessageScheduledThread_"));
            consumeExecutor = new ExecutorService(defaultMQPushConsumer.getConsumeThreadMax());
            scheduledExecutorService = new ScheduledExecutorService();
        }

        public void start()
        {
            if (MessageModel.CLUSTERING.Equals(defaultMQPushConsumerImpl.messageModel()))
            {
                this.scheduledExecutorService.ScheduleAtFixedRate(new Runnable()
                {
                    //public void run()
                    Run = () =>
                     {
                         try
                         {
                             lockMQPeriodically();
                         }
                         catch (Exception e)
                         {
                             log.Error("scheduleAtFixedRate lockMQPeriodically exception", e.ToString());
                         }
                     }
                }, 1000 * 1, ProcessQueue.REBALANCE_LOCK_INTERVAL);
            }
        }

        public void shutdown(long awaitTerminateMillis)
        {
            this.stopped = true;
            this.scheduledExecutorService.Shutdown();
            ThreadUtils.shutdownGracefully(this.consumeExecutor, awaitTerminateMillis, TimeUnit.MILLISECONDS);
            if (MessageModel.CLUSTERING.Equals(this.defaultMQPushConsumerImpl.messageModel()))
            {
                this.unlockAllMQ();
            }
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public void unlockAllMQ()
        {
            this.defaultMQPushConsumerImpl.getRebalanceImpl().unlockAll(false);
        }

        //@Override
        public void updateCorePoolSize(int corePoolSize)
        {
            if (corePoolSize > 0
                && corePoolSize <= short.MaxValue
                && corePoolSize < this.defaultMQPushConsumer.getConsumeThreadMax())
            {
                this.consumeExecutor.setCorePoolSize(corePoolSize);
            }
        }

        //@Override
        public void incCorePoolSize()
        {
        }

        //@Override
        public void decCorePoolSize()
        {
        }

        //@Override
        public int getCorePoolSize()
        {
            return this.consumeExecutor.getCorePoolSize();
        }

        //@Override
        public ConsumeMessageDirectlyResult consumeMessageDirectly(MessageExt msg, string brokerName)
        {
            ConsumeMessageDirectlyResult result = new ConsumeMessageDirectlyResult();
            result.order = true;

            List<MessageExt> msgs = new List<MessageExt>();
            msgs.Add(msg);
            MessageQueue mq = new MessageQueue();
            mq.setBrokerName(brokerName);
            mq.setTopic(msg.getTopic());
            mq.setQueueId(msg.getQueueId());

            ConsumeOrderlyContext context = new ConsumeOrderlyContext(mq);

            this.defaultMQPushConsumerImpl.resetRetryAndNamespace(msgs, this.consumerGroup);

            long beginTime = Sys.currentTimeMillis();
            log.Info("consumeMessageDirectly receive new message: {}", msg);

            try
            {
                ConsumeOrderlyStatus status = this.messageListener.consumeMessage(msgs, context);
                if (status != null)
                {
                    switch (status)
                    {
                        case ConsumeOrderlyStatus.COMMIT:
                            result.consumeResult = (CMResult.CR_COMMIT);
                            break;
                        case ConsumeOrderlyStatus.ROLLBACK:
                            result.consumeResult = (CMResult.CR_ROLLBACK);
                            break;
                        case ConsumeOrderlyStatus.SUCCESS:
                            result.consumeResult = (CMResult.CR_SUCCESS);
                            break;
                        case ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT:
                            result.consumeResult = (CMResult.CR_LATER);
                            break;
                        default:
                            break;
                    }
                }
                else
                {
                    result.consumeResult = (CMResult.CR_RETURN_NULL);
                }
            }
            catch (Exception e)
            {
                result.consumeResult = (CMResult.CR_THROW_EXCEPTION);
                result.remark = (RemotingHelper.exceptionSimpleDesc(e));

                log.Warn(String.Format("consumeMessageDirectly exception: %s Group: %s Msgs: %s MQ: %s",
                    RemotingHelper.exceptionSimpleDesc(e),
                    consumerGroup,
                    msgs,
                    mq), e);
            }

            result.autoCommit = context.isAutoCommit();
            result.spentTimeMills = Sys.currentTimeMillis() - beginTime;

            log.Info("consumeMessageDirectly Result: {}", result);
            return result;
        }

        //@Override
        public void submitConsumeRequest(
            List<MessageExt> msgs,
            ProcessQueue processQueue,
            MessageQueue messageQueue,
            bool dispathToConsume)
        {
            if (dispathToConsume)
            {
                ConsumeRequest consumeRequest = new ConsumeRequest(processQueue, messageQueue);
                this.consumeExecutor.Submit(consumeRequest);
            }
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public void lockMQPeriodically()
        {
            if (!this.stopped)
            {
                this.defaultMQPushConsumerImpl.getRebalanceImpl().lockAll();
            }
        }

        public void tryLockLaterAndReconsume(MessageQueue mq, ProcessQueue processQueue,
            long delayMills)
        {
            this.scheduledExecutorService.Schedule(new Runnable()
            {
                //public void run()
                Run = () =>
            {
                bool lockOK = /*ConsumeMessageOrderlyService.this.*/lockOneMQ(mq);
                if (lockOK)
                {
                /*ConsumeMessageOrderlyService.this.*/
                    submitConsumeRequestLater(processQueue, mq, 10);
                }
                else
                {
                /*ConsumeMessageOrderlyService.this.*/
                    submitConsumeRequestLater(processQueue, mq, 3000);
                }
            }
            }, delayMills);
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public bool lockOneMQ(MessageQueue mq)
        {
            if (!this.stopped)
            {
                return this.defaultMQPushConsumerImpl.getRebalanceImpl().lockImpl(mq) ;
            }

            return false;
        }

        private void submitConsumeRequestLater(
            ProcessQueue processQueue,
            MessageQueue messageQueue,
            long suspendTimeMillis
        )
        {
            long timeMillis = suspendTimeMillis;
            if (timeMillis == -1)
            {
                timeMillis = this.defaultMQPushConsumer.getSuspendCurrentQueueTimeMillis();
            }

            if (timeMillis < 10)
            {
                timeMillis = 10;
            }
            else if (timeMillis > 30000)
            {
                timeMillis = 30000;
            }

            this.scheduledExecutorService.Schedule(new Runnable()
            {

                //public void run()
                Run = () =>
                {
                    /*ConsumeMessageOrderlyService.this.*/
                    submitConsumeRequest(null, processQueue, messageQueue, true);
                }
            }, timeMillis);
        }

        private bool processConsumeResult(List<MessageExt> msgs,  ConsumeOrderlyStatus status,
            ConsumeOrderlyContext context,ConsumeRequest consumeRequest)
        {
            bool continueConsume = true;
            long commitOffset = -1L;
            if (context.isAutoCommit())
            {
                switch (status)
                {
                    case ConsumeOrderlyStatus.COMMIT:
                    case ConsumeOrderlyStatus.ROLLBACK:
                        log.Warn("the message queue consume result is illegal, we think you want to ack these message {}",
                            consumeRequest.getMessageQueue());
                        break; //???
                    case ConsumeOrderlyStatus.SUCCESS:
                        commitOffset = consumeRequest.getProcessQueue().commit();
                        this.getConsumerStatsManager().incConsumeOKTPS(consumerGroup, consumeRequest.getMessageQueue().getTopic(), msgs.Count);
                        break;
                    case ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT:
                        this.getConsumerStatsManager().incConsumeFailedTPS(consumerGroup, consumeRequest.getMessageQueue().getTopic(), msgs.Count);
                        if (checkReconsumeTimes(msgs))
                        {
                            consumeRequest.getProcessQueue().makeMessageToConsumeAgain(msgs);
                            this.submitConsumeRequestLater(
                                consumeRequest.getProcessQueue(),
                                consumeRequest.getMessageQueue(),
                                context.getSuspendCurrentQueueTimeMillis());
                            continueConsume = false;
                        }
                        else
                        {
                            commitOffset = consumeRequest.getProcessQueue().commit();
                        }
                        break;
                    default:
                        break;
                }
            }
            else
            {
                switch (status)
                {
                    case ConsumeOrderlyStatus.SUCCESS:
                        this.getConsumerStatsManager().incConsumeOKTPS(consumerGroup, consumeRequest.getMessageQueue().getTopic(), msgs.Count);
                        break;
                    case ConsumeOrderlyStatus.COMMIT:
                        commitOffset = consumeRequest.getProcessQueue().commit();
                        break;
                    case ConsumeOrderlyStatus.ROLLBACK:
                        consumeRequest.getProcessQueue().rollback();
                        this.submitConsumeRequestLater(
                            consumeRequest.getProcessQueue(),
                            consumeRequest.getMessageQueue(),
                            context.getSuspendCurrentQueueTimeMillis());
                        continueConsume = false;
                        break;
                    case ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT:
                        this.getConsumerStatsManager().incConsumeFailedTPS(consumerGroup, consumeRequest.getMessageQueue().getTopic(), msgs.Count);
                        if (checkReconsumeTimes(msgs))
                        {
                            consumeRequest.getProcessQueue().makeMessageToConsumeAgain(msgs);
                            this.submitConsumeRequestLater(
                                consumeRequest.getProcessQueue(),
                                consumeRequest.getMessageQueue(),
                                context.getSuspendCurrentQueueTimeMillis());
                            continueConsume = false;
                        }
                        break;
                    default:
                        break;
                }
            }

            if (commitOffset >= 0 && !consumeRequest.getProcessQueue().isDropped())
            {
                this.defaultMQPushConsumerImpl.getOffsetStore().updateOffset(consumeRequest.getMessageQueue(), commitOffset, false);
            }

            return continueConsume;
        }

        public ConsumerStatsManager getConsumerStatsManager()
        {
            return this.defaultMQPushConsumerImpl.getConsumerStatsManager();
        }

        private int getMaxReconsumeTimes()
        {
            // default reconsume times: Integer.MAX_VALUE
            if (this.defaultMQPushConsumer.getMaxReconsumeTimes() == -1)
            {
                return int.MaxValue;
            }
            else
            {
                return this.defaultMQPushConsumer.getMaxReconsumeTimes();
            }
        }

        private bool checkReconsumeTimes(List<MessageExt> msgs)
        {
            bool suspend = false;
            if (msgs != null && !msgs.IsEmpty())
            {
                foreach (MessageExt msg in msgs)
                {
                    if (msg.getReconsumeTimes() >= getMaxReconsumeTimes())
                    {
                        MessageAccessor.setReconsumeTime(msg, msg.getReconsumeTimes().ToString());
                        if (!sendMessageBack(msg))
                        {
                            suspend = true;
                            msg.setReconsumeTimes(msg.getReconsumeTimes() + 1);
                        }
                    }
                    else
                    {
                        suspend = true;
                        msg.setReconsumeTimes(msg.getReconsumeTimes() + 1);
                    }
                }
            }
            return suspend;
        }

        public bool sendMessageBack(MessageExt msg)
        {
            try
            {
                // max reconsume times exceeded then send to dead letter queue.
                Message newMsg = new Message(MixAll.getRetryTopic(this.defaultMQPushConsumer.getConsumerGroup()), msg.getBody());
                string originMsgId = MessageAccessor.getOriginMessageId(msg);
                MessageAccessor.setOriginMessageId(newMsg, UtilAll.isBlank(originMsgId) ? msg.getMsgId() : originMsgId);
                newMsg.setFlag(msg.getFlag());
                MessageAccessor.setProperties(newMsg, msg.getProperties());
                MessageAccessor.putProperty(newMsg, MessageConst.PROPERTY_RETRY_TOPIC, msg.getTopic());
                MessageAccessor.setReconsumeTime(newMsg, msg.getReconsumeTimes().ToString());
                MessageAccessor.setMaxReconsumeTimes(newMsg, getMaxReconsumeTimes().ToString());
                MessageAccessor.clearProperty(newMsg, MessageConst.PROPERTY_TRANSACTION_PREPARED);
                newMsg.setDelayTimeLevel(3 + msg.getReconsumeTimes());

                this.defaultMQPushConsumer.getDefaultMQPushConsumerImpl().getmQClientFactory().getDefaultMQProducer().send(newMsg);
                return true;
            }
            catch (Exception e)
            {
                log.Error("sendMessageBack exception, group: " + this.consumerGroup + " msg: " + msg.ToString(), e.ToString());
            }

            return false;
        }

        public void resetNamespace(List<MessageExt> msgs)
        {
            foreach (MessageExt msg in msgs)
            {
                if (Str.isNotEmpty(this.defaultMQPushConsumer.getNamespace()))
                {
                    msg.setTopic(NamespaceUtil.withoutNamespace(msg.getTopic(), this.defaultMQPushConsumer.getNamespace()));
                }
            }
        }

        class ConsumeRequest : IRunnable
        {

            private ConsumeMessageOrderlyService owner;
            private ConsumeRequest(ConsumeMessageOrderlyService owner)
            {
                this.owner = owner;
            }

            private ProcessQueue processQueue;
            private MessageQueue messageQueue;

            public ConsumeRequest(ProcessQueue processQueue, MessageQueue messageQueue)
            {
                this.processQueue = processQueue;
                this.messageQueue = messageQueue;
            }

            public ProcessQueue getProcessQueue()
            {
                return processQueue;
            }

            public MessageQueue getMessageQueue()
            {
                return messageQueue;
            }

            public void run()
            {
                if (this.processQueue.isDropped())
                {
                    log.Warn("run, the message queue not be able to consume, because it's dropped. {}", this.messageQueue);
                    return;
                }

                object objLock = owner.messageQueueLock.fetchLockObject(this.messageQueue);
                lock (objLock)
                {
                    if (MessageModel.BROADCASTING.Equals(owner.defaultMQPushConsumerImpl.messageModel())
                        || (this.processQueue.isLocked() && !this.processQueue.isLockExpired()))
                    {
                        long beginTime = Sys.currentTimeMillis();
                        for (bool continueConsume = true; continueConsume;)
                        {
                            if (this.processQueue.isDropped())
                            {
                                log.Warn("the message queue not be able to consume, because it's dropped. {}", this.messageQueue);
                                break;
                            }

                            if (MessageModel.CLUSTERING.Equals(owner.defaultMQPushConsumerImpl.messageModel())
                                && !this.processQueue.isLocked())
                            {
                                log.Warn("the message queue not locked, so consume later, {}", this.messageQueue);
                                owner.tryLockLaterAndReconsume(this.messageQueue, this.processQueue, 10);
                                break;
                            }

                            if (MessageModel.CLUSTERING.Equals(owner.defaultMQPushConsumerImpl.messageModel())
                                && this.processQueue.isLockExpired())
                            {
                                log.Warn("the message queue lock expired, so consume later, {}", this.messageQueue);
                                owner.tryLockLaterAndReconsume(this.messageQueue, this.processQueue, 10);
                                break;
                            }

                            long interval = Sys.currentTimeMillis() - beginTime;
                            if (interval > MAX_TIME_CONSUME_CONTINUOUSLY)
                            {
                                /*ConsumeMessageOrderlyService.this.*/
                                owner.submitConsumeRequestLater(processQueue, messageQueue, 10);
                                break;
                            }

                            int consumeBatchSize =
                                /*ConsumeMessageOrderlyService.this.*/owner.defaultMQPushConsumer.getConsumeMessageBatchMaxSize();

                            List<MessageExt> msgs = this.processQueue.takeMessages(consumeBatchSize);
                            owner.defaultMQPushConsumerImpl.resetRetryAndNamespace(msgs, owner.defaultMQPushConsumer.getConsumerGroup());
                            if (!msgs.IsEmpty())
                            {
                                ConsumeOrderlyContext context = new ConsumeOrderlyContext(this.messageQueue);

                                ConsumeOrderlyStatus status = ConsumeOrderlyStatus.UNKNOWN;
                                ConsumeMessageContext consumeMessageContext = null;
                                if (owner.defaultMQPushConsumerImpl.hasHook())
                                {
                                    consumeMessageContext = new ConsumeMessageContext();
                                    consumeMessageContext
                                        .setConsumerGroup(owner.defaultMQPushConsumer.getConsumerGroup());
                                    consumeMessageContext.setNamespace(owner.defaultMQPushConsumer.getNamespace());
                                    consumeMessageContext.setMq(messageQueue);
                                    consumeMessageContext.setMsgList(msgs);
                                    consumeMessageContext.setSuccess(false);
                                    // init the consume context type
                                    consumeMessageContext.setProps(new HashMap<String, String>());
                                    owner.defaultMQPushConsumerImpl.executeHookBefore(consumeMessageContext);
                                }

                                long beginTimestamp = Sys.currentTimeMillis();
                                ConsumeReturnType returnType = ConsumeReturnType.SUCCESS;
                                bool hasException = false;
                                try
                                {
                                    //this.processQueue.getConsumeLock().lock () ;
                                    Monitor.Enter(processQueue.getConsumeLock());
                                    if (this.processQueue.isDropped())
                                    {
                                        log.Warn("consumeMessage, the message queue not be able to consume, because it's dropped. {}",
                                            this.messageQueue);
                                        break;
                                    }

                                    status = owner.messageListener.consumeMessage(Collections.unmodifiableList(msgs), context);
                                }
                                catch (Exception e)
                                {
                                    log.Warn(String.Format("consumeMessage exception: %s Group: %s Msgs: %s MQ: %s",
                                        RemotingHelper.exceptionSimpleDesc(e),
                                        owner.consumerGroup,
                                        msgs,
                                        messageQueue), e.ToString());
                                    hasException = true;
                                }
                                finally
                                {
                                    //this.processQueue.getConsumeLock().unlock();
                                    Monitor.Exit(processQueue.getConsumeLock());
                                }

                                if (null == status
                                    || ConsumeOrderlyStatus.ROLLBACK == status
                                    || ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT == status)
                                {
                                    log.Warn("consumeMessage Orderly return not OK, Group: {} Msgs: {} MQ: {}",
                                        owner.consumerGroup,
                                        msgs,
                                        messageQueue);
                                }

                                long consumeRT = Sys.currentTimeMillis() - beginTimestamp;
                                if (null == status)
                                {
                                    if (hasException)
                                    {
                                        returnType = ConsumeReturnType.EXCEPTION;
                                    }
                                    else
                                    {
                                        returnType = ConsumeReturnType.RETURNNULL;
                                    }
                                }
                                else if (consumeRT >= owner.defaultMQPushConsumer.getConsumeTimeout() * 60 * 1000)
                                {
                                    returnType = ConsumeReturnType.TIME_OUT;
                                }
                                else if (ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT == status)
                                {
                                    returnType = ConsumeReturnType.FAILED;
                                }
                                else if (ConsumeOrderlyStatus.SUCCESS == status)
                                {
                                    returnType = ConsumeReturnType.SUCCESS;
                                }

                                if (owner.defaultMQPushConsumerImpl.hasHook())
                                {
                                    consumeMessageContext.getProps().Put(MixAll.CONSUME_CONTEXT_TYPE, returnType.name());
                                }

                                if (null == status)
                                {
                                    status = ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT;
                                }

                                if (owner.defaultMQPushConsumerImpl.hasHook())
                                {
                                    consumeMessageContext.setStatus(status.ToString());
                                    consumeMessageContext
                                        .setSuccess(ConsumeOrderlyStatus.SUCCESS == status || ConsumeOrderlyStatus.COMMIT == status);
                                    owner.defaultMQPushConsumerImpl.executeHookAfter(consumeMessageContext);
                                }

                                owner.getConsumerStatsManager().incConsumeRT(owner.consumerGroup, messageQueue.getTopic(), consumeRT);
                                continueConsume = owner.processConsumeResult(msgs, status, context, this);
                            }
                            else
                            {
                                continueConsume = false;
                            }
                        }
                    }
                    else
                    {
                        if (this.processQueue.isDropped())
                        {
                            log.Warn("the message queue not be able to consume, because it's dropped. {}", this.messageQueue);
                            return;
                        }

                        owner.tryLockLaterAndReconsume(this.messageQueue, this.processQueue, 100);
                    }
                }
            }

        }
    }
}
