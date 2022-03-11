using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace RocketMQ.Client
{
    public class ConsumeMessageConcurrentlyService : ConsumeMessageService
    {
        //private static readonly InternalLogger log = ClientLogger.getLog();
        static NLog.Logger log = NLog.LogManager.GetCurrentClassLogger();
        private readonly DefaultMQPushConsumerImpl defaultMQPushConsumerImpl;
        private readonly DefaultMQPushConsumer defaultMQPushConsumer;
        private readonly MessageListenerConcurrently messageListener;
        private readonly BlockingQueue<Runnable> consumeRequestQueue;
        //private readonly ThreadPoolExecutor consumeExecutor;
        private readonly ExecutorService consumeExecutor;
        private readonly string consumerGroup;

        private readonly ScheduledExecutorService scheduledExecutorService;
        private readonly ScheduledExecutorService cleanExpireMsgExecutors;

        public ConsumeMessageConcurrentlyService(DefaultMQPushConsumerImpl defaultMQPushConsumerImpl,
            MessageListenerConcurrently messageListener)
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
            //this.cleanExpireMsgExecutors = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl("CleanExpireMsgScheduledThread_"));
            consumeExecutor = new ExecutorService(defaultMQPushConsumer.getConsumeThreadMax());
            scheduledExecutorService = new ScheduledExecutorService();
            cleanExpireMsgExecutors = new ScheduledExecutorService();
        }

        public void start()
        {
            this.cleanExpireMsgExecutors.ScheduleAtFixedRate(new Runnable()
            {

                //public void run()
                Run = () =>
                {
                    try
                    {
                        cleanExpireMsg();
                    }
                    catch (Exception e)
                    {
                        log.Error("scheduleAtFixedRate cleanExpireMsg exception", e.ToString());
                    }
                }

            }, this.defaultMQPushConsumer.getConsumeTimeout(), this.defaultMQPushConsumer.getConsumeTimeout());
        }

        public void shutdown(long awaitTerminateMillis)
        {
            this.scheduledExecutorService.Shutdown();
            ThreadUtils.shutdownGracefully(this.consumeExecutor, awaitTerminateMillis, TimeUnit.MILLISECONDS);
            this.cleanExpireMsgExecutors.Shutdown();
        }

        //@Override
        public void updateCorePoolSize(int corePoolSize)
        {
            if (corePoolSize > 0  && corePoolSize <= short.MaxValue 
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
            result.order = false;
            result.autoCommit = (true);

            List<MessageExt> msgs = new List<MessageExt>();
            msgs.Add(msg);
            MessageQueue mq = new MessageQueue();
            mq.setBrokerName(brokerName);
            mq.setTopic(msg.getTopic());
            mq.setQueueId(msg.getQueueId());

            ConsumeConcurrentlyContext context = new ConsumeConcurrentlyContext(mq);

            this.defaultMQPushConsumerImpl.resetRetryAndNamespace(msgs, this.consumerGroup);

            long beginTime = Sys.currentTimeMillis();

            log.Info("consumeMessageDirectly receive new message: {}", msg);

            try
            {
                ConsumeConcurrentlyStatus status = this.messageListener.consumeMessage(msgs, context);
                if (status != null)
                {
                    switch (status)
                    {
                        case ConsumeConcurrentlyStatus.CONSUME_SUCCESS:
                            result.consumeResult = CMResult.CR_SUCCESS;
                            break;
                        case ConsumeConcurrentlyStatus.RECONSUME_LATER:
                            result.consumeResult = CMResult.CR_LATER;
                            break;
                        default:
                            break;
                    }
                }
                else
                {
                    result.consumeResult = CMResult.CR_RETURN_NULL;
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
                    mq), e.ToString());
            }

            result.spentTimeMills = (Sys.currentTimeMillis() - beginTime);

            log.Info("consumeMessageDirectly Result: {}", result);

            return result;
        }

        //@Override
        public void submitConsumeRequest(List<MessageExt> msgs, ProcessQueue processQueue, MessageQueue messageQueue, bool dispatchToConsume)
        {
            int consumeBatchSize = this.defaultMQPushConsumer.getConsumeMessageBatchMaxSize();
            if (msgs.Count <= consumeBatchSize)
            {
                ConsumeRequest consumeRequest = new ConsumeRequest(msgs, processQueue, messageQueue);
                try
                {
                    this.consumeExecutor.Submit(consumeRequest);
                }
                catch (Exception e)
                {
                    this.submitConsumeRequestLater(consumeRequest);
                }
            }
            else
            {
                for (int total = 0; total < msgs.Count;)
                {
                    List<MessageExt> msgThis = new ArrayList<MessageExt>(consumeBatchSize);
                    for (int i = 0; i < consumeBatchSize; i++, total++)
                    {
                        if (total < msgs.Count())
                        {
                            msgThis.Add(msgs.Get(total));
                        }
                        else
                        {
                            break;
                        }
                    }

                    ConsumeRequest consumeRequest = new ConsumeRequest(msgThis, processQueue, messageQueue);
                    try
                    {
                        this.consumeExecutor.Submit(consumeRequest);
                    }
                    catch (Exception e)
                    {
                        for (; total < msgs.Count; total++)
                        {
                            msgThis.Add(msgs.Get(total));
                        }

                        this.submitConsumeRequestLater(consumeRequest);
                    }
                }
            }
        }


        private void cleanExpireMsg()
        {
            //Iterator<Map.Entry<MessageQueue, ProcessQueue>> it =
            //this.defaultMQPushConsumerImpl.getRebalanceImpl().getProcessQueueTable().entrySet().iterator();
            //while (it.hasNext())
            foreach (var entry in defaultMQPushConsumerImpl.getRebalanceImpl().getProcessQueueTable())
            {
                //Map.Entry<MessageQueue, ProcessQueue> next = it.next();
                ProcessQueue pq = entry.Value;
                pq.cleanExpiredMsg(this.defaultMQPushConsumer);
            }
        }

        private void processConsumeResult(ConsumeConcurrentlyStatus status, ConsumeConcurrentlyContext context, ConsumeRequest consumeRequest)
        {
            int ackIndex = context.getAckIndex();

            if (consumeRequest.getMsgs().IsEmpty())
                return;

            switch (status)
            {
                case ConsumeConcurrentlyStatus.CONSUME_SUCCESS:
                    if (ackIndex >= consumeRequest.getMsgs().Count)
                    {
                        ackIndex = consumeRequest.getMsgs().Count - 1;
                    }
                    int ok = ackIndex + 1;
                    int failed = consumeRequest.getMsgs().Count - ok;
                    this.getConsumerStatsManager().incConsumeOKTPS(consumerGroup, consumeRequest.getMessageQueue().getTopic(), ok);
                    this.getConsumerStatsManager().incConsumeFailedTPS(consumerGroup, consumeRequest.getMessageQueue().getTopic(), failed);
                    break;
                case ConsumeConcurrentlyStatus.RECONSUME_LATER:
                    ackIndex = -1;
                    this.getConsumerStatsManager().incConsumeFailedTPS(consumerGroup, consumeRequest.getMessageQueue().getTopic(),
                        consumeRequest.getMsgs().Count);
                    break;
                default:
                    break;
            }

            switch (this.defaultMQPushConsumer.getMessageModel())
            {
                case MessageModel.BROADCASTING:
                    for (int i = ackIndex + 1; i < consumeRequest.getMsgs().Count; i++)
                    {
                        MessageExt msg = consumeRequest.getMsgs().Get(i);
                        log.Warn("BROADCASTING, the message consume failed, drop it, {}", msg.ToString());
                    }
                    break;
                case MessageModel.CLUSTERING:
                    List<MessageExt> msgBackFailed = new ArrayList<MessageExt>(consumeRequest.getMsgs().Count);
                    for (int i = ackIndex + 1; i < consumeRequest.getMsgs().Count; i++)
                    {
                        MessageExt msg = consumeRequest.getMsgs().Get(i);
                        bool result = this.sendMessageBack(msg, context);
                        if (!result)
                        {
                            msg.setReconsumeTimes(msg.getReconsumeTimes() + 1);
                            msgBackFailed.Add(msg);
                        }
                    }

                    if (!msgBackFailed.IsEmpty())
                    {
                        //consumeRequest.getMsgs().RemoveAll(msgBackFailed);
                        consumeRequest.getMsgs().RemoveAll(it => msgBackFailed.Contains(it)); //???
                        this.submitConsumeRequestLater(msgBackFailed, consumeRequest.getProcessQueue(), consumeRequest.getMessageQueue());
                    }
                    break;
                default:
                    break;
            }

            long offset = consumeRequest.getProcessQueue().removeMessage(consumeRequest.getMsgs());
            if (offset >= 0 && !consumeRequest.getProcessQueue().isDropped())
            {
                this.defaultMQPushConsumerImpl.getOffsetStore().updateOffset(consumeRequest.getMessageQueue(), offset, true);
            }
        }

        public ConsumerStatsManager getConsumerStatsManager()
        {
            return this.defaultMQPushConsumerImpl.getConsumerStatsManager();
        }

        public bool sendMessageBack(MessageExt msg, ConsumeConcurrentlyContext context)
        {
            int delayLevel = context.getDelayLevelWhenNextConsume();

            // Wrap topic with namespace before sending back message.
            msg.setTopic(this.defaultMQPushConsumer.withNamespace(msg.getTopic()));
            try
            {
                this.defaultMQPushConsumerImpl.sendMessageBack(msg, delayLevel, context.getMessageQueue().getBrokerName());
                return true;
            }
            catch (Exception e)
            {
                log.Error("sendMessageBack exception, group: " + this.consumerGroup + " msg: " + msg.ToString(), e.ToString());
            }

            return false;
        }

        private void submitConsumeRequestLater(List<MessageExt> msgs, ProcessQueue processQueue, MessageQueue messageQueue)
        {

            this.scheduledExecutorService.Schedule(new Runnable()
            {
                //public void run()
                Run = () =>
            {
                /*ConsumeMessageConcurrentlyService.this.*/
                submitConsumeRequest(msgs, processQueue, messageQueue, true);
            }
            }, 5000);
        }

        private void submitConsumeRequestLater(ConsumeRequest consumeRequest)
        {

            this.scheduledExecutorService.Schedule(new Runnable()
            {
                //public void run()
                Run = () =>
                {
                    /*ConsumeMessageConcurrentlyService.this.*/
                    consumeExecutor.Submit(consumeRequest);
                }
            }, 5000);
        }

        class ConsumeRequest : IRunnable
        {

            private ConsumeMessageConcurrentlyService owner;
            public ConsumeRequest(ConsumeMessageConcurrentlyService owner)
            {
                this.owner = owner;
            }

            private readonly List<MessageExt> msgs;
            private readonly ProcessQueue processQueue;
            private readonly MessageQueue messageQueue;

            public ConsumeRequest(List<MessageExt> msgs, ProcessQueue processQueue, MessageQueue messageQueue)
            {
                this.msgs = msgs;
                this.processQueue = processQueue;
                this.messageQueue = messageQueue;
            }

            public List<MessageExt> getMsgs()
            {
                return msgs;
            }

            public ProcessQueue getProcessQueue()
            {
                return processQueue;
            }

            public void run()
            {
                if (this.processQueue.isDropped())
                {
                    log.Info("the message queue not be able to consume, because it's dropped. group={} {}", owner.consumerGroup, this.messageQueue);
                    return;
                }

                MessageListenerConcurrently listener = owner.messageListener;
                ConsumeConcurrentlyContext context = new ConsumeConcurrentlyContext(messageQueue);
                ConsumeConcurrentlyStatus status = ConsumeConcurrentlyStatus.UNKNOWN;//???
                owner.defaultMQPushConsumerImpl.resetRetryAndNamespace(msgs, owner.defaultMQPushConsumer.getConsumerGroup());

                ConsumeMessageContext consumeMessageContext = null;
                if (owner.defaultMQPushConsumerImpl.hasHook())
                {
                    consumeMessageContext = new ConsumeMessageContext();
                    consumeMessageContext.setNamespace(owner.defaultMQPushConsumer.getNamespace());
                    consumeMessageContext.setConsumerGroup(owner.defaultMQPushConsumer.getConsumerGroup());
                    consumeMessageContext.setProps(new HashMap<String, String>());
                    consumeMessageContext.setMq(messageQueue);
                    consumeMessageContext.setMsgList(msgs);
                    consumeMessageContext.setSuccess(false);
                    owner.defaultMQPushConsumerImpl.executeHookBefore(consumeMessageContext);
                }

                long beginTimestamp = Sys.currentTimeMillis();
                bool hasException = false;
                ConsumeReturnType returnType = ConsumeReturnType.SUCCESS;
                try
                {
                    if (msgs != null && !msgs.IsEmpty())
                    {
                        foreach (MessageExt msg in msgs)
                        {
                            MessageAccessor.setConsumeStartTimeStamp(msg, Sys.currentTimeMillis().ToString());
                        }
                    }
                    status = listener.consumeMessage(Collections.unmodifiableList(msgs), context);
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
                else if (ConsumeConcurrentlyStatus.RECONSUME_LATER == status)
                {
                    returnType = ConsumeReturnType.FAILED;
                }
                else if (ConsumeConcurrentlyStatus.CONSUME_SUCCESS == status)
                {
                    returnType = ConsumeReturnType.SUCCESS;
                }

                if (owner.defaultMQPushConsumerImpl.hasHook())
                {
                    consumeMessageContext.getProps().Put(MixAll.CONSUME_CONTEXT_TYPE, returnType.name());
                }

                if (null == status)
                {
                    log.Warn("consumeMessage return null, Group: {} Msgs: {} MQ: {}",
                        owner.consumerGroup,
                        msgs,
                        messageQueue);
                    status = ConsumeConcurrentlyStatus.RECONSUME_LATER;
                }

                if (owner.defaultMQPushConsumerImpl.hasHook())
                {
                    consumeMessageContext.setStatus(status.ToString());
                    consumeMessageContext.setSuccess(ConsumeConcurrentlyStatus.CONSUME_SUCCESS == status);
                    owner.defaultMQPushConsumerImpl.executeHookAfter(consumeMessageContext);
                }

                owner.getConsumerStatsManager().incConsumeRT(owner.consumerGroup, messageQueue.getTopic(), consumeRT);

                if (!processQueue.isDropped())
                {
                    owner.processConsumeResult(status, context, this);
                }
                else
                {
                    log.Warn("processQueue is dropped without process consume result. messageQueue={}, msgs={}", messageQueue, msgs);
                }
            }

            public MessageQueue getMessageQueue()
            {
                return messageQueue;
            }

        }
    }
}
