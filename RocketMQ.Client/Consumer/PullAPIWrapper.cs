using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;

namespace RocketMQ.Client
{
    public class PullAPIWrapper
    {
        //private final InternalLogger log = ClientLogger.getLog();
        static NLog.Logger log = NLog.LogManager.GetCurrentClassLogger();
        private readonly MQClientInstance mQClientFactory;
        private readonly string consumerGroup;
        private readonly bool unitMode;
        //private ConcurrentDictionary<MessageQueue, AtomicLong/* brokerId */> pullFromWhichNodeTable =
            //new ConcurrentDictionary<MessageQueue, AtomicLong>(32);
        private ConcurrentDictionary<MessageQueue, AtomicLong/* brokerId */> pullFromWhichNodeTable =
            new ConcurrentDictionary<MessageQueue, AtomicLong>();
        private /*volatile*/ bool connectBrokerByUser = false;
        private /*volatile*/ long defaultBrokerId = MixAll.MASTER_ID;
        private Random random = new Random(TimeUtils.CurrentTimeSecondUTC());
        private List<FilterMessageHook> filterMessageHookList = new List<FilterMessageHook>();

        public PullAPIWrapper(MQClientInstance mQClientFactory, string consumerGroup, bool unitMode)
        {
            this.mQClientFactory = mQClientFactory;
            this.consumerGroup = consumerGroup;
            this.unitMode = unitMode;
        }

        public PullResult processPullResult(MessageQueue mq, PullResult pullResult, SubscriptionData subscriptionData)
        {
            PullResultExt pullResultExt = (PullResultExt)pullResult;

            this.updatePullFromWhichNode(mq, pullResultExt.getSuggestWhichBrokerId());
            if (PullStatus.FOUND == pullResult.getPullStatus())
            {
                //ByteBuffer byteBuffer = ByteBuffer.wrap(pullResultExt.getMessageBinary());
                ByteBuffer byteBuffer = ByteBuffer.wrap(pullResultExt.getMessageBinary());
                List<MessageExt> msgList = MessageDecoder.decodes(byteBuffer);

                List<MessageExt> msgListFilterAgain = msgList;
                if (subscriptionData.tagsSet.Count > 0 && !subscriptionData.classFilterMode)
                {
                    msgListFilterAgain = new List<MessageExt>(msgList.Count);
                    foreach (MessageExt msg in msgList)
                    {
                        if (msg.getTags() != null)
                        {
                            if (subscriptionData.tagsSet.Contains(msg.getTags()))
                            {
                                msgListFilterAgain.Add(msg);
                            }
                        }
                    }
                }

                if (this.hasHook())
                {
                    FilterMessageContext filterMessageContext = new FilterMessageContext();
                    filterMessageContext.setUnitMode(unitMode);
                    filterMessageContext.setMsgList(msgListFilterAgain);
                    this.executeHook(filterMessageContext);
                }

                foreach (MessageExt msg in msgListFilterAgain)
                {
                    string traFlag = msg.getProperty(MessageConst.PROPERTY_TRANSACTION_PREPARED);
                    if (bool.Parse(traFlag))
                    {
                        msg.setTransactionId(msg.getProperty(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX));
                    }
                    MessageAccessor.putProperty(msg, MessageConst.PROPERTY_MIN_OFFSET,
                        pullResult.getMinOffset().ToString());
                    MessageAccessor.putProperty(msg, MessageConst.PROPERTY_MAX_OFFSET,
                        pullResult.getMaxOffset().ToString());
                    msg.setBrokerName(mq.getBrokerName());
                }

                pullResultExt.setMsgFoundList(msgListFilterAgain);
            }

            pullResultExt.setMessageBinary(null);

            return pullResult;
        }

        public void updatePullFromWhichNode(MessageQueue mq, long brokerId)
        {
            //AtomicLong suggest = this.pullFromWhichNodeTable.Get(mq);
            pullFromWhichNodeTable.TryGetValue(mq, out AtomicLong suggest);
            if (null == suggest)
            {
                //this.pullFromWhichNodeTable.put(mq, new AtomicLong(brokerId));
                this.pullFromWhichNodeTable[mq] = new AtomicLong(brokerId);
            }
            else
            {
                suggest.Set(brokerId);
            }
        }

        public bool hasHook()
        {
            return filterMessageHookList.Count > 0;
        }

        public void executeHook(FilterMessageContext context)
        {
            if (this.filterMessageHookList.Count > 0)
            {
                foreach (FilterMessageHook hook in this.filterMessageHookList)
                {
                    try
                    {
                        hook.filterMessage(context);
                    }
                    catch (Exception e)
                    {
                        log.Error("execute hook error. hookName={}", hook.hookName());
                    }
                }
            }
        }

        public PullResult pullKernelImpl(
            MessageQueue mq,
            string subExpression,
            string expressionType,
            long subVersion,
            long offset,
            int maxNums,
            int sysFlag,
            long commitOffset,
            long brokerSuspendMaxTimeMillis,
            long timeoutMillis,
            CommunicationMode communicationMode,
            PullCallback pullCallback

        )
        {
            FindBrokerResult findBrokerResult =
                this.mQClientFactory.findBrokerAddressInSubscribe(mq.getBrokerName(),
                    this.recalculatePullFromWhichNode(mq), false);
            if (null == findBrokerResult)
            {
                this.mQClientFactory.updateTopicRouteInfoFromNameServer(mq.getTopic());
                findBrokerResult =
                    this.mQClientFactory.findBrokerAddressInSubscribe(mq.getBrokerName(),
                        this.recalculatePullFromWhichNode(mq), false);
            }

            if (findBrokerResult != null)
            {
                {
                    // check version
                    if (!ExpressionType.isTagType(expressionType)
                        && findBrokerResult.getBrokerVersion() < (int)MQVersion.Version.V4_1_0_SNAPSHOT)
                    {
                        throw new MQClientException("The broker[" + mq.getBrokerName() + ", "
                            + findBrokerResult.getBrokerVersion() + "] does not upgrade to support for filter message by " + expressionType, null);
                    }
                }
                int sysFlagInner = sysFlag;

                if (findBrokerResult.isSlave())
                {
                    sysFlagInner = PullSysFlag.clearCommitOffsetFlag(sysFlagInner);
                }

                PullMessageRequestHeader requestHeader = new PullMessageRequestHeader();
                requestHeader.consumerGroup = this.consumerGroup;
                requestHeader.topic = mq.getTopic();
                requestHeader.queueId = mq.getQueueId();
                requestHeader.queueOffset = offset;
                requestHeader.maxMsgNums = maxNums;
                requestHeader.sysFlag = sysFlagInner;
                requestHeader.commitOffset = commitOffset;
                requestHeader.suspendTimeoutMillis = brokerSuspendMaxTimeMillis;
                requestHeader.subscription = subExpression;
                requestHeader.subVersion = subVersion;
                requestHeader.expressionType = expressionType;

                string brokerAddr = findBrokerResult.getBrokerAddr();
                if (PullSysFlag.hasClassFilterFlag(sysFlagInner))
                {
                    brokerAddr = computePullFromWhichFilterServer(mq.getTopic(), brokerAddr);
                }

                PullResult pullResult = this.mQClientFactory.getMQClientAPIImpl().pullMessage(
                    brokerAddr,
                    requestHeader,
                    timeoutMillis,
                    communicationMode,
                    pullCallback);

                return pullResult;
            }

            throw new MQClientException("The broker[" + mq.getBrokerName() + "] not exist", null);
        }

        public long recalculatePullFromWhichNode(MessageQueue mq)
        {
            if (this.isConnectBrokerByUser())
            {
                return getDefaultBrokerId();   //小心只能通过Volatile读取
            }

            AtomicLong suggest = this.pullFromWhichNodeTable.Get(mq);
            if (suggest != null)
            {
                return suggest.Get();
            }

            return MixAll.MASTER_ID;
        }

        private string computePullFromWhichFilterServer(String topic, string brokerAddr)
        {
            ConcurrentDictionary<String, TopicRouteData> topicRouteTable = this.mQClientFactory.getTopicRouteTable();
            if (topicRouteTable != null)
            {
                TopicRouteData topicRouteData = topicRouteTable.Get(topic);
                List<String> list = topicRouteData.filterServerTable.Get(brokerAddr);

                if (list != null && list.Count > 0)
                {
                    //return list.get(randomNum() % list.Count);
                    return list[randomNum() % list.Count];
                }
            }

            throw new MQClientException("Find Filter Server Failed, Broker Addr: " + brokerAddr + " topic: "
                + topic, null);
        }

        public bool isConnectBrokerByUser()
        {
            //return connectBrokerByUser;
            return Volatile.Read(ref connectBrokerByUser);
        }

        public void setConnectBrokerByUser(bool byUser)
        {
            //this.connectBrokerByUser = connectBrokerByUser;
            Volatile.Write(ref connectBrokerByUser, byUser);
        }

        public int randomNum()
        {
            //java api
            //public int nextInt() { return next(32); }
            //int value = random.nextInt();
            int value = random.Next(32); //
            if (value < 0)
            {
                value = Math.Abs(value);
                if (value < 0)
                    value = 0;
            }
            return value;
        }

        public void registerFilterMessageHook(List<FilterMessageHook> filterMessageHookList)
        {
            this.filterMessageHookList = filterMessageHookList;
        }

        public long getDefaultBrokerId()
        {
            //return defaultBrokerId;
            return Volatile.Read(ref defaultBrokerId);
        }

        public void setDefaultBrokerId(long brokerId)
        {
            //this.defaultBrokerId = defaultBrokerId;
            Volatile.Write(ref defaultBrokerId, brokerId);
        }
    }
}
