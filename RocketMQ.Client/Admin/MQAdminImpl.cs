using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace RocketMQ.Client
{
    public class MQAdminImpl
    {
        //private final InternalLogger log = ClientLogger.getLog();
        static NLog.Logger log = NLog.LogManager.GetCurrentClassLogger();
        private readonly MQClientInstance mQClientFactory;
        private int timeoutMillis = 6000;

        public MQAdminImpl(MQClientInstance mQClientFactory)
        {
            this.mQClientFactory = mQClientFactory;
        }

        public int getTimeoutMillis()
        {
            return timeoutMillis;
        }

        public void setTimeoutMillis(int timeoutMillis)
        {
            this.timeoutMillis = timeoutMillis;
        }

        ///<exception cref="MQClientException"/>
        public void createTopic(String key, String newTopic, int queueNum)
        {
            createTopic(key, newTopic, queueNum, 0);
        }

        ///<exception cref="MQClientException"/>
        public void createTopic(String key, String newTopic, int queueNum, int topicSysFlag)
        {
            try
            {
                Validators.checkTopic(newTopic);
                Validators.isSystemTopic(newTopic);
                TopicRouteData topicRouteData = this.mQClientFactory.getMQClientAPIImpl().getTopicRouteInfoFromNameServer(key, timeoutMillis);
                List<BrokerData> brokerDataList = topicRouteData.brokerDatas;
                if (brokerDataList != null && !brokerDataList.isEmpty())
                {
                    //Collections.sort(brokerDataList);
                    brokerDataList.Sort();

                    bool createOKAtLeastOnce = false;
                    MQClientException exception = null;

                    StringBuilder orderTopicString = new StringBuilder();

                    foreach (BrokerData brokerData in brokerDataList)
                    {
                        //String addr = brokerData.getBrokerAddrs().get(MixAll.MASTER_ID);
                        brokerData.brokerAddrs.TryGetValue(MixAll.MASTER_ID, out string addr);
                        if (addr != null)
                        {
                            TopicConfig topicConfig = new TopicConfig(newTopic);
                            topicConfig.setReadQueueNums(queueNum);
                            topicConfig.setWriteQueueNums(queueNum);
                            topicConfig.setTopicSysFlag(topicSysFlag);

                            bool createOK = false;
                            for (int i = 0; i < 5; i++)
                            {
                                try
                                {
                                    this.mQClientFactory.getMQClientAPIImpl().createTopic(addr, key, topicConfig, timeoutMillis);
                                    createOK = true;
                                    createOKAtLeastOnce = true;
                                    break;
                                }
                                catch (Exception e)
                                {
                                    if (4 == i)
                                    {
                                        exception = new MQClientException("create topic to broker exception", e);
                                    }
                                }
                            }

                            if (createOK)
                            {
                                orderTopicString.Append(brokerData.brokerName);
                                orderTopicString.Append(":");
                                orderTopicString.Append(queueNum);
                                orderTopicString.Append(";");
                            }
                        }
                    }

                    if (exception != null && !createOKAtLeastOnce)
                    {
                        throw exception;
                    }
                }
                else
                {
                    throw new MQClientException("Not found broker, maybe key is wrong", null);
                }
            }
            catch (Exception e)
            {
                throw new MQClientException("create new topic failed", e);
            }
        }

        ///<exception cref="MQClientException"/>
        public List<MessageQueue> fetchPublishMessageQueues(String topic)
        {
            try
            {
                TopicRouteData topicRouteData = this.mQClientFactory.getMQClientAPIImpl().getTopicRouteInfoFromNameServer(topic, timeoutMillis);
                if (topicRouteData != null)
                {
                    TopicPublishInfo topicPublishInfo = MQClientInstance.topicRouteData2TopicPublishInfo(topic, topicRouteData);
                    if (topicPublishInfo != null && topicPublishInfo.ok())
                    {
                        return parsePublishMessageQueues(topicPublishInfo.getMessageQueueList());
                    }
                }
            }
            catch (Exception e)
            {
                throw new MQClientException("Can not find Message Queue for this topic, " + topic, e);
            }

            throw new MQClientException("Unknow why, Can not find Message Queue for this topic, " + topic, null);
        }

        public List<MessageQueue> parsePublishMessageQueues(List<MessageQueue> messageQueueList)
        {
            List<MessageQueue> resultQueues = new List<MessageQueue>();
            foreach (MessageQueue queue in messageQueueList)
            {
                String userTopic = NamespaceUtil.withoutNamespace(queue.getTopic(), this.mQClientFactory.getClientConfig().getNamespace());
                resultQueues.Add(new MessageQueue(userTopic, queue.getBrokerName(), queue.getQueueId()));
            }

            return resultQueues;
        }

        ///<exception cref="MQClientException"/>
        public HashSet<MessageQueue> fetchSubscribeMessageQueues(String topic)
        {
            try
            {
                TopicRouteData topicRouteData = this.mQClientFactory.getMQClientAPIImpl().getTopicRouteInfoFromNameServer(topic, timeoutMillis);
                if (topicRouteData != null)
                {
                    HashSet<MessageQueue> mqList = MQClientInstance.topicRouteData2TopicSubscribeInfo(topic, topicRouteData);
                    if (!mqList.isEmpty())
                    {
                        return mqList;
                    }
                    else
                    {
                        throw new MQClientException("Can not find Message Queue for this topic, " + topic + " Namesrv return empty", null);
                    }
                }
            }
            catch (Exception e)
            {
                throw new MQClientException(
                    "Can not find Message Queue for this topic, " + topic + FAQUrl.suggestTodo(FAQUrl.MQLIST_NOT_EXIST),
                    e);
            }

            throw new MQClientException("Unknow why, Can not find Message Queue for this topic, " + topic, null);
        }

        ///<exception cref="MQClientException"/>
        public long searchOffset(MessageQueue mq, long timestamp)
        {
            String brokerAddr = this.mQClientFactory.findBrokerAddressInPublish(mq.getBrokerName());
            if (null == brokerAddr)
            {
                this.mQClientFactory.updateTopicRouteInfoFromNameServer(mq.getTopic());
                brokerAddr = this.mQClientFactory.findBrokerAddressInPublish(mq.getBrokerName());
            }

            if (brokerAddr != null)
            {
                try
                {
                    return this.mQClientFactory.getMQClientAPIImpl().searchOffset(brokerAddr, mq.getTopic(), mq.getQueueId(), timestamp,
                        timeoutMillis);
                }
                catch (Exception e)
                {
                    throw new MQClientException("Invoke Broker[" + brokerAddr + "] exception", e);
                }
            }

            throw new MQClientException("The broker[" + mq.getBrokerName() + "] not exist", null);
        }

        ///<exception cref="MQClientException"/>
        public long maxOffset(MessageQueue mq)
        {
            String brokerAddr = this.mQClientFactory.findBrokerAddressInPublish(mq.getBrokerName());
            if (null == brokerAddr)
            {
                this.mQClientFactory.updateTopicRouteInfoFromNameServer(mq.getTopic());
                brokerAddr = this.mQClientFactory.findBrokerAddressInPublish(mq.getBrokerName());
            }

            if (brokerAddr != null)
            {
                try
                {
                    return this.mQClientFactory.getMQClientAPIImpl().getMaxOffset(brokerAddr, mq.getTopic(), mq.getQueueId(), timeoutMillis);
                }
                catch (Exception e)
                {
                    throw new MQClientException("Invoke Broker[" + brokerAddr + "] exception", e);
                }
            }

            throw new MQClientException("The broker[" + mq.getBrokerName() + "] not exist", null);
        }
        ///<exception cref="MQClientException"/>
        public long minOffset(MessageQueue mq)
        {
            String brokerAddr = this.mQClientFactory.findBrokerAddressInPublish(mq.getBrokerName());
            if (null == brokerAddr)
            {
                this.mQClientFactory.updateTopicRouteInfoFromNameServer(mq.getTopic());
                brokerAddr = this.mQClientFactory.findBrokerAddressInPublish(mq.getBrokerName());
            }

            if (brokerAddr != null)
            {
                try
                {
                    return this.mQClientFactory.getMQClientAPIImpl().getMinOffset(brokerAddr, mq.getTopic(), mq.getQueueId(), timeoutMillis);
                }
                catch (Exception e)
                {
                    throw new MQClientException("Invoke Broker[" + brokerAddr + "] exception", e);
                }
            }

            throw new MQClientException("The broker[" + mq.getBrokerName() + "] not exist", null);
        }

        ///<exception cref="MQClientException"/>
        public long earliestMsgStoreTime(MessageQueue mq)
        {
            String brokerAddr = this.mQClientFactory.findBrokerAddressInPublish(mq.getBrokerName());
            if (null == brokerAddr)
            {
                this.mQClientFactory.updateTopicRouteInfoFromNameServer(mq.getTopic());
                brokerAddr = this.mQClientFactory.findBrokerAddressInPublish(mq.getBrokerName());
            }

            if (brokerAddr != null)
            {
                try
                {
                    return this.mQClientFactory.getMQClientAPIImpl().getEarliestMsgStoretime(brokerAddr, mq.getTopic(), mq.getQueueId(),
                        timeoutMillis);
                }
                catch (Exception e)
                {
                    throw new MQClientException("Invoke Broker[" + brokerAddr + "] exception", e);
                }
            }

            throw new MQClientException("The broker[" + mq.getBrokerName() + "] not exist", null);
        }

        ///<exception cref="RemotingException"/>
        ///<exception cref="MQBrokerException"/>
        ///<exception cref="InterruptedException"/>
        ///<exception cref="MQClientException"/>
        public MessageExt viewMessage(
        String msgId)
        {

            MessageId messageId = null;
            try
            {
                messageId = MessageDecoder.decodeMessageId(msgId);
            }
            catch (Exception e)
            {
                throw new MQClientException(ResponseCode.NO_MESSAGE, "query message by id finished, but no message.");
            }
            return this.mQClientFactory.getMQClientAPIImpl().viewMessage(RemotingUtil.socketAddress2String(messageId.getAddress()),
                messageId.getOffset(), timeoutMillis);
        }

        ///<exception cref="MQClientException"/>
        ///<exception cref="InterruptedException"/>
        public QueryResult queryMessage(String topic, String key, int maxNum, long begin, long end)
        {

            return queryMessage(topic, key, maxNum, begin, end, false);
        }

        ///<exception cref="MQClientException"/>
        ///<exception cref="InterruptedException"/>
        public QueryResult queryMessageByUniqKey(String topic, String uniqKey, int maxNum, long begin, long end)
        {

            return queryMessage(topic, uniqKey, maxNum, begin, end, true);
        }

        ///<exception cref="MQClientException"/>
        ///<exception cref="InterruptedException"/>
        public MessageExt queryMessageByUniqKey(String topic, String uniqKey)
        {

            QueryResult qr = queryMessageByUniqKey(topic, uniqKey, 32,
                    MessageClientIDSetter.getNearlyTimeFromID(uniqKey) - 1000, long.MaxValue);
            if (qr != null && qr.getMessageList() != null && qr.getMessageList().Count > 0)
            {
                //return qr.getMessageList()[0];
                return qr.getMessageList().getFirst();
            }
            else
            {
                return null;
            }
        }

        ///<exception cref="MQClientException"/>
        ///<exception cref="InterruptedException"/>
        protected QueryResult queryMessage(String topic, String key, int maxNum, long begin, long end, bool isUniqKey)
        {
            TopicRouteData topicRouteData = this.mQClientFactory.getAnExistTopicRouteData(topic);
            if (null == topicRouteData)
            {
                this.mQClientFactory.updateTopicRouteInfoFromNameServer(topic);
                topicRouteData = this.mQClientFactory.getAnExistTopicRouteData(topic);
            }

            if (topicRouteData != null)
            {
                List<String> brokerAddrs = new List<String>();
                foreach (BrokerData brokerData in topicRouteData.brokerDatas)
                {
                    String addr = brokerData.selectBrokerAddr();
                    if (addr != null)
                    {
                        brokerAddrs.Add(addr);
                    }
                }

                if (!brokerAddrs.isEmpty())
                {
                    //CountDownLatch countDownLatch = new CountDownLatch(brokerAddrs.Count);
                    CountdownEvent countDownLatch = new CountdownEvent(brokerAddrs.Count);
                    LinkedList<QueryResult> queryResultList = new LinkedList<QueryResult>();
                    //ReadWriteLock rwLock = new ReentrantReadWriteLock(false); //不公平
                    ReaderWriterLockSlim rwLock = new ReaderWriterLockSlim();

                    foreach (String addr in brokerAddrs)
                    {
                        try
                        {
                            QueryMessageRequestHeader requestHeader = new QueryMessageRequestHeader();
                            requestHeader.topic = topic;
                            requestHeader.key = key;
                            requestHeader.maxNum = maxNum;
                            requestHeader.beginTimestamp = begin;
                            requestHeader.endTimestamp = end;

                            this.mQClientFactory.getMQClientAPIImpl().queryMessage(addr, requestHeader, timeoutMillis * 3,
                                new InvokeCallback()
                                {
                                    //@Override
                                    //public void operationComplete(ResponseFuture responseFuture)
                                    OperationComplete = (responseFuture) =>
                                    {
                                        try
                                        {
                                            RemotingCommand response = responseFuture.getResponseCommand();
                                            if (response != null)
                                            {
                                                switch (response.getCode())
                                                {
                                                    case ResponseCode.SUCCESS:
                                                        {
                                                            QueryMessageResponseHeader responseHeader = null;
                                                            try
                                                            {
                                                        //        responseHeader =
                                                        //(QueryMessageResponseHeader)response
                                                        //    .decodeCommandCustomHeader(QueryMessageResponseHeader/*.class*/);

                                                                responseHeader = response.decodeCommandCustomHeader<QueryMessageResponseHeader>();
                                                            }
                                                            catch (RemotingCommandException e)
                                                            {
                                                                log.Error("decodeCommandCustomHeader exception", e);
                                                                return;
                                                            }

                                                            List<MessageExt> wrappers =
                                        MessageDecoder.decodes(ByteBuffer.wrap(response.getBody()), true);

                                                            QueryResult qr = new QueryResult(responseHeader.indexLastUpdateTimestamp, wrappers);
                                                            try
                                                            {
                                                                //rwLock.writeLock().lock000();
                                                                rwLock.EnterWriteLock();
                                                                //queryResultList.Add(qr);
                                                                queryResultList.AddLast(qr);
                                                            }
                                                            finally
                                                            {
                                                                //rwLock.writeLock().unlock();
                                                                rwLock.ExitWriteLock();
                                                            }
                                                            break;
                                                        }
                                                    default:
                                                        log.Warn("getResponseCommand failed, {} {}", response.getCode(), response.getRemark());
                                                        break;
                                                }
                                            }
                                            else
                                            {
                                                log.Warn("getResponseCommand return null");
                                            }
                                        }
                                        finally
                                        {
                                            //countDownLatch.countDown();
                                            countDownLatch.Signal();
                                        }
                                    }
                                }, isUniqKey);
                        }
                        catch (Exception e)
                        {
                            log.Warn("queryMessage exception", e.ToString());
                        }

                    }

                    //bool ok = countDownLatch.await(timeoutMillis * 4);
                    bool ok = countDownLatch.Wait(timeoutMillis * 4);
                    if (!ok)
                    {
                        log.Warn("queryMessage, maybe some broker failed");
                    }

                    long indexLastUpdateTimestamp = 0;
                    LinkedList<MessageExt> messageList = new LinkedList<MessageExt>();
                    //List<MessageExt> messageList = new List<MessageExt>();
                    foreach (QueryResult qr in queryResultList)
                    {
                        if (qr.getIndexLastUpdateTimestamp() > indexLastUpdateTimestamp)
                        {
                            indexLastUpdateTimestamp = qr.getIndexLastUpdateTimestamp();
                        }

                        foreach (MessageExt msgExt in qr.getMessageList())
                        {
                            if (isUniqKey)
                            {
                                if (msgExt.getMsgId().Equals(key))
                                {

                                    if (messageList.Count > 0)
                                    {

                                        //if (messageList[0].getStoreTimestamp() > msgExt.getStoreTimestamp())
                                        if (messageList.First.Value.getStoreTimestamp() > msgExt.getStoreTimestamp())
                                        {
                                            messageList.Clear();
                                            //messageList.Add(msgExt);
                                            messageList.AddLast(msgExt);
                                        }

                                    }
                                    else
                                    {

                                        messageList.AddLast(msgExt);
                                    }
                                }
                                else
                                {
                                    log.Warn("queryMessage by uniqKey, find message key not matched, maybe hash duplicate {}", msgExt.ToString());
                                }
                            }
                            else
                            {
                                String keys = msgExt.getKeys();
                                String msgTopic = msgExt.getTopic();
                                if (keys != null)
                                {
                                    bool matched = false;
                                    String[] keyArray = keys.Split(MessageConst.KEY_SEPARATOR);
                                    if (keyArray != null)
                                    {
                                        foreach (String k in keyArray)
                                        {
                                            // both topic and key must be equal at the same time
                                            //if (Objects.Equals(key, k) && Objects.Equals(topic, msgTopic))
                                            if (object.Equals(key, k) && object.Equals(topic, msgTopic))
                                            {
                                                matched = true;
                                                break;
                                            }
                                        }
                                    }

                                    if (matched)
                                    {
                                        //messageList.Add(msgExt);
                                        messageList.AddLast(msgExt);
                                    }
                                    else
                                    {
                                        log.Warn("queryMessage, find message key not matched, maybe hash duplicate {}", msgExt.ToString());
                                    }
                                }
                            }
                        }
                    }

                    //If namespace not null , reset Topic without namespace.
                    foreach (MessageExt messageExt in messageList)
                    {
                        if (null != this.mQClientFactory.getClientConfig().getNamespace())
                        {
                            messageExt.setTopic(NamespaceUtil.withoutNamespace(messageExt.getTopic(), this.mQClientFactory.getClientConfig().getNamespace()));
                        }
                    }

                    if (!messageList.isEmpty())
                    {
                        return new QueryResult(indexLastUpdateTimestamp, messageList);
                    }
                    else
                    {
                        throw new MQClientException(ResponseCode.NO_MESSAGE, "query message by key finished, but no message.");
                    }
                }
            }

            throw new MQClientException(ResponseCode.TOPIC_NOT_EXIST, "The topic[" + topic + "] not matched route info");
        }
    }
}
