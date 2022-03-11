using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;

namespace RocketMQ.Client
{
    public abstract class RebalanceImpl
    {
        //protected static final InternalLogger log = ClientLogger.getLog();
        static NLog.Logger log = NLog.LogManager.GetCurrentClassLogger();
        protected readonly ConcurrentDictionary<MessageQueue, ProcessQueue>
            processQueueTable = new ConcurrentDictionary<MessageQueue, ProcessQueue>();
        public readonly ConcurrentDictionary<string/* topic */, HashSet<MessageQueue>> topicSubscribeInfoTable =
            new ConcurrentDictionary<string, HashSet<MessageQueue>>(); //public ???
        //protected readonly ConcurrentDictionary<string /* topic */, SubscriptionData> subscriptionInner =
        //    new ConcurrentDictionary<string, SubscriptionData>();
        public readonly ConcurrentDictionary<string /* topic */, SubscriptionData> subscriptionInner =
           new ConcurrentDictionary<string, SubscriptionData>();//public ???
        protected string consumerGroup;
        protected MessageModel messageModel;
        protected AllocateMessageQueueStrategy allocateMessageQueueStrategy;
        protected MQClientInstance mQClientFactory;

        public RebalanceImpl(String consumerGroup, MessageModel messageModel,
            AllocateMessageQueueStrategy allocateMessageQueueStrategy,
            MQClientInstance mQClientFactory)
        {
            this.consumerGroup = consumerGroup;
            this.messageModel = messageModel;
            this.allocateMessageQueueStrategy = allocateMessageQueueStrategy;
            this.mQClientFactory = mQClientFactory;
        }

        public void unlock(MessageQueue mq, bool oneway)
        {
            FindBrokerResult findBrokerResult = this.mQClientFactory.findBrokerAddressInSubscribe(mq.getBrokerName(), MixAll.MASTER_ID, true);
            if (findBrokerResult != null)
            {
                UnlockBatchRequestBody requestBody = new UnlockBatchRequestBody();
                requestBody.consumerGroup = this.consumerGroup;
                requestBody.clientId = this.mQClientFactory.getClientId();
                requestBody.mqSet.Add(mq);

                try
                {
                    this.mQClientFactory.getMQClientAPIImpl().unlockBatchMQ(findBrokerResult.getBrokerAddr(), requestBody, 1000, oneway);
                    log.Warn("unlock messageQueue. group:{}, clientId:{}, mq:{}",
                        this.consumerGroup,
                        this.mQClientFactory.getClientId(),
                        mq);
                }
                catch (Exception e)
                {
                    log.Error("unlockBatchMQ exception, " + mq, e.ToString());
                }
            }
        }

        public void unlockAll(bool oneway)
        {
            var brokerMqs = this.buildProcessQueueTableByBrokerName();
            foreach (var entry in brokerMqs)
            {
                string brokerName = entry.Key;
                HashSet<MessageQueue> mqs = entry.Value;
                if (mqs.IsEmpty())
                    continue;

                FindBrokerResult findBrokerResult = this.mQClientFactory.findBrokerAddressInSubscribe(brokerName, MixAll.MASTER_ID, true);
                if (findBrokerResult != null)
                {
                    UnlockBatchRequestBody requestBody = new UnlockBatchRequestBody();
                    requestBody.consumerGroup = this.consumerGroup;
                    requestBody.clientId = this.mQClientFactory.getClientId();
                    requestBody.mqSet = mqs;

                    try
                    {
                        this.mQClientFactory.getMQClientAPIImpl().unlockBatchMQ(findBrokerResult.getBrokerAddr(), requestBody, 1000, oneway);

                        foreach (MessageQueue mq in mqs)
                        {
                            ProcessQueue processQueue = this.processQueueTable.Get(mq);
                            if (processQueue != null)
                            {
                                processQueue.setLocked(false);
                                log.Info("the message queue unlock OK, Group: {} {}", this.consumerGroup, mq);
                            }
                        }
                    }
                    catch (Exception e)
                    {
                        log.Error("unlockBatchMQ exception, " + mqs, e.ToString());
                    }
                }
            }
        }

        private Dictionary<String/* brokerName */, HashSet<MessageQueue>> buildProcessQueueTableByBrokerName()
        {
            Dictionary<String, HashSet<MessageQueue>> result = new Dictionary<String, HashSet<MessageQueue>>();
            foreach (var keyValue in this.processQueueTable)
            {
                string brokerName = keyValue.Key.getBrokerName();
                result.TryGetValue(brokerName, out HashSet<MessageQueue> mqs);
                if (null == mqs)
                {
                    mqs = new HashSet<MessageQueue>();
                    //result.put(mq.getBrokerName(), mqs);
                    result[brokerName] = mqs;
                }
                mqs.Add(keyValue.Key);
            }
            return result;
        }

        /// <summary>
        /// lock //???
        /// </summary>
        public bool lockImpl(MessageQueue mq)
        {
            FindBrokerResult findBrokerResult = this.mQClientFactory.findBrokerAddressInSubscribe(mq.getBrokerName(), MixAll.MASTER_ID, true);
            if (findBrokerResult != null)
            {
                LockBatchRequestBody requestBody = new LockBatchRequestBody();
                requestBody.consumerGroup = this.consumerGroup;
                requestBody.clientId = this.mQClientFactory.getClientId();
                requestBody.mqSet.Add(mq);

                try
                {
                    HashSet<MessageQueue> lockedMq =
                        this.mQClientFactory.getMQClientAPIImpl().lockBatchMQ(findBrokerResult.getBrokerAddr(), requestBody, 1000);
                    foreach (MessageQueue mmqq in lockedMq)
                    {
                        ProcessQueue processQueue = this.processQueueTable.Get(mmqq);
                        if (processQueue != null)
                        {
                            processQueue.setLocked(true);
                            processQueue.LastLockTimestamp = Sys.currentTimeMillis();
                        }
                    }

                    bool lockOK = lockedMq.Contains(mq);
                    log.Info("the message queue lock {}, {} {}",
                        lockOK ? "OK" : "Failed",
                        this.consumerGroup,
                        mq);
                    return lockOK;
                }
                catch (Exception e)
                {
                    log.Error("lockBatchMQ exception, " + mq, e.ToString());
                }
            }

            return false;
        }

        public void lockAll()
        {
            var brokerMqs = this.buildProcessQueueTableByBrokerName();

            //Iterator<Entry<String, Set<MessageQueue>>> it = brokerMqs.entrySet().iterator();
            //while (it.hasNext())
            foreach (var entry in brokerMqs)
            {
                //Entry<String, Set<MessageQueue>> entry = it.next();
                string brokerName = entry.Key;
                HashSet<MessageQueue> mqs = entry.Value;

                if (mqs.IsEmpty())
                    continue;

                FindBrokerResult findBrokerResult = this.mQClientFactory.findBrokerAddressInSubscribe(brokerName, MixAll.MASTER_ID, true);
                if (findBrokerResult != null)
                {
                    LockBatchRequestBody requestBody = new LockBatchRequestBody();
                    requestBody.consumerGroup = this.consumerGroup;
                    requestBody.clientId = this.mQClientFactory.getClientId();
                    requestBody.mqSet = mqs;

                    try
                    {
                        HashSet<MessageQueue> lockOKMQSet =
                            this.mQClientFactory.getMQClientAPIImpl().lockBatchMQ(findBrokerResult.getBrokerAddr(), requestBody, 1000);

                        foreach (MessageQueue mq in lockOKMQSet)
                        {
                            ProcessQueue processQueue = this.processQueueTable.Get(mq);
                            if (processQueue != null)
                            {
                                if (!processQueue.isLocked())
                                {
                                    log.Info("the message queue locked OK, Group: {} {}", this.consumerGroup, mq);
                                }

                                processQueue.setLocked(true);
                                processQueue.LastLockTimestamp = Sys.currentTimeMillis();
                            }
                        }
                        foreach (MessageQueue mq in mqs)
                        {
                            if (!lockOKMQSet.Contains(mq))
                            {
                                ProcessQueue processQueue = this.processQueueTable.Get(mq);
                                if (processQueue != null)
                                {
                                    processQueue.setLocked(false);
                                    log.Warn("the message queue locked Failed, Group: {} {}", this.consumerGroup, mq);
                                }
                            }
                        }
                    }
                    catch (Exception e)
                    {
                        log.Error("lockBatchMQ exception, " + mqs, e.ToString());
                    }
                }
            }
        }

        public void doRebalance(bool isOrder)
        {
            var subTable = this.getSubscriptionInner();
            if (subTable != null)
            {
                foreach (var entry in subTable)
                {
                    string topic = entry.Key;
                    try
                    {
                        this.rebalanceByTopic(topic, isOrder);
                    }
                    catch (Exception e)
                    {
                        if (!topic.StartsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX))
                        {
                            log.Warn("rebalanceByTopic Exception", e.ToString());
                        }
                    }
                }
            }

            this.truncateMessageQueueNotMyTopic();
        }

        public ConcurrentDictionary<String, SubscriptionData> getSubscriptionInner()
        {
            return subscriptionInner;
        }

        private void rebalanceByTopic(String topic, bool isOrder)
        {
            switch (messageModel)
            {
                case MessageModel.BROADCASTING:
                    {
                        //HashSet<MessageQueue> mqSet = this.topicSubscribeInfoTable.get(topic);
                        topicSubscribeInfoTable.TryGetValue(topic, out HashSet<MessageQueue> mqSet);
                        if (mqSet != null)
                        {
                            bool changed = this.updateProcessQueueTableInRebalance(topic, mqSet, isOrder);
                            if (changed)
                            {
                                this.messageQueueChanged(topic, mqSet, mqSet);
                                log.Info("messageQueueChanged {} {} {} {}",
                                    consumerGroup,
                                    topic,
                                    mqSet,
                                    mqSet);
                            }
                        }
                        else
                        {
                            log.Warn("doRebalance, {}, but the topic[{}] not exist.", consumerGroup, topic);
                        }
                        break;
                    }
                case MessageModel.CLUSTERING:
                    {
                        //Set<MessageQueue> mqSet = this.topicSubscribeInfoTable.get(topic);
                        topicSubscribeInfoTable.TryGetValue(topic, out HashSet<MessageQueue> mqSet);
                        List<String> cidAll = this.mQClientFactory.findConsumerIdList(topic, consumerGroup);
                        if (null == mqSet)
                        {
                            if (!topic.StartsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX))
                            {
                                log.Warn("doRebalance, {}, but the topic[{}] not exist.", consumerGroup, topic);
                            }
                        }

                        if (null == cidAll)
                        {
                            log.Warn("doRebalance, {} {}, get consumer id list failed", consumerGroup, topic);
                        }

                        if (mqSet != null && cidAll != null)
                        {
                            List<MessageQueue> mqAll = new List<MessageQueue>();
                            //mqAll.addAll(mqSet);
                            mqAll.AddRange(mqSet);

                            //Collections.sort(mqAll);
                            //Collections.sort(cidAll);
                            mqAll.Sort();
                            cidAll.Sort();

                            AllocateMessageQueueStrategy strategy = this.allocateMessageQueueStrategy;

                            List<MessageQueue> allocateResult = null;
                            try
                            {
                                allocateResult = strategy.allocate(
                                    this.consumerGroup,
                                    this.mQClientFactory.getClientId(),
                                    mqAll,
                                    cidAll);
                            }
                            catch (Exception e)
                            {
                                log.Error("AllocateMessageQueueStrategy.allocate Exception. allocateMessageQueueStrategyName={}", strategy.getName(),
                                    e);
                                return;
                            }

                            HashSet<MessageQueue> allocateResultSet = new HashSet<MessageQueue>();
                            if (allocateResult != null)
                            {
                                //allocateResultSet.addAll(allocateResult);
                                allocateResultSet = allocateResultSet.Concat(allocateResult).ToHashSet();
                            }

                            bool changed = this.updateProcessQueueTableInRebalance(topic, allocateResultSet, isOrder);
                            if (changed)
                            {
                                log.Info(
                                    "rebalanced result changed. allocateMessageQueueStrategyName={}, group={}, topic={}, clientId={}, mqAllSize={}, cidAllSize={}, rebalanceResultSize={}, rebalanceResultSet={}",
                                    strategy.getName(), consumerGroup, topic, this.mQClientFactory.getClientId(), mqSet.Count, cidAll.Count,
                                    allocateResultSet.Count, allocateResultSet);
                                this.messageQueueChanged(topic, mqSet, allocateResultSet);
                            }
                        }
                        break;
                    }
                default:
                    break;
            }
        }

        private void truncateMessageQueueNotMyTopic()
        {
            var subTable = this.getSubscriptionInner();
            foreach (MessageQueue mq in this.processQueueTable.Keys)
            {
                if (!subTable.ContainsKey(mq.getTopic()))
                {

                    ProcessQueue pq = this.processQueueTable.JavaRemove(mq);
                    if (pq != null)
                    {
                        pq.setDropped(true);
                        log.Info("doRebalance, {}, truncateMessageQueueNotMyTopic remove unnecessary mq, {}", consumerGroup, mq);
                    }
                }
            }
        }

        private bool updateProcessQueueTableInRebalance(String topic, HashSet<MessageQueue> mqSet, bool isOrder)
        {
            bool changed = false;

            //Iterator<Entry<MessageQueue, ProcessQueue>> it = this.processQueueTable.entrySet().iterator();
            //while (it.hasNext())
            foreach (var entry in this.processQueueTable)
            {
                //Entry<MessageQueue, ProcessQueue> next = it.next();
                MessageQueue mq = entry.Key;
                ProcessQueue pq = entry.Value;

                if (mq.getTopic().Equals(topic))
                {
                    if (!mqSet.Contains(mq))
                    {
                        pq.setDropped(true);
                        if (this.removeUnnecessaryMessageQueue(mq, pq))
                        {
                            //it.remove();
                            processQueueTable.JavaRemove(entry.Key);
                            changed = true;
                            log.Info("doRebalance, {}, remove unnecessary mq, {}", consumerGroup, mq);
                        }
                    }
                    else if (pq.isPullExpired())
                    {
                        switch (this.consumeType())
                        {
                            case ConsumeType.CONSUME_ACTIVELY:
                                break;
                            case ConsumeType.CONSUME_PASSIVELY:
                                pq.setDropped(true);
                                if (this.removeUnnecessaryMessageQueue(mq, pq))
                                {
                                    //it.remove();
                                    processQueueTable.JavaRemove(entry.Key);
                                    changed = true;
                                    log.Error("[BUG]doRebalance, {}, remove unnecessary mq, {}, because pull is pause, so try to fixed it",
                                        consumerGroup, mq);
                                }
                                break;
                            default:
                                break;
                        }
                    }
                }
            }

            List<PullRequest> pullRequestList = new List<PullRequest>();
            foreach (MessageQueue mq in mqSet)
            {
                if (!this.processQueueTable.ContainsKey(mq))
                {
                    if (isOrder && !this.lockImpl(mq))
                    {
                        log.Warn("doRebalance, {}, add a new mq failed, {}, because lock failed", consumerGroup, mq);
                        continue;
                    }

                    this.removeDirtyOffset(mq);
                    ProcessQueue pq = new ProcessQueue();

                    long nextOffset = -1L;
                    try
                    {
                        nextOffset = this.computePullFromWhereWithException(mq);
                    }
                    catch (Exception e)
                    {
                        log.Info("doRebalance, {}, compute offset failed, {}", consumerGroup, mq);
                        continue;
                    }

                    if (nextOffset >= 0)
                    {
                        ProcessQueue pre = this.processQueueTable.PutIfAbsent(mq, pq);
                        if (pre != null)
                        {
                            log.Info("doRebalance, {}, mq already exists, {}", consumerGroup, mq);
                        }
                        else
                        {
                            log.Info("doRebalance, {}, add a new mq, {}", consumerGroup, mq);
                            PullRequest pullRequest = new PullRequest();
                            pullRequest.setConsumerGroup(consumerGroup);
                            pullRequest.setNextOffset(nextOffset);
                            pullRequest.setMessageQueue(mq);
                            pullRequest.setProcessQueue(pq);
                            pullRequestList.Add(pullRequest);
                            changed = true;
                        }
                    }
                    else
                    {
                        log.Warn("doRebalance, {}, add new mq failed, {}", consumerGroup, mq);
                    }
                }
            }

            this.dispatchPullRequest(pullRequestList);

            return changed;
        }

        public abstract void messageQueueChanged(String topic, HashSet<MessageQueue> mqAll,
            HashSet<MessageQueue> mqDivided);

        public abstract bool removeUnnecessaryMessageQueue(MessageQueue mq, ProcessQueue pq);

        public abstract ConsumeType consumeType();

        public abstract void removeDirtyOffset(MessageQueue mq);

        /**
         * When the network is unstable, using this interface may return wrong offset.
         * It is recommended to use computePullFromWhereWithException instead.
         * @param mq
         * @return offset
         */
        [Obsolete]//@Deprecated
        public abstract long computePullFromWhere(MessageQueue mq);

        public abstract long computePullFromWhereWithException(MessageQueue mq);

        public abstract void dispatchPullRequest(List<PullRequest> pullRequestList);

        public void removeProcessQueue(MessageQueue mq)
        {
            //ProcessQueue prev = this.processQueueTable.remove(mq);
            processQueueTable.TryRemove(mq, out ProcessQueue prev);
            if (prev != null)
            {
                bool droped = prev.isDropped();
                prev.setDropped(true);
                this.removeUnnecessaryMessageQueue(mq, prev);
                log.Info("Fix Offset, {}, remove unnecessary mq, {} Droped: {}", consumerGroup, mq, droped);
            }
        }

        public ConcurrentDictionary<MessageQueue, ProcessQueue> getProcessQueueTable()
        {
            return processQueueTable;
        }

        public ConcurrentDictionary<String, HashSet<MessageQueue>> getTopicSubscribeInfoTable()
        {
            return topicSubscribeInfoTable;
        }

        public string getConsumerGroup()
        {
            return consumerGroup;
        }

        public void setConsumerGroup(String consumerGroup)
        {
            this.consumerGroup = consumerGroup;
        }

        public MessageModel getMessageModel()
        {
            return messageModel;
        }

        public void setMessageModel(MessageModel messageModel)
        {
            this.messageModel = messageModel;
        }

        public AllocateMessageQueueStrategy getAllocateMessageQueueStrategy()
        {
            return allocateMessageQueueStrategy;
        }

        public void setAllocateMessageQueueStrategy(AllocateMessageQueueStrategy allocateMessageQueueStrategy)
        {
            this.allocateMessageQueueStrategy = allocateMessageQueueStrategy;
        }

        public MQClientInstance getmQClientFactory()
        {
            return mQClientFactory;
        }

        public void setmQClientFactory(MQClientInstance mQClientFactory)
        {
            this.mQClientFactory = mQClientFactory;
        }

        public void destroy()
        {
            //Iterator<Entry<MessageQueue, ProcessQueue>> it = this.processQueueTable.entrySet().iterator();
            //while (it.hasNext())
            foreach (var entry in processQueueTable)
            {
                //Entry<MessageQueue, ProcessQueue> next = it.next();
                entry.Value.setDropped(true);
            }

            this.processQueueTable.Clear();
        }
    }
}
