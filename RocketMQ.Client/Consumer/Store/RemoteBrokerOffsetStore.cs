using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RocketMQ.Client//.Consumer.Store
{
    public class RemoteBrokerOffsetStore : OffsetStore
    {
        //private readonly static InternalLogger log = ClientLogger.getLog();
        static NLog.Logger log = NLog.LogManager.GetCurrentClassLogger();
        private readonly MQClientInstance mQClientFactory;
        private readonly String groupName;
        private ConcurrentDictionary<MessageQueue, AtomicLong> offsetTable =
            new ConcurrentDictionary<MessageQueue, AtomicLong>();

        public RemoteBrokerOffsetStore(MQClientInstance mQClientFactory, String groupName)
        {
            this.mQClientFactory = mQClientFactory;
            this.groupName = groupName;
        }

        //@Override
        public void load()
        {
        }

        //@Override
        public void updateOffset(MessageQueue mq, long offset, bool increaseOnly)
        {
            if (mq != null)
            {
                AtomicLong offsetOld = this.offsetTable.get(mq);
                if (null == offsetOld)
                {
                    offsetOld = this.offsetTable.putIfAbsent(mq, new AtomicLong(offset));
                }

                if (null != offsetOld)
                {
                    if (increaseOnly)
                    {
                        MixAll.compareAndIncreaseOnly(offsetOld, offset);
                    }
                    else
                    {
                        offsetOld.set(offset);
                    }
                }
            }
        }

        //@Override
        public long readOffset(MessageQueue mq, ReadOffsetType type)
        {
            if (mq != null)
            {
                switch (type)
                {
                    case ReadOffsetType.MEMORY_FIRST_THEN_STORE:
                    case ReadOffsetType.READ_FROM_MEMORY:
                        {
                            AtomicLong offset = this.offsetTable.get(mq);
                            if (offset != null)
                            {
                                return offset.get();
                            }
                            else if (ReadOffsetType.READ_FROM_MEMORY == type)
                            {
                                return -1;
                            }
                            break;
                        }
                    case ReadOffsetType.READ_FROM_STORE:
                        {
                            try
                            {
                                long brokerOffset = this.fetchConsumeOffsetFromBroker(mq);
                                AtomicLong offset = new AtomicLong(brokerOffset);
                                this.updateOffset(mq, offset.get(), false);
                                return brokerOffset;
                            }
                            // No offset in broker
                            catch (MQBrokerException e)
                            {
                                return -1;
                            }
                            //Other exceptions
                            catch (Exception e)
                            {
                                log.Warn("fetchConsumeOffsetFromBroker exception, " + mq, e);
                                return -2;
                            }
                        }
                    default:
                        break;
                }
            }

            return -1;
        }

        //@Override
        public void persistAll(HashSet<MessageQueue> mqs)
        {
            if (null == mqs || mqs.isEmpty())
                return;

            HashSet<MessageQueue> unusedMQ = new HashSet<MessageQueue>();

            foreach (var entry in this.offsetTable)
            {
                MessageQueue mq = entry.Key;
                AtomicLong offset = entry.Value;
                if (offset != null)
                {
                    if (mqs.Contains(mq))
                    {
                        try
                        {
                            this.updateConsumeOffsetToBroker(mq, offset.get());
                            log.Info("[persistAll] Group: {} ClientId: {} updateConsumeOffsetToBroker {} {}",
                                this.groupName,
                                this.mQClientFactory.getClientId(),
                                mq,
                                offset.get());
                        }
                        catch (Exception e)
                        {
                            log.Error("updateConsumeOffsetToBroker exception, " + mq.ToString(), e);
                        }
                    }
                    else
                    {
                        unusedMQ.Add(mq);
                    }
                }
            }

            if (!unusedMQ.isEmpty())
            {
                foreach (MessageQueue mq in unusedMQ)
                {
                    this.offsetTable.remove(mq);
                    log.Info("remove unused mq, {}, {}", mq, this.groupName);
                }
            }
        }

        //@Override
        public void persist(MessageQueue mq)
        {
            AtomicLong offset = this.offsetTable.get(mq);
            if (offset != null)
            {
                try
                {
                    this.updateConsumeOffsetToBroker(mq, offset.get());
                    log.Info("[persist] Group: {} ClientId: {} updateConsumeOffsetToBroker {} {}",
                        this.groupName,
                        this.mQClientFactory.getClientId(),
                        mq,
                        offset.get());
                }
                catch (Exception e)
                {
                    log.Error("updateConsumeOffsetToBroker exception, " + mq.ToString(), e);
                }
            }
        }

        public void removeOffset(MessageQueue mq)
        {
            if (mq != null)
            {
                //this.offsetTable.remove(mq);
                this.offsetTable.TryRemove(mq, out _);
                log.Info("remove unnecessary messageQueue offset. group={}, mq={}, offsetTableSize={}", this.groupName, mq, offsetTable.Count);
            }
        }

        //@Override
        public Dictionary<MessageQueue, long> cloneOffsetTable(String topic)
        {
            Dictionary<MessageQueue, long> cloneOffsetTable = new Dictionary<MessageQueue, long>();
            foreach (var entry in this.offsetTable)
            {
                MessageQueue mq = entry.Key;
                if (!UtilAll.isBlank(topic) && !topic.Equals(mq.getTopic()))
                {
                    continue;
                }
                //cloneOffsetTable.put(mq, entry.Value.Get());
                cloneOffsetTable[mq] =  entry.Value.get();
            }
            return cloneOffsetTable;
        }

        /**
         * Update the Consumer Offset in one way, once the Master is off, updated to Slave, here need to be optimized.
         */
        ///<exception cref="RemotingException"/>
        ///<exception cref="MQBrokerException"/>
        ///<exception cref="InterruptedException"/>
        ///<exception cref="MQClientException"/>
        private void updateConsumeOffsetToBroker(MessageQueue mq, long offset)
        {
            updateConsumeOffsetToBroker(mq, offset, true);
        }

        /**
         * Update the Consumer Offset synchronously, once the Master is off, updated to Slave, here need to be optimized.
         */
        //@Override
        public void updateConsumeOffsetToBroker(MessageQueue mq, long offset, bool isOneway)
        {
            FindBrokerResult findBrokerResult = this.mQClientFactory.findBrokerAddressInSubscribe(mq.getBrokerName(), MixAll.MASTER_ID, true);
            if (null == findBrokerResult)
            {
                this.mQClientFactory.updateTopicRouteInfoFromNameServer(mq.getTopic());
                findBrokerResult = this.mQClientFactory.findBrokerAddressInSubscribe(mq.getBrokerName(), MixAll.MASTER_ID, false);
            }

            if (findBrokerResult != null)
            {
                UpdateConsumerOffsetRequestHeader requestHeader = new UpdateConsumerOffsetRequestHeader();
                requestHeader.topic = (mq.getTopic());
                requestHeader.consumerGroup = (this.groupName);
                requestHeader.queueId = (mq.getQueueId());
                requestHeader.commitOffset = (offset);

                if (isOneway)
                {
                    this.mQClientFactory.getMQClientAPIImpl().updateConsumerOffsetOneway(
                        findBrokerResult.getBrokerAddr(), requestHeader, 1000 * 5);
                }
                else
                {
                    this.mQClientFactory.getMQClientAPIImpl().updateConsumerOffset(
                        findBrokerResult.getBrokerAddr(), requestHeader, 1000 * 5);
                }
            }
            else
            {
                throw new MQClientException("The broker[" + mq.getBrokerName() + "] not exist", null);
            }
        }

        ///<exception cref="RemotingException"/>
        ///<exception cref="MQBrokerException"/>
        ///<exception cref="InterruptedException"/>
        ///<exception cref="MQClientException"/>
        private long fetchConsumeOffsetFromBroker(MessageQueue mq)
        {
            FindBrokerResult findBrokerResult = this.mQClientFactory.findBrokerAddressInSubscribe(mq.getBrokerName(), MixAll.MASTER_ID, true);
            if (null == findBrokerResult)
            {

                this.mQClientFactory.updateTopicRouteInfoFromNameServer(mq.getTopic());
                findBrokerResult = this.mQClientFactory.findBrokerAddressInSubscribe(mq.getBrokerName(), MixAll.MASTER_ID, false);
            }

            if (findBrokerResult != null)
            {
                QueryConsumerOffsetRequestHeader requestHeader = new QueryConsumerOffsetRequestHeader();
                requestHeader.topic = (mq.getTopic());
                requestHeader.consumerGroup = (this.groupName);
                requestHeader.queueId = (mq.getQueueId());

                return this.mQClientFactory.getMQClientAPIImpl().queryConsumerOffset(
                    findBrokerResult.getBrokerAddr(), requestHeader, 1000 * 5);
            }
            else
            {
                throw new MQClientException("The broker[" + mq.getBrokerName() + "] not exist", null);
            }
        }
    }
}
