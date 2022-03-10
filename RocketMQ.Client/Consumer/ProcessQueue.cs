using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace RocketMQ.Client
{
    public class ProcessQueue
    {
        public readonly static long REBALANCE_LOCK_MAX_LIVE_TIME =
        long.Parse(Sys.getProperty("rocketmq.client.rebalance.lockMaxLiveTime", "30000"));
        public readonly static long REBALANCE_LOCK_INTERVAL = long.Parse(Sys.getProperty("rocketmq.client.rebalance.lockInterval", "20000"));
        private readonly static long PULL_MAX_IDLE_TIME = long.Parse(Sys.getProperty("rocketmq.client.pull.pullMaxIdleTime", "120000"));
        //private readonly InternalLogger log = ClientLogger.getLog();
        static NLog.Logger log = NLog.LogManager.GetCurrentClassLogger();
        //private readonly ReadWriteLock treeMapLock = new ReentrantReadWriteLock();
        private ReaderWriterLockSlim treeMapLock = new ReaderWriterLockSlim();
        //private readonly object treeMapLock = new object();
        //private readonly TreeMap<long, MessageExt> msgTreeMap = new TreeMap<long, MessageExt>();
        private readonly SortedDictionary<long, MessageExt> msgTreeMap = new SortedDictionary<long, MessageExt>();
        private readonly AtomicLong msgCount = new AtomicLong();
        private readonly AtomicLong msgSize = new AtomicLong();
        //private readonly Lock consumeLock = new ReentrantLock();
        private readonly object consumeLock = new object();
        /**
         * A subset of msgTreeMap, will only be used when orderly consume
         */
        //private readonly TreeMap<long, MessageExt> consumingMsgOrderlyTreeMap = new TreeMap<long, MessageExt>();
        private readonly SortedDictionary<long, MessageExt> consumingMsgOrderlyTreeMap = new SortedDictionary<long, MessageExt>();
        private readonly AtomicLong tryUnlockTimes = new AtomicLong(0);

        private /*volatile*/ long queueOffsetMax = 0L;  //不要直接使用此属性读写
        public long QueueOffsetMax
        {
            get 
            {
                return Volatile.Read(ref queueOffsetMax);
            }
            set 
            {
                Volatile.Write(ref queueOffsetMax, value);
            }
        }
        private volatile bool dropped = false;
        private /*volatile*/ long lastPullTimestamp = Sys.currentTimeMillis();//不要直接使用此属性读写
        public long LastPullTimestamp
        {
            get
            {
                return Volatile.Read(ref lastPullTimestamp);
            }
            set
            {
                Volatile.Write(ref lastPullTimestamp, value);
            }
        }
        private /*volatile*/ long lastConsumeTimestamp = Sys.currentTimeMillis();//不要直接使用此属性读写
        public long LastConsumeTimestamp
        {
            get
            {
                return Volatile.Read(ref lastConsumeTimestamp);
            }
            set
            {
                Volatile.Write(ref lastConsumeTimestamp, value);
            }
        }
        private volatile bool locked = false;
        private /*volatile*/ long lastLockTimestamp = Sys.currentTimeMillis();//不要直接使用此属性读写
        public long LastLockTimestamp
        {
            get
            {
                return Volatile.Read(ref lastLockTimestamp);
            }
            set
            {
                Volatile.Write(ref lastLockTimestamp, value);
            }
        }
        private volatile bool consuming = false;//不要直接使用此属性读写
        private /*volatile*/ long msgAccCnt = 0;//不要直接使用此属性读写
        public long MsgAccCnt
        {
            get
            {
                return Volatile.Read(ref msgAccCnt);
            }
            set
            {
                Volatile.Write(ref msgAccCnt, value);
            }
        }

        public bool isLockExpired()
        {
            return (TimeUtils.CurrentTimeMillisUTC() - this.LastLockTimestamp) > REBALANCE_LOCK_MAX_LIVE_TIME;
        }

        public bool isPullExpired()
        {
            return (TimeUtils.CurrentTimeMillisUTC() - this.LastPullTimestamp) > PULL_MAX_IDLE_TIME;
        }

        /**
         * @param pushConsumer
         */
        public void cleanExpiredMsg(DefaultMQPushConsumer pushConsumer)
        {
            if (pushConsumer.getDefaultMQPushConsumerImpl().isConsumeOrderly())
            {
                return;
            }

            int loop = msgTreeMap.Count < 16 ? msgTreeMap.Count : 16;
            for (int i = 0; i < loop; i++)
            {
                MessageExt msg = null;
                try
                {
                    treeMapLock.EnterReadLock();
                    //this.treeMapLock.readLock().lockInterruptibly();
                    try
                    {
                        if (!msgTreeMap.isEmpty())
                        {
                            String consumeStartTimeStamp = MessageAccessor.getConsumeStartTimeStamp(msgTreeMap.First().Value);
                            if (StringUtils.isNotEmpty(consumeStartTimeStamp) && 
                                Sys.currentTimeMillis() - long.Parse(consumeStartTimeStamp) > pushConsumer.getConsumeTimeout() * 60 * 1000)
                            {
                                msg = msgTreeMap.First().Value;
                            }
                            else
                            {
                                break;
                            }
                        }
                        else
                        {
                            break;
                        }
                    }
                    finally
                    {
                        //this.treeMapLock.readLock().unlock();
                        this.treeMapLock.ExitReadLock();
                    }
                }
                //catch (InterruptedException e)
                catch (Exception e)
                {
                    log.Error("getExpiredMsg exception", e.ToString());
                }

                try
                {

                    pushConsumer.sendMessageBack(msg, 3);
                    log.Info("send expire msg back. topic={}, msgId={}, storeHost={}, queueId={}, queueOffset={}", msg.getTopic(), msg.getMsgId(), msg.getStoreHost(), msg.getQueueId(), msg.getQueueOffset());
                    try
                    {
                        treeMapLock.EnterWriteLock();
                        //this.treeMapLock.writeLock().lockInterruptibly();
                        try
                        {
                            //if (!msgTreeMap.isEmpty() && msg.getQueueOffset() == msgTreeMap.firstKey())
                            if (!msgTreeMap.isEmpty() && msg.getQueueOffset() == msgTreeMap.First().Key)
                            {
                                try
                                {
                                    removeMessage(Collections.singletonList(msg));
                                }
                                catch (Exception e)
                                {
                                    log.Error("send expired msg exception", e.ToString());
                                }
                            }
                        }
                        finally
                        {
                            //this.treeMapLock.writeLock().unlock();
                            treeMapLock.ExitWriteLock();
                        }
                    }
                    //catch (InterruptedException e)
                    catch (Exception e)
                    {
                        log.Error("getExpiredMsg exception", e.ToString());
                    }
                }
                catch (Exception e)
                {
                    log.Error("send expired msg exception", e.ToString());
                }
            }
        }

        public bool putMessage(List<MessageExt> msgs)
        {
            bool dispatchToConsume = false;
            try
            {
                treeMapLock.EnterWriteLock();
                //this.treeMapLock.writeLock().lockInterruptibly();
                try
                {
                    int validMsgCnt = 0;
                    foreach (MessageExt msg in msgs)
                    {
                        MessageExt old = msgTreeMap.put(msg.getQueueOffset(), msg);
                        if (null == old)
                        {
                            validMsgCnt++;
                            this.QueueOffsetMax = msg.getQueueOffset();
                            msgSize.addAndGet(msg.getBody().Length);
                        }
                    }
                    msgCount.addAndGet(validMsgCnt);

                    if (!msgTreeMap.isEmpty() && !this.consuming)
                    {
                        dispatchToConsume = true;
                        this.consuming = true;
                    }

                    if (!msgs.isEmpty())
                    {
                        MessageExt messageExt = msgs.get(msgs.Count - 1);
                        String property = messageExt.getProperty(MessageConst.PROPERTY_MAX_OFFSET);
                        if (property != null)
                        {
                            long accTotal = long.Parse(property) - messageExt.getQueueOffset();
                            if (accTotal > 0)
                            {
                                this.MsgAccCnt = accTotal;
                            }
                        }
                    }
                }
                finally
                {
                    //this.treeMapLock.writeLock().unlock();
                    treeMapLock.ExitWriteLock();
                }
            }
            //catch (InterruptedException e)
            catch (Exception e)
            {
                log.Error("putMessage exception", e.ToString());
            }

            return dispatchToConsume;
        }

        public long getMaxSpan()
        {
            try
            {
                treeMapLock.EnterReadLock();
                //this.treeMapLock.readLock().lockInterruptibly();
                try
                {
                    if (!this.msgTreeMap.isEmpty())
                    {
                        //return this.msgTreeMap.lastKey() - this.msgTreeMap.firstKey();
                        return this.msgTreeMap.Last().Key - this.msgTreeMap.First().Key;
                    }
                }
                finally
                {
                    //this.treeMapLock.readLock().unlock();
                    treeMapLock.ExitReadLock();
                }
            }
            //catch (InterruptedException e)
            catch (Exception e)
            {
                log.Error("getMaxSpan exception", e.ToString());
            }

            return 0;
        }

        public long removeMessage(List<MessageExt> msgs)
        {
            long result = -1;
            long now = Sys.currentTimeMillis();
            try
            {
                treeMapLock.EnterWriteLock();
                //this.treeMapLock.writeLock().lockInterruptibly();
                this.LastConsumeTimestamp = now;
                try
                {
                    if (!msgTreeMap.isEmpty())
                    {
                        result = this.QueueOffsetMax + 1;
                        int removedCnt = 0;
                        foreach (MessageExt msg in msgs)
                        {
                            MessageExt prev = msgTreeMap.remove(msg.getQueueOffset());
                            if (prev != null)
                            {
                                removedCnt--;
                                msgSize.addAndGet(0 - msg.getBody().Length);
                            }
                        }
                        msgCount.addAndGet(removedCnt);
                        if (!msgTreeMap.isEmpty())
                        {
                            result = msgTreeMap.First().Key;
                        }
                    }
                }
                finally
                {
                    //this.treeMapLock.writeLock().unlock();
                    treeMapLock.ExitWriteLock();
                }
            }
            catch (Exception e)
            {
                log.Error("removeMessage exception", e.ToString());
            }

            return result;
        }

        public SortedDictionary<long, MessageExt> getMsgTreeMap()
        {
            return msgTreeMap;
        }

        public AtomicLong getMsgCount()
        {
            return msgCount;
        }

        public AtomicLong getMsgSize()
        {
            return msgSize;
        }

        public bool isDropped()
        {
            return dropped;
        }

        public void setDropped(bool dropped)
        {
            this.dropped = dropped;
        }

        public bool isLocked()
        {
            return locked;
        }

        public void setLocked(bool locked)
        {
            this.locked = locked;
        }

        public void rollback()
        {
            try
            {
                treeMapLock.EnterWriteLock();
                //this.treeMapLock.writeLock().lockInterruptibly();
                try
                {
                    this.msgTreeMap.putAll(this.consumingMsgOrderlyTreeMap);
                    this.consumingMsgOrderlyTreeMap.Clear();
                }
                finally
                {
                    //this.treeMapLock.writeLock().unlock();
                    treeMapLock.ExitWriteLock();
                }
            }
            //catch (InterruptedException e)
            catch (Exception e)
            {
                log.Error("rollback exception", e.ToString());
            }
        }

        public long commit()
        {
            try
            {
                treeMapLock.EnterWriteLock();
                //this.treeMapLock.writeLock().lockInterruptibly();
                try
                {
                    long offset = this.consumingMsgOrderlyTreeMap.Last().Key;
                    msgCount.addAndGet(0 - consumingMsgOrderlyTreeMap.Count);
                    foreach (MessageExt msg in this.consumingMsgOrderlyTreeMap.Values)
                    {
                        msgSize.addAndGet(0 - msg.getBody().Length);
                    }
                    this.consumingMsgOrderlyTreeMap.Clear();
                    if (offset != 0) //???if (offset != null)
                    {
                        return offset + 1;
                    }
                }
                finally
                {
                    //this.treeMapLock.writeLock().unlock();
                    treeMapLock.ExitWriteLock();
                }
            }
            //catch (InterruptedException e)
            catch (Exception e)
            {
                log.Error("commit exception", e.ToString());
            }

            return -1;
        }

        public void makeMessageToConsumeAgain(List<MessageExt> msgs)
        {
            try
            {
                treeMapLock.EnterWriteLock();
                //this.treeMapLock.writeLock().lockInterruptibly();
                try
                {
                    foreach (MessageExt msg in msgs)
                    {
                        this.consumingMsgOrderlyTreeMap.remove(msg.getQueueOffset());
                        this.msgTreeMap.put(msg.getQueueOffset(), msg);
                    }
                }
                finally
                {
                    //this.treeMapLock.writeLock().unlock();
                    treeMapLock.ExitWriteLock();
                }
            }
            //catch (InterruptedException e)
            catch (Exception e)
            {
                log.Error("makeMessageToCosumeAgain exception", e.ToString());
            }
        }

        public List<MessageExt> takeMessages(int batchSize)
        {
            List<MessageExt> result = new ArrayList<MessageExt>(batchSize);
            long now = Sys.currentTimeMillis();
            try
            {
                this.treeMapLock.EnterWriteLock();
                //this.treeMapLock.writeLock().lockInterruptibly();
                this.LastConsumeTimestamp = now;
                try
                {
                    if (!this.msgTreeMap.isEmpty())
                    {
                        for (int i = 0; i < batchSize; i++)
                        {
                            var entry = this.msgTreeMap.pollFirstEntry();
                            //if (entry != null)
                            if (!entry.Equals(default))
                            {
                                result.Add(entry.Value);
                                consumingMsgOrderlyTreeMap.put(entry.Key, entry.Value);
                            }
                            else
                            {
                                break;
                            }
                        }
                    }

                    if (result.isEmpty())
                    {
                        consuming = false;
                    }
                }
                finally
                {
                    //this.treeMapLock.writeLock().unlock();
                    treeMapLock.ExitWriteLock();
                }
            }
            //catch (InterruptedException e)
            catch (Exception e)
            {
                log.Error("take Messages exception", e.ToString());
            }

            return result;
        }

        public bool hasTempMessage()
        {
            try
            {
                treeMapLock.EnterReadLock();
                //this.treeMapLock.readLock().lockInterruptibly();
                try
                {
                    return !this.msgTreeMap.isEmpty();
                }
                finally
                {
                    //this.treeMapLock.readLock().unlock();
                    treeMapLock.ExitReadLock();
                }
            }
            //catch (InterruptedException e)
            catch (Exception e)
            {
            }

            return true;
        }

        public void clear()
        {
            try
            {
                treeMapLock.EnterWriteLock();
                //this.treeMapLock.writeLock().lockInterruptibly();
                try
                {
                    this.msgTreeMap.Clear();
                    this.consumingMsgOrderlyTreeMap.Clear();
                    this.msgCount.set(0);
                    this.msgSize.set(0);
                    this.QueueOffsetMax = 0L;
                }
                finally
                {
                    //this.treeMapLock.writeLock().unlock();
                    treeMapLock.ExitWriteLock();
                }
            }
            //catch (InterruptedException e)
            catch (Exception e)
            {
                log.Error("rollback exception", e.ToString());
            }
        }

        public object getConsumeLock()
        {
            return consumeLock;
        }

        public long getTryUnlockTimes()
        {
            return this.tryUnlockTimes.get();
        }

        public void incTryUnlockTimes()
        {
            this.tryUnlockTimes.incrementAndGet();
        }

        public void fillProcessQueueInfo(ProcessQueueInfo info)
        {
            try
            {
                treeMapLock.EnterReadLock();
                //this.treeMapLock.readLock().lockInterruptibly();
                if (!this.msgTreeMap.isEmpty())
                {
                    info.setCachedMsgMinOffset(this.msgTreeMap.First().Key);
                    info.setCachedMsgMaxOffset(this.msgTreeMap.First().Key);
                    info.setCachedMsgCount(this.msgTreeMap.Count);
                    info.setCachedMsgSizeInMiB((int)(this.msgSize.get() / (1024 * 1024)));
                }

                if (!this.consumingMsgOrderlyTreeMap.isEmpty())
                {
                    info.setTransactionMsgMinOffset(this.consumingMsgOrderlyTreeMap.First().Key);
                    info.setTransactionMsgMaxOffset(this.consumingMsgOrderlyTreeMap.First().Key);
                    info.setTransactionMsgCount(this.consumingMsgOrderlyTreeMap.Count);
                }

                info.setLocked(this.locked);
                info.setTryUnlockTimes(this.tryUnlockTimes.get());
                info.setLastLockTimestamp(this.LastLockTimestamp);

                info.setDroped(this.dropped);
                info.setLastPullTimestamp(this.LastPullTimestamp);
                info.setLastConsumeTimestamp(this.LastConsumeTimestamp);
            }
            catch (Exception e)
            {
            }
            finally
            {
                //this.treeMapLock.readLock().unlock();
                treeMapLock.ExitReadLock();
            }
        }

    }
}
