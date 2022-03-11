using System;
using System.Collections.Generic;
using System.Threading;

namespace RocketMQ.Client
{
    public class RebalancePushImpl : RebalanceImpl
    {
        static NLog.Logger log = NLog.LogManager.GetCurrentClassLogger();
        private readonly static long UNLOCK_DELAY_TIME_MILLS = long.Parse(Sys.getProperty("rocketmq.client.unlockDelayTimeMills", "20000"));
        private readonly DefaultMQPushConsumerImpl defaultMQPushConsumerImpl;

        public RebalancePushImpl(DefaultMQPushConsumerImpl defaultMQPushConsumerImpl)
            : this(null, MessageModel.UNKNOWN, null, null, defaultMQPushConsumerImpl)
        {

        }

        public RebalancePushImpl(String consumerGroup, MessageModel messageModel,
            AllocateMessageQueueStrategy allocateMessageQueueStrategy,
            MQClientInstance mQClientFactory, DefaultMQPushConsumerImpl defaultMQPushConsumerImpl)
            : base(consumerGroup, messageModel, allocateMessageQueueStrategy, mQClientFactory)
        {
            this.defaultMQPushConsumerImpl = defaultMQPushConsumerImpl;
        }

        //@Override
        public override void messageQueueChanged(String topic, HashSet<MessageQueue> mqAll, HashSet<MessageQueue> mqDivided)
        {
            /**
             * When rebalance result changed, should update subscription's version to notify broker.
             * Fix: inconsistency subscription may lead to consumer miss messages.
             */
            SubscriptionData subscriptionData = this.subscriptionInner.Get(topic);
            long newVersion = Sys.currentTimeMillis();
            log.Info("{} Rebalance changed, also update version: {}, {}", topic, subscriptionData.subVersion, newVersion);
            subscriptionData.subVersion = newVersion;

            int currentQueueCount = this.processQueueTable.Count;
            if (currentQueueCount != 0)
            {
                int pullThresholdForTopic = this.defaultMQPushConsumerImpl.getDefaultMQPushConsumer().getPullThresholdForTopic();
                if (pullThresholdForTopic != -1)
                {
                    int newVal = Math.Max(1, pullThresholdForTopic / currentQueueCount);
                    log.Info("The pullThresholdForQueue is changed from {} to {}",
                        this.defaultMQPushConsumerImpl.getDefaultMQPushConsumer().getPullThresholdForQueue(), newVal);
                    this.defaultMQPushConsumerImpl.getDefaultMQPushConsumer().setPullThresholdForQueue(newVal);
                }

                int pullThresholdSizeForTopic = this.defaultMQPushConsumerImpl.getDefaultMQPushConsumer().getPullThresholdSizeForTopic();
                if (pullThresholdSizeForTopic != -1)
                {
                    int newVal = Math.Max(1, pullThresholdSizeForTopic / currentQueueCount);
                    log.Info("The pullThresholdSizeForQueue is changed from {} to {}",
                        this.defaultMQPushConsumerImpl.getDefaultMQPushConsumer().getPullThresholdSizeForQueue(), newVal);
                    this.defaultMQPushConsumerImpl.getDefaultMQPushConsumer().setPullThresholdSizeForQueue(newVal);
                }
            }

            // notify broker
            this.getmQClientFactory().sendHeartbeatToAllBrokerWithLock();
        }

        //@Override
        public override bool removeUnnecessaryMessageQueue(MessageQueue mq, ProcessQueue pq)
        {
            this.defaultMQPushConsumerImpl.getOffsetStore().persist(mq);
            this.defaultMQPushConsumerImpl.getOffsetStore().removeOffset(mq);
            if (this.defaultMQPushConsumerImpl.isConsumeOrderly()
                && MessageModel.CLUSTERING.Equals(this.defaultMQPushConsumerImpl.messageModel()))
            {
                try
                {
                    //if (pq.getConsumeLock().tryLock(1000, TimeUnit.MILLISECONDS))
                    if (Monitor.TryEnter(pq.getConsumeLock(), 1000))
                    {
                        try
                        {
                            return this.unlockDelay(mq, pq);
                        }
                        finally
                        {
                            //pq.getConsumeLock().unlock();
                            Monitor.Exit(pq.getConsumeLock());
                        }
                    }
                    else
                    {
                        log.Warn("[WRONG]mq is consuming, so can not unlock it, {}. maybe hanged for a while, {}",
                            mq,
                            pq.getTryUnlockTimes());
                        pq.incTryUnlockTimes();
                    }
                }
                catch (Exception e)
                {
                    log.Error("removeUnnecessaryMessageQueue Exception", e.ToString());
                }

                return false;
            }
            return true;
        }

        private bool unlockDelay(MessageQueue mq, ProcessQueue pq)
        {
            // ok
            //        if (pq.hasTempMessage())
            //        {
            //            log.info("[{}]unlockDelay, begin {} ", mq.hashCode(), mq);
            //            this.defaultMQPushConsumerImpl.getmQClientFactory().getScheduledExecutorService().schedule(new Runnable() {
            //            @Override
            //            public void run()
            //            {
            //                log.info("[{}]unlockDelay, execute at once {}", mq.hashCode(), mq);
            //                RebalancePushImpl.this.unlock(mq, true);
            //            }
            //        }, UNLOCK_DELAY_TIME_MILLS, TimeUnit.MILLISECONDS);
            //    } else {
            //        this.unlock(mq, true);
            //}
            //    return true;
            if (pq.hasTempMessage())
            {
                log.Info("[{}]unlockDelay, begin {} ", mq.GetHashCode(), mq);
                var executor = this.defaultMQPushConsumerImpl.getmQClientFactory().getScheduledExecutorService();
                executor.Schedule(() =>
                {
                    log.Info("[{}]unlockDelay, execute at once {}", mq.GetHashCode(), mq);
                    unlock(mq, true);
                }, UNLOCK_DELAY_TIME_MILLS);
            }
            else
            {
                unlock(mq, true);
            }
            return true;
        }

        //@Override
        public override ConsumeType consumeType()
        {
            return ConsumeType.CONSUME_PASSIVELY;
        }

        //@Override
        public override void removeDirtyOffset(MessageQueue mq)
        {
            this.defaultMQPushConsumerImpl.getOffsetStore().removeOffset(mq);
        }

        [Obsolete]//@Deprecated
        //@Override
        public override long computePullFromWhere(MessageQueue mq)
        {
            long result = -1L;
            try
            {
                result = computePullFromWhereWithException(mq);
            }
            catch (MQClientException e)
            {
                log.Warn("Compute consume offset exception, mq={}", mq);
            }
            return result;
        }

        //@Override
        ///<exception cref="MQClientException"/>
        public override long computePullFromWhereWithException(MessageQueue mq)
        {
            long result = -1;
            ConsumeFromWhere consumeFromWhere = this.defaultMQPushConsumerImpl.getDefaultMQPushConsumer().getConsumeFromWhere();
            OffsetStore offsetStore = this.defaultMQPushConsumerImpl.getOffsetStore();
            switch (consumeFromWhere)
            {
                case ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET_AND_FROM_MIN_WHEN_BOOT_FIRST:
                case ConsumeFromWhere.CONSUME_FROM_MIN_OFFSET:
                case ConsumeFromWhere.CONSUME_FROM_MAX_OFFSET:
                case ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET:
                    {
                        long lastOffset = offsetStore.readOffset(mq, ReadOffsetType.READ_FROM_STORE);
                        if (lastOffset >= 0)
                        {
                            result = lastOffset;
                        }
                        // First start,no offset
                        else if (-1 == lastOffset)
                        {
                            if (mq.getTopic().StartsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX))
                            {
                                result = 0L;
                            }
                            else
                            {
                                try
                                {
                                    result = this.mQClientFactory.getMQAdminImpl().maxOffset(mq);
                                }
                                catch (MQClientException e)
                                {
                                    log.Warn("Compute consume offset from last offset exception, mq={}, exception={}", mq, e);
                                    throw e;
                                }
                            }
                        }
                        else
                        {
                            result = -1;
                        }
                        break;
                    }
                case ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET:
                    {
                        long lastOffset = offsetStore.readOffset(mq, ReadOffsetType.READ_FROM_STORE);
                        if (lastOffset >= 0)
                        {
                            result = lastOffset;
                        }
                        else if (-1 == lastOffset)
                        {
                            result = 0L;
                        }
                        else
                        {
                            result = -1;
                        }
                        break;
                    }
                case ConsumeFromWhere.CONSUME_FROM_TIMESTAMP:
                    {
                        long lastOffset = offsetStore.readOffset(mq, ReadOffsetType.READ_FROM_STORE);
                        if (lastOffset >= 0)
                        {
                            result = lastOffset;
                        }
                        else if (-1 == lastOffset)
                        {
                            if (mq.getTopic().StartsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX))
                            {
                                try
                                {
                                    result = this.mQClientFactory.getMQAdminImpl().maxOffset(mq);
                                }
                                catch (MQClientException e)
                                {
                                    log.Warn("Compute consume offset from last offset exception, mq={}, exception={}", mq, e);
                                    throw e;
                                }
                            }
                            else
                            {
                                try
                                {
                                    long timestamp = UtilAll.parseDate(this.defaultMQPushConsumerImpl.getDefaultMQPushConsumer().getConsumeTimestamp(),
                                        UtilAll.YYYYMMDDHHMMSS).Value.Ticks;
                                    result = this.mQClientFactory.getMQAdminImpl().searchOffset(mq, timestamp);
                                }
                                catch (MQClientException e)
                                {
                                    log.Warn("Compute consume offset from last offset exception, mq={}, exception={}", mq, e);
                                    throw e;
                                }
                            }
                        }
                        else
                        {
                            result = -1;
                        }
                        break;
                    }

                default:
                    break;
            }

            return result;
        }

        //@Override
        public override void dispatchPullRequest(List<PullRequest> pullRequestList)
        {
            foreach (PullRequest pullRequest in pullRequestList)
            {
                this.defaultMQPushConsumerImpl.executePullRequestImmediately(pullRequest);
                log.Info("doRebalance, {}, add a new pull request {}", consumerGroup, pullRequest);
            }
        }

    }
}
