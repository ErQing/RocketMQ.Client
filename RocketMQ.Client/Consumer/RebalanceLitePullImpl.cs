using System;
using System.Collections.Generic;

namespace RocketMQ.Client
{
    public class RebalanceLitePullImpl : RebalanceImpl
    {
        static NLog.Logger log = NLog.LogManager.GetCurrentClassLogger();

        private readonly DefaultLitePullConsumerImpl litePullConsumerImpl;

        public RebalanceLitePullImpl(DefaultLitePullConsumerImpl litePullConsumerImpl) : this(null, MessageModel.UNKNOWN, null, null, litePullConsumerImpl)
        {

        }

        public RebalanceLitePullImpl(String consumerGroup, MessageModel messageModel,
            AllocateMessageQueueStrategy allocateMessageQueueStrategy,
            MQClientInstance mQClientFactory, DefaultLitePullConsumerImpl litePullConsumerImpl)
            : base(consumerGroup, messageModel, allocateMessageQueueStrategy, mQClientFactory)
        {

            this.litePullConsumerImpl = litePullConsumerImpl;
        }

        public override void messageQueueChanged(String topic, HashSet<MessageQueue> mqAll, HashSet<MessageQueue> mqDivided)
        {
            MessageQueueListener messageQueueListener = this.litePullConsumerImpl.getDefaultLitePullConsumer().getMessageQueueListener();
            if (messageQueueListener != null)
            {
                try
                {
                    messageQueueListener.messageQueueChanged(topic, mqAll, mqDivided);
                }
                catch (Exception e)
                {
                    log.Error("messageQueueChanged exception", e.ToString());
                }
            }
        }

        public override bool removeUnnecessaryMessageQueue(MessageQueue mq, ProcessQueue pq)
        {
            this.litePullConsumerImpl.getOffsetStore().persist(mq);
            this.litePullConsumerImpl.getOffsetStore().removeOffset(mq);
            return true;
        }

        public override ConsumeType consumeType()
        {
            return ConsumeType.CONSUME_ACTIVELY;
        }

        public override void removeDirtyOffset(MessageQueue mq)
        {
            this.litePullConsumerImpl.getOffsetStore().removeOffset(mq);
        }

        [Obsolete]//@Deprecated
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

        public override long computePullFromWhereWithException(MessageQueue mq)
        {
            ConsumeFromWhere consumeFromWhere = litePullConsumerImpl.getDefaultLitePullConsumer().getConsumeFromWhere();
            long result = -1;
            switch (consumeFromWhere)
            {
                case ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET:
                    {
                        long lastOffset = litePullConsumerImpl.getOffsetStore().readOffset(mq, ReadOffsetType.MEMORY_FIRST_THEN_STORE);
                        if (lastOffset >= 0)
                        {
                            result = lastOffset;
                        }
                        else if (-1 == lastOffset)
                        {
                            if (mq.getTopic().StartsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX))
                            { // First start, no offset
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
                        long lastOffset = litePullConsumerImpl.getOffsetStore().readOffset(mq, ReadOffsetType.MEMORY_FIRST_THEN_STORE);
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
                        long lastOffset = litePullConsumerImpl.getOffsetStore().readOffset(mq, ReadOffsetType.MEMORY_FIRST_THEN_STORE);
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
                                    long timestamp = UtilAll.parseDate(this.litePullConsumerImpl.getDefaultLitePullConsumer().getConsumeTimestamp(),
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
            }
            return result;
        }

        public override void dispatchPullRequest(List<PullRequest> pullRequestList)
        {
        }

    }
}
