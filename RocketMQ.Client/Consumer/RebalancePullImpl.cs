using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RocketMQ.Client//.Consumer
{
    public class RebalancePullImpl : RebalanceImpl
    {
        static NLog.Logger log = NLog.LogManager.GetCurrentClassLogger();
        private readonly DefaultMQPullConsumerImpl defaultMQPullConsumerImpl;

        public RebalancePullImpl(DefaultMQPullConsumerImpl defaultMQPullConsumerImpl)
                : this(null, MessageModel.UNKNOWN, null, null, defaultMQPullConsumerImpl)
        {

        }

        public RebalancePullImpl(String consumerGroup, MessageModel messageModel,
            AllocateMessageQueueStrategy allocateMessageQueueStrategy,
            MQClientInstance mQClientFactory, DefaultMQPullConsumerImpl defaultMQPullConsumerImpl)
            : base(consumerGroup, messageModel, allocateMessageQueueStrategy, mQClientFactory)
        {

            this.defaultMQPullConsumerImpl = defaultMQPullConsumerImpl;
        }

        //@Override
        public override void messageQueueChanged(String topic, HashSet<MessageQueue> mqAll, HashSet<MessageQueue> mqDivided)
        {
            MessageQueueListener messageQueueListener = this.defaultMQPullConsumerImpl.getDefaultMQPullConsumer().getMessageQueueListener();
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

        //@Override
        public override bool removeUnnecessaryMessageQueue(MessageQueue mq, ProcessQueue pq)
        {
            this.defaultMQPullConsumerImpl.getOffsetStore().persist(mq);
            this.defaultMQPullConsumerImpl.getOffsetStore().removeOffset(mq);
            return true;
        }

        //@Override
        public override ConsumeType consumeType()
        {
            return ConsumeType.CONSUME_ACTIVELY;
        }

        //@Override
        public override void removeDirtyOffset(MessageQueue mq)
        {
            this.defaultMQPullConsumerImpl.getOffsetStore().removeOffset(mq);
        }

        [Obsolete] //@Deprecated @Override
        public override long computePullFromWhere(MessageQueue mq)
        {
            return 0;
        }

        //@Override
        public override long computePullFromWhereWithException(MessageQueue mq)
        {
            return 0;
        }

        //@Override
        public override void dispatchPullRequest(List<PullRequest> pullRequestList)
        {
        }
    }
}
