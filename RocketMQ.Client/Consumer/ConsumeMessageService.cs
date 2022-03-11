using System;
using System.Collections.Generic;

namespace RocketMQ.Client
{
    public interface ConsumeMessageService
    {
        void start();
        void shutdown(long awaitTerminateMillis);

        void updateCorePoolSize(int corePoolSize);

        void incCorePoolSize();

        void decCorePoolSize();

        int getCorePoolSize();

        ConsumeMessageDirectlyResult consumeMessageDirectly(MessageExt msg, string brokerName);

        void submitConsumeRequest(
            List<MessageExt> msgs,
            ProcessQueue processQueue,
            MessageQueue messageQueue,
            bool dispathToConsume);
    }
}
