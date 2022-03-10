using System;
using System.Collections.Generic;

namespace RocketMQ.Client
{
    public interface MQProducerInner
    {
        HashSet<String> getPublishTopicList();

        bool isPublishTopicNeedUpdate(string topic);

        TransactionCheckListener checkListener();
        TransactionListener getCheckListener();

        void checkTransactionState(
            String addr,
            MessageExt msg,
            CheckTransactionStateRequestHeader checkRequestHeader);

        void updateTopicPublishInfo(string topic, TopicPublishInfo info);

        bool isUnitMode();
    }
}
