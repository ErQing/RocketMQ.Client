using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RocketMQ.Client
{
    public interface MQConsumerInner
    {
        string groupName();

        MessageModel messageModel();

        ConsumeType consumeType();

        ConsumeFromWhere consumeFromWhere();

        HashSet<SubscriptionData> subscriptions();

        void doRebalance();

        void persistConsumerOffset();

        void updateTopicSubscribeInfo(string topic, HashSet<MessageQueue> info);

        bool isSubscribeTopicNeedUpdate(string topic);

        bool isUnitMode();

        ConsumerRunningInfo consumerRunningInfo();
    }
}
