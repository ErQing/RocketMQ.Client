using System;
using System.Collections.Generic;

namespace RocketMQ.Client
{
    public class ConsumerData
    {
        public string groupName { get; set; }
        public ConsumeType consumeType { get; set; }
        public MessageModel messageModel { get; set; }
        public ConsumeFromWhere consumeFromWhere { get; set; }
        public HashSet<SubscriptionData> subscriptionDataSet { get; set; } = new HashSet<SubscriptionData>();
        public bool unitMode { get; set; }
        //@Override
        public override string ToString()
        {
            return "ConsumerData [groupName=" + groupName + ", consumeType=" + consumeType + ", messageModel="
                + messageModel + ", consumeFromWhere=" + consumeFromWhere + ", unitMode=" + unitMode
                + ", subscriptionDataSet=" + subscriptionDataSet + "]";
        }
    }
}
