using System;
using System.Collections.Concurrent;
using System.Collections.Generic;

namespace RocketMQ.Client
{
    public class ConsumerConnection : RemotingSerializable
    {
        public HashSet<Connection> connectionSet { get; set; }
        public ConcurrentDictionary<String/* Topic */, SubscriptionData> subscriptionTable { get; set; }
        public ConsumeType consumeType { get; set; }
        public MessageModel messageModel { get; set; }
        public ConsumeFromWhere consumeFromWhere { get; set; }

        public int computeMinVersion
        {
            get
            {
                int minVersion = int.MaxValue;
                foreach (Connection c in this.connectionSet)
                {
                    if (c.version < minVersion)
                    {
                        minVersion = c.version;
                    }
                }
                return minVersion;
            }
        }
    }
}
