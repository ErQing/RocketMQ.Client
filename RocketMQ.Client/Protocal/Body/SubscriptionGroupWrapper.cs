using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RocketMQ.Client//.Protocal.Body
{
    public class SubscriptionGroupWrapper : RemotingSerializable
    {
        public ConcurrentDictionary<String, SubscriptionGroupConfig> subscriptionGroupTable { get; set; } = new ConcurrentDictionary<String, SubscriptionGroupConfig>();// (1024);
        public DataVersion dataVersion { get; set; } = new DataVersion();
    }
}
