using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RocketMQ.Client
{
    public class QueryConsumeQueueResponseBody : RemotingSerializable
    {
        public SubscriptionData subscriptionData { get; set; }
        public String filterData { get; set; }
        public List<ConsumeQueueData> queueData { get; set; }
        public long maxQueueIndex { get; set; }
        public long minQueueIndex { get; set; }
    }
}
