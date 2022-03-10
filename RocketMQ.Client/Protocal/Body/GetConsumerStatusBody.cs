using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RocketMQ.Client//.Protocal.Header
{
    public class GetConsumerStatusBody : RemotingSerializable
    {
        public Dictionary<MessageQueue, long> messageQueueTable { get; set; }
        public Dictionary<String, Dictionary<MessageQueue, long>> consumerTable { get; set; }
    }
}
