using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RocketMQ.Client
{
    public class TopicStatsTable : RemotingSerializable
    {
        private Dictionary<MessageQueue, TopicOffset> offsetTable { get; set; } = new Dictionary<MessageQueue, TopicOffset>();
    }
}
