using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RocketMQ.Client//.Protocal.Body
{
    public class TopicConfigSerializeWrapper : RemotingSerializable
    {
        public ConcurrentDictionary<String, TopicConfig> topicConfigTable { get; set; } = new ConcurrentDictionary<String, TopicConfig>();
        public DataVersion dataVersion { get; set; } = new DataVersion();
    }
}
