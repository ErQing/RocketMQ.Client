using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RocketMQ.Client//.Protocal.Body
{
    public class TopicList : RemotingSerializable
    {
        public HashSet<String> topicList { get; set; } = new HashSet<String>();
        public string brokerAddr { get; set; }
    }
}
