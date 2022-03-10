using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RocketMQ.Client//.Protocal.Body
{
    public class ConsumeStatsList : RemotingSerializable
    {
        public List<Dictionary<String/*subscriptionGroupName*/, List<ConsumeStats>>> consumeStatsList { get; set; } = new List<Dictionary<String, List<ConsumeStats>>>();
        public String brokerAddr { get; set; }
        public long totalDiff { get; set; }
    }
}
