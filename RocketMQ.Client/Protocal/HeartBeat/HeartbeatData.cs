using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RocketMQ.Client
{
    public class HeartbeatData : RemotingSerializable
    {
        public String clientID { get; set; }
        public HashSet<ProducerData> producerDataSet { get; set; } = new HashSet<ProducerData>();
        public HashSet<ConsumerData> consumerDataSet { get; set; } = new HashSet<ConsumerData>();
        //@Override
        public override String ToString()
        {
            return "HeartbeatData [clientID=" + clientID + ", producerDataSet=" + producerDataSet
                + ", consumerDataSet=" + consumerDataSet + "]";
        }
    }
}
