using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RocketMQ.Client//.Protocal.Body
{
    public class ProducerConnection : RemotingSerializable
    {
        public HashSet<Connection> connectionSet { get; set; } = new HashSet<Connection>();
    }
}
