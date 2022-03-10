using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RocketMQ.Client
{
    public class GetConsumerListByGroupResponseBody : RemotingSerializable
    {
        public List<String> consumerIdList { get; set; }
    }
}
