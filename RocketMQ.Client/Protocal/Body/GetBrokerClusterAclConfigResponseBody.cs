using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RocketMQ.Client
{
    public class GetBrokerClusterAclConfigResponseBody : RemotingSerializable
    {
        public List<String> globalWhiteAddrs { get; set; }

        public List<PlainAccessConfig> plainAccessConfigs { get; set; }
    }
}
