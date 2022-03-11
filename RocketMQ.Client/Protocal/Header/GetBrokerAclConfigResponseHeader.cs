using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RocketMQ.Client
{
    public class GetBrokerAclConfigResponseHeader : CommandCustomHeader
    {
        [CFNotNull]
        public string version { get; set; }

        public string allAclFileVersion { get; set; }

        [CFNotNull]
        public string brokerName { get; set; }

        [CFNotNull]
        public string brokerAddr { get; set; }

        [CFNotNull]
        public string clusterName { get; set; }

        public void checkFields() { }
        
    }
}
