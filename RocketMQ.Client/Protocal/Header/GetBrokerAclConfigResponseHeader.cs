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
        public String version { get; set; }

        public String allAclFileVersion { get; set; }

        [CFNotNull]
        public String brokerName { get; set; }

        [CFNotNull]
        public String brokerAddr { get; set; }

        [CFNotNull]
        public String clusterName { get; set; }

        public void checkFields() { }
        
    }
}
