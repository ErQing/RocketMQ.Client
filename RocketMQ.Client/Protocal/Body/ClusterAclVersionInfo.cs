using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RocketMQ.Client
{
    public class ClusterAclVersionInfo : RemotingSerializable
    {
        public string brokerName { get; set; }
        public string brokerAddr { get; set; }

        [Obsolete]//@Deprecated
        public DataVersion aclConfigDataVersion { get; set; }
        public Dictionary<String, DataVersion> allAclConfigDataVersion { get; set; }
        public string clusterName { get; set; }
    }
}
