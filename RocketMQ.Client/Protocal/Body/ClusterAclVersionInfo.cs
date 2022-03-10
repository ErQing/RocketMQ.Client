using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RocketMQ.Client
{
    public class ClusterAclVersionInfo : RemotingSerializable
    {
        public String brokerName { get; set; }
        public String brokerAddr { get; set; }

        [Obsolete]//@Deprecated
        public DataVersion aclConfigDataVersion { get; set; }
        public Dictionary<String, DataVersion> allAclConfigDataVersion { get; set; }
        public String clusterName { get; set; }
    }
}
