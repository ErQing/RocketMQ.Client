using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RocketMQ.Client//.Protocal.Body
{
    public class ClusterInfo : RemotingSerializable
    {
        public Dictionary<String/* brokerName */, BrokerData> brokerAddrTable { get; set; }
        public Dictionary<String/* clusterName */, HashSet<String/* brokerName */>> clusterAddrTable { get; set; }

        public String[] retrieveAllAddrByCluster(String cluster)
        {
            List<String> addrs = new List<String>();
            if (clusterAddrTable.ContainsKey(cluster))
            {
                HashSet<String> brokerNames = clusterAddrTable[cluster];
                foreach (String brokerName in brokerNames)
                {
                    //BrokerData brokerData = brokerAddrTable.get(brokerName);
                    brokerAddrTable.TryGetValue(brokerName, out BrokerData brokerData);
                    if (null != brokerData)
                    {
                        //addrs.addAll(brokerData.getBrokerAddrs().values());
                        addrs.AddRange(brokerData.brokerAddrs.Values);
                    }
                }
            }

            //return addrs.toArray(new String[] { });
            return addrs.ToArray();
        }

        public String[] retrieveAllClusterNames => clusterAddrTable.Keys.ToArray();
    }
}
