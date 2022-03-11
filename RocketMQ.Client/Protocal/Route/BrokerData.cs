using System;
using System.Collections.Generic;
using System.Linq;

namespace RocketMQ.Client
{
    public class BrokerData : IComparable<BrokerData>
    {
        public string cluster { get; set; }
        public string brokerName { get; set; }
        public Dictionary<long/* brokerId */, String/* broker address */> brokerAddrs { get; set; }

        private readonly Random random = new Random();

        public BrokerData()
        {

        }

        public BrokerData(String cluster, string brokerName, Dictionary<long, String> brokerAddrs)
        {
            this.cluster = cluster;
            this.brokerName = brokerName;
            this.brokerAddrs = brokerAddrs;
        }

        /**
         * Selects a (preferably master) broker address from the registered list.
         * If the master's address cannot be found, a slave broker address is selected in a random manner.
         *
         * @return Broker address.
         */
        public string selectBrokerAddr()
        {
            //String addr = this.brokerAddrs.get(MixAll.MASTER_ID);
            brokerAddrs.TryGetValue(MixAll.MASTER_ID, out string addr);
            if (addr == null)
            {
                //List<String> addrs = new List<String>(brokerAddrs.Values); 
                List<String> addrs = brokerAddrs.Values.ToList();
                return addrs[random.Next(addrs.Count)];//next不会包含最大值
            }

            return addr;
        }

        public override int GetHashCode()
        {
            int prime = 31;
            int result = 1;
            result = prime * result + ((brokerAddrs == null) ? 0 : brokerAddrs.GetHashCode());
            result = prime * result + ((brokerName == null) ? 0 : brokerName.GetHashCode());
            return result;
        }

        public override bool Equals(Object obj)
        {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (GetType() != obj.GetType())
                return false;
            BrokerData other = (BrokerData)obj;
            if (brokerAddrs == null)
            {
                if (other.brokerAddrs != null)
                    return false;
            }
            else if (!brokerAddrs.Equals(other.brokerAddrs))
                return false;
            if (brokerName == null)
            {
                if (other.brokerName != null)
                    return false;
            }
            else if (!brokerName.Equals(other.brokerName))
                return false;
            return true;
        }

        public override string ToString()
        {
            return "BrokerData [brokerName=" + brokerName + ", brokerAddrs=" + brokerAddrs + "]";
        }

        public int CompareTo(BrokerData o)
        {
            return this.brokerName.CompareTo(o.brokerName);
        }
    }
}
