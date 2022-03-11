using System;
using System.Collections.Generic;

namespace RocketMQ.Client
{
    public class TopicRouteData : RemotingSerializable
    {
        public string orderTopicConf { get; set; }
        public List<QueueData> queueDatas { get; set; }
        public List<BrokerData> brokerDatas { get; set; }
        public Dictionary<String/* brokerAddr */, List<String>/* Filter Server */> filterServerTable { get; set; }

        public TopicRouteData cloneTopicRouteData()
        {
            TopicRouteData topicRouteData = new TopicRouteData();
            //topicRouteData.setQueueDatas(new List<QueueData>());
            //topicRouteData.setBrokerDatas(new List<BrokerData>());
            //topicRouteData.setFilterServerTable(new Dictionary<String, List<String>>());
            //topicRouteData.setOrderTopicConf(this.orderTopicConf);
            topicRouteData.queueDatas = new List<QueueData>();
            topicRouteData.brokerDatas = new List<BrokerData>();
            topicRouteData.filterServerTable = new Dictionary<String, List<String>>();
            topicRouteData.orderTopicConf = orderTopicConf;

            if (this.queueDatas != null)
            {
                topicRouteData.queueDatas.AddRange(this.queueDatas);
            }

            if (this.brokerDatas != null)
            {
                topicRouteData.brokerDatas.AddRange(this.brokerDatas);
            }

            if (this.filterServerTable != null)
            {
                //topicRouteData.getFilterServerTable().putAll(this.filterServerTable);
                topicRouteData.filterServerTable.PutAll(this.filterServerTable);
            }

            return topicRouteData;
        }
        public override int GetHashCode()
        {
            int prime = 31;
            int result = 1;
            result = prime * result + ((brokerDatas == null) ? 0 : brokerDatas.GetHashCode());
            result = prime * result + ((orderTopicConf == null) ? 0 : orderTopicConf.GetHashCode());
            result = prime * result + ((queueDatas == null) ? 0 : queueDatas.GetHashCode());
            result = prime * result + ((filterServerTable == null) ? 0 : filterServerTable.GetHashCode());
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
            TopicRouteData other = (TopicRouteData)obj;
            if (brokerDatas == null)
            {
                if (other.brokerDatas != null)
                    return false;
            }
            else if (!brokerDatas.Equals(other.brokerDatas))
                return false;
            if (orderTopicConf == null)
            {
                if (other.orderTopicConf != null)
                    return false;
            }
            else if (!orderTopicConf.Equals(other.orderTopicConf))
                return false;
            if (queueDatas == null)
            {
                if (other.queueDatas != null)
                    return false;
            }
            else if (!queueDatas.Equals(other.queueDatas))
                return false;
            if (filterServerTable == null)
            {
                if (other.filterServerTable != null)
                    return false;
            }
            else if (!filterServerTable.Equals(other.filterServerTable))
                return false;
            return true;
        }

        public override string ToString()
        {
            return "TopicRouteData [orderTopicConf=" + orderTopicConf + ", queueDatas=" + queueDatas
                + ", brokerDatas=" + brokerDatas + ", filterServerTable=" + filterServerTable + "]";
        }
    }
}
