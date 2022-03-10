using System;

namespace RocketMQ.Client
{
    public class MessageQueue : IComparable<MessageQueue>
    {
        private string topic;
        private string brokerName;
        private int queueId;

        public MessageQueue()
        {

        }

        public MessageQueue(string topic, string brokerName, int queueId)
        {
            this.topic = topic;
            this.brokerName = brokerName;
            this.queueId = queueId;
        }

        public string getTopic()
        {
            return topic;
        }

        public void setTopic(string topic)
        {
            this.topic = topic;
        }

        public string getBrokerName()
        {
            return brokerName;
        }

        public void setBrokerName(string brokerName)
        {
            this.brokerName = brokerName;
        }

        public int getQueueId()
        {
            return queueId;
        }

        public void setQueueId(int queueId)
        {
            this.queueId = queueId;
        }

        public override int GetHashCode()
        {
            int prime = 31;
            int result = 1;
            result = prime * result + ((brokerName == null) ? 0 : brokerName.GetHashCode());
            result = prime * result + queueId;
            result = prime * result + ((topic == null) ? 0 : topic.GetHashCode());
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
            MessageQueue other = (MessageQueue)obj;
            if (brokerName == null)
            {
                if (other.brokerName != null)
                    return false;
            }
            else if (!brokerName.Equals(other.brokerName))
                return false;
            if (queueId != other.queueId)
                return false;
            if (topic == null)
            {
                if (other.topic != null)
                    return false;
            }
            else if (!topic.Equals(other.topic))
                return false;
            return true;
        }

        public override string ToString()
        {
            return "MessageQueue [topic=" + topic + ", brokerName=" + brokerName + ", queueId=" + queueId + "]";
        }

        public int CompareTo(MessageQueue o)
        {
            {
                int result = this.topic.CompareTo(o.topic);
                if (result != 0)
                {
                    return result;
                }
            }

            {
                int result = this.brokerName.CompareTo(o.brokerName);
                if (result != 0)
                {
                    return result;
                }
            }

            return this.queueId - o.queueId;
        }
    }
}
