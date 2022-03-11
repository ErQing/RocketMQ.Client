using System;

namespace RocketMQ.Client
{
    public class QueueData : IComparable<QueueData>
    {
        public string brokerName{ get; set; }
        public int readQueueNums{ get; set; }
        public int writeQueueNums{ get; set; }
        public int perm{ get; set; }
        public int topicSysFlag{ get; set; }
        public override int GetHashCode()
        {
            int prime = 31;
            int result = 1;
            result = prime * result + ((brokerName == null) ? 0 : brokerName.GetHashCode());
            result = prime * result + perm;
            result = prime * result + readQueueNums;
            result = prime * result + writeQueueNums;
            result = prime * result + topicSysFlag;
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
            QueueData other = (QueueData)obj;
            if (brokerName == null)
            {
                if (other.brokerName != null)
                    return false;
            }
            else if (!brokerName.Equals(other.brokerName))
                return false;
            if (perm != other.perm)
                return false;
            if (readQueueNums != other.readQueueNums)
                return false;
            if (writeQueueNums != other.writeQueueNums)
                return false;
            if (topicSysFlag != other.topicSysFlag)
                return false;
            return true;
        }

        public override string ToString()
        {
            return "QueueData [brokerName=" + brokerName + ", readQueueNums=" + readQueueNums
                + ", writeQueueNums=" + writeQueueNums + ", perm=" + perm + ", topicSysFlag=" + topicSysFlag
                + "]";
        }

        public int CompareTo(QueueData o)
        {
            return this.brokerName.CompareTo(o.brokerName);
        }
    }
}
