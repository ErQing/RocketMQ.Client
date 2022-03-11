using System;
using System.Text;

namespace RocketMQ.Client
{
    public class TopicConfig
    {
        private static readonly string SEPARATOR = " ";
        public static int defaultReadQueueNums = 16;
        public static int defaultWriteQueueNums = 16;
        private string topicName;
        private int readQueueNums = defaultReadQueueNums;
        private int writeQueueNums = defaultWriteQueueNums;
        private int perm = PermName.PERM_READ | PermName.PERM_WRITE;
        private TopicFilterType topicFilterType = TopicFilterType.SINGLE_TAG;
        private int topicSysFlag = 0;
        private bool order = false;

        public TopicConfig()
        {
        }

        public TopicConfig(String topicName)
        {
            this.topicName = topicName;
        }

        public TopicConfig(String topicName, int readQueueNums, int writeQueueNums, int perm)
        {
            this.topicName = topicName;
            this.readQueueNums = readQueueNums;
            this.writeQueueNums = writeQueueNums;
            this.perm = perm;
        }

        public string encode()
        {
            StringBuilder sb = new StringBuilder();
            sb.Append(this.topicName);
            sb.Append(SEPARATOR);
            sb.Append(this.readQueueNums);
            sb.Append(SEPARATOR);
            sb.Append(this.writeQueueNums);
            sb.Append(SEPARATOR);
            sb.Append(this.perm);
            sb.Append(SEPARATOR);
            sb.Append(this.topicFilterType);

            return sb.ToString();
        }

        public bool decode(String str)
        {
            String[] strs = str.Split(SEPARATOR);
            if (strs != null && strs.Length == 5)
            {
                this.topicName = strs[0];

                this.readQueueNums = int.Parse(strs[1]);

                this.writeQueueNums = int.Parse(strs[2]);

                this.perm = int.Parse(strs[3]);

                //this.topicFilterType = TopicFilterType.valueOf(strs[4]);
                this.topicFilterType = (TopicFilterType)int.Parse(strs[4]); //???

                return true;
            }

            return false;
        }

        public string getTopicName()
        {
            return topicName;
        }

        public void setTopicName(String topicName)
        {
            this.topicName = topicName;
        }

        public int getReadQueueNums()
        {
            return readQueueNums;
        }

        public void setReadQueueNums(int readQueueNums)
        {
            this.readQueueNums = readQueueNums;
        }

        public int getWriteQueueNums()
        {
            return writeQueueNums;
        }

        public void setWriteQueueNums(int writeQueueNums)
        {
            this.writeQueueNums = writeQueueNums;
        }

        public int getPerm()
        {
            return perm;
        }

        public void setPerm(int perm)
        {
            this.perm = perm;
        }

        public TopicFilterType getTopicFilterType()
        {
            return topicFilterType;
        }

        public void setTopicFilterType(TopicFilterType topicFilterType)
        {
            this.topicFilterType = topicFilterType;
        }

        public int getTopicSysFlag()
        {
            return topicSysFlag;
        }

        public void setTopicSysFlag(int topicSysFlag)
        {
            this.topicSysFlag = topicSysFlag;
        }

        public bool isOrder()
        {
            return order;
        }

        public void setOrder(bool isOrder)
        {
            this.order = isOrder;
        }

        //@Override
        public override bool Equals(Object o)
        {
            if (this == o)
                return true;
            if (o == null || GetType() != o.GetType())
                return false;

            TopicConfig that = (TopicConfig)o;

            if (readQueueNums != that.readQueueNums)
                return false;
            if (writeQueueNums != that.writeQueueNums)
                return false;
            if (perm != that.perm)
                return false;
            if (topicSysFlag != that.topicSysFlag)
                return false;
            if (order != that.order)
                return false;
            if (topicName != null ? !topicName.Equals(that.topicName) : that.topicName != null)
                return false;
            return topicFilterType == that.topicFilterType;

        }

        //@Override
        public override int GetHashCode()
        {
            int result = topicName != null ? topicName.GetHashCode() : 0;
            result = 31 * result + readQueueNums;
            result = 31 * result + writeQueueNums;
            result = 31 * result + perm;
            result = 31 * result + (topicFilterType != null ? topicFilterType.GetHashCode() : 0);
            result = 31 * result + topicSysFlag;
            result = 31 * result + (order ? 1 : 0);
            return result;
        }

        //@Override
        public override string ToString()
        {
            return "TopicConfig [topicName=" + topicName + ", readQueueNums=" + readQueueNums
                + ", writeQueueNums=" + writeQueueNums + ", perm=" + PermName.perm2String(perm)
                + ", topicFilterType=" + topicFilterType + ", topicSysFlag=" + topicSysFlag + ", order="
                + order + "]";
        }
    }
}
