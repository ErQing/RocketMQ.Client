using System;

namespace RocketMQ.Client
{
    public class SubscriptionGroupConfig
    {
        private String groupName;

        private bool consumeEnable = true;
        private bool consumeFromMinEnable = true;

        private bool consumeBroadcastEnable = true;

        private int retryQueueNums = 1;

        private int retryMaxTimes = 16;

        private ulong brokerId = (ulong)MixAll.MASTER_ID; //???

        private long whichBrokerWhenConsumeSlowly = 1;

        private bool notifyConsumerIdsChangedEnable = true;

        public String getGroupName()
        {
            return groupName;
        }

        public void setGroupName(String groupName)
        {
            this.groupName = groupName;
        }

        public bool isConsumeEnable()
        {
            return consumeEnable;
        }

        public void setConsumeEnable(bool consumeEnable)
        {
            this.consumeEnable = consumeEnable;
        }

        public bool isConsumeFromMinEnable()
        {
            return consumeFromMinEnable;
        }

        public void setConsumeFromMinEnable(bool consumeFromMinEnable)
        {
            this.consumeFromMinEnable = consumeFromMinEnable;
        }

        public bool isConsumeBroadcastEnable()
        {
            return consumeBroadcastEnable;
        }

        public void setConsumeBroadcastEnable(bool consumeBroadcastEnable)
        {
            this.consumeBroadcastEnable = consumeBroadcastEnable;
        }

        public int getRetryQueueNums()
        {
            return retryQueueNums;
        }

        public void setRetryQueueNums(int retryQueueNums)
        {
            this.retryQueueNums = retryQueueNums;
        }

        public int getRetryMaxTimes()
        {
            return retryMaxTimes;
        }

        public void setRetryMaxTimes(int retryMaxTimes)
        {
            this.retryMaxTimes = retryMaxTimes;
        }

        public ulong getBrokerId()
        {
            return brokerId;
        }

        public void setBrokerId(ulong brokerId)
        {
            this.brokerId = brokerId;
        }

        public long getWhichBrokerWhenConsumeSlowly()
        {
            return whichBrokerWhenConsumeSlowly;
        }

        public void setWhichBrokerWhenConsumeSlowly(long whichBrokerWhenConsumeSlowly)
        {
            this.whichBrokerWhenConsumeSlowly = whichBrokerWhenConsumeSlowly;
        }

        public bool isNotifyConsumerIdsChangedEnable()
        {
            return notifyConsumerIdsChangedEnable;
        }

        public void setNotifyConsumerIdsChangedEnable(bool notifyConsumerIdsChangedEnable)
        {
            this.notifyConsumerIdsChangedEnable = notifyConsumerIdsChangedEnable;
        }

        public override int GetHashCode()
        {
            int prime = 31;
            int result = 1;
            result = prime * result + (int)(brokerId ^ (brokerId >> 32)); //ulong
            result = prime * result + (consumeBroadcastEnable ? 1231 : 1237);
            result = prime * result + (consumeEnable ? 1231 : 1237);
            result = prime * result + (consumeFromMinEnable ? 1231 : 1237);
            result = prime * result + (notifyConsumerIdsChangedEnable ? 1231 : 1237);
            result = prime * result + ((groupName == null) ? 0 : groupName.GetHashCode());
            result = prime * result + retryMaxTimes;
            result = prime * result + retryQueueNums;
            result =
                prime * result + (int)(whichBrokerWhenConsumeSlowly ^ (whichBrokerWhenConsumeSlowly >> 32)); //ulong
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
            SubscriptionGroupConfig other = (SubscriptionGroupConfig)obj;
            if (brokerId != other.brokerId)
                return false;
            if (consumeBroadcastEnable != other.consumeBroadcastEnable)
                return false;
            if (consumeEnable != other.consumeEnable)
                return false;
            if (consumeFromMinEnable != other.consumeFromMinEnable)
                return false;
            if (groupName == null)
            {
                if (other.groupName != null)
                    return false;
            }
            else if (!groupName.Equals(other.groupName))
                return false;
            if (retryMaxTimes != other.retryMaxTimes)
                return false;
            if (retryQueueNums != other.retryQueueNums)
                return false;
            if (whichBrokerWhenConsumeSlowly != other.whichBrokerWhenConsumeSlowly)
                return false;
            if (notifyConsumerIdsChangedEnable != other.notifyConsumerIdsChangedEnable)
                return false;
            return true;
        }

        public override String ToString()
        {
            return "SubscriptionGroupConfig [groupName=" + groupName + ", consumeEnable=" + consumeEnable
                + ", consumeFromMinEnable=" + consumeFromMinEnable + ", consumeBroadcastEnable="
                + consumeBroadcastEnable + ", retryQueueNums=" + retryQueueNums + ", retryMaxTimes="
                + retryMaxTimes + ", brokerId=" + brokerId + ", whichBrokerWhenConsumeSlowly="
                + whichBrokerWhenConsumeSlowly + ", notifyConsumerIdsChangedEnable="
                + notifyConsumerIdsChangedEnable + "]";
        }
    }
}
