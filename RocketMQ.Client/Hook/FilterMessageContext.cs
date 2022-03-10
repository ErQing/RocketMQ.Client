using System.Collections.Generic;

namespace RocketMQ.Client
{
    public class FilterMessageContext
    {
        private string consumerGroup;
        private List<MessageExt> msgList;
        private MessageQueue mq;
        private object arg;
        private bool unitMode;

        public string getConsumerGroup()
        {
            return consumerGroup;
        }

        public void setConsumerGroup(string consumerGroup)
        {
            this.consumerGroup = consumerGroup;
        }

        public List<MessageExt> getMsgList()
        {
            return msgList;
        }

        public void setMsgList(List<MessageExt> msgList)
        {
            this.msgList = msgList;
        }

        public MessageQueue getMq()
        {
            return mq;
        }

        public void setMq(MessageQueue mq)
        {
            this.mq = mq;
        }

        public object getArg()
        {
            return arg;
        }

        public void setArg(object arg)
        {
            this.arg = arg;
        }

        public bool isUnitMode()
        {
            return unitMode;
        }

        public void setUnitMode(bool isUnitMode)
        {
            this.unitMode = isUnitMode;
        }

        public override string ToString()
        {
            return "ConsumeMessageContext [consumerGroup=" + consumerGroup + ", msgList=" + msgList + ", mq="
                + mq + ", arg=" + arg + "]";
        }
    }
}
