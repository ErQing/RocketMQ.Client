using System;
using System.Collections.Generic;

namespace RocketMQ.Client
{
    public class ConsumeMessageContext
    {
        private string consumerGroup;
        private List<MessageExt> msgList;
        private MessageQueue mq;
        private bool success;
        private string status;
        private object mqTraceContext;
        private Dictionary<String, String> props;
        private string nameSpace;

        public string getConsumerGroup()
        {
            return consumerGroup;
        }

        public void setConsumerGroup(String consumerGroup)
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

        public bool isSuccess()
        {
            return success;
        }

        public void setSuccess(bool success)
        {
            this.success = success;
        }

        public Object getMqTraceContext()
        {
            return mqTraceContext;
        }

        public void setMqTraceContext(Object mqTraceContext)
        {
            this.mqTraceContext = mqTraceContext;
        }

        public Dictionary<String, String> getProps()
        {
            return props;
        }

        public void setProps(Dictionary<String, String> props)
        {
            this.props = props;
        }

        public string getStatus()
        {
            return status;
        }

        public void setStatus(String status)
        {
            this.status = status;
        }

        public string getNamespace()
        {
            return nameSpace;
        }

        public void setNamespace(String nameSpace)
        {
            this.nameSpace = nameSpace;
        }
    }
}
