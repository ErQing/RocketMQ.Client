using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RocketMQ.Client
{
    public class CheckForbiddenContext
    {
        private String nameSrvAddr;
        private String group;
        private Message message;
        private MessageQueue mq;
        private String brokerAddr;
        private CommunicationMode communicationMode;
        private SendResult sendResult;
        private Exception exception;
        private Object arg;
        private bool unitMode = false;

        public String getGroup()
        {
            return group;
        }

        public void setGroup(String group)
        {
            this.group = group;
        }

        public Message getMessage()
        {
            return message;
        }

        public void setMessage(Message message)
        {
            this.message = message;
        }

        public MessageQueue getMq()
        {
            return mq;
        }

        public void setMq(MessageQueue mq)
        {
            this.mq = mq;
        }

        public String getBrokerAddr()
        {
            return brokerAddr;
        }

        public void setBrokerAddr(String brokerAddr)
        {
            this.brokerAddr = brokerAddr;
        }

        public CommunicationMode getCommunicationMode()
        {
            return communicationMode;
        }

        public void setCommunicationMode(CommunicationMode communicationMode)
        {
            this.communicationMode = communicationMode;
        }

        public SendResult getSendResult()
        {
            return sendResult;
        }

        public void setSendResult(SendResult sendResult)
        {
            this.sendResult = sendResult;
        }

        public Exception getException()
        {
            return exception;
        }

        public void setException(Exception exception)
        {
            this.exception = exception;
        }

        public Object getArg()
        {
            return arg;
        }

        public void setArg(Object arg)
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

        public String getNameSrvAddr()
        {
            return nameSrvAddr;
        }

        public void setNameSrvAddr(String nameSrvAddr)
        {
            this.nameSrvAddr = nameSrvAddr;
        }

        public override String ToString()
        {
            return "SendMessageContext [nameSrvAddr=" + nameSrvAddr + ", group=" + group + ", message=" + message
                + ", mq=" + mq + ", brokerAddr=" + brokerAddr + ", communicationMode=" + communicationMode
                + ", sendResult=" + sendResult + ", exception=" + exception + ", unitMode=" + unitMode
                + ", arg=" + arg + "]";
        }
    }
}
