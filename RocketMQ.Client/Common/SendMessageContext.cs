using System;
using System.Collections.Generic;

namespace RocketMQ.Client
{
    public class SendMessageContext
    {
        private string producerGroup;
        private Message message;
        private MessageQueue mq;
        private string brokerAddr;
        private string bornHost;
        private CommunicationMode communicationMode;
        private SendResult sendResult;
        private Exception exception;
        private Object mqTraceContext;
        private Dictionary<String, String> props;
        private DefaultMQProducerImpl producer;
        private MessageType msgType = MessageType.Normal_Msg;
        private string nameSpace;

        public MessageType getMsgType()
        {
            return msgType;
        }

        public void setMsgType(MessageType msgType)
        {
            this.msgType = msgType;
        }

        public DefaultMQProducerImpl getProducer()
        {
            return producer;
        }

        public void setProducer(DefaultMQProducerImpl producer)
        {
            this.producer = producer;
        }

        public string getProducerGroup()
        {
            return producerGroup;
        }

        public void setProducerGroup(String producerGroup)
        {
            this.producerGroup = producerGroup;
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

        public string getBrokerAddr()
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

        public string getBornHost()
        {
            return bornHost;
        }

        public void setBornHost(String bornHost)
        {
            this.bornHost = bornHost;
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
