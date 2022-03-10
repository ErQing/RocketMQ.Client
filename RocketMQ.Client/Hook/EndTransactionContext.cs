using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RocketMQ.Client
{
    public class EndTransactionContext
    {
        private String producerGroup;
        private Message message;
        private String brokerAddr;
        private String msgId;
        private String transactionId;
        private LocalTransactionState transactionState;
        private bool fromTransactionCheck;

        public String getProducerGroup()
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

        public String getBrokerAddr()
        {
            return brokerAddr;
        }

        public void setBrokerAddr(String brokerAddr)
        {
            this.brokerAddr = brokerAddr;
        }

        public String getMsgId()
        {
            return msgId;
        }

        public void setMsgId(String msgId)
        {
            this.msgId = msgId;
        }

        public String getTransactionId()
        {
            return transactionId;
        }

        public void setTransactionId(String transactionId)
        {
            this.transactionId = transactionId;
        }

        public LocalTransactionState getTransactionState()
        {
            return transactionState;
        }

        public void setTransactionState(LocalTransactionState transactionState)
        {
            this.transactionState = transactionState;
        }

        public bool isFromTransactionCheck()
        {
            return fromTransactionCheck;
        }

        public void setFromTransactionCheck(bool fromTransactionCheck)
        {
            this.fromTransactionCheck = fromTransactionCheck;
        }
    }
}
