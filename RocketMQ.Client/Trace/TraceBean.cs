using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RocketMQ.Client
{
    public class TraceBean
    {
        private static readonly String LOCAL_ADDRESS = UtilAll.ipToIPv4Str(UtilAll.getIP());
        private string topic = "";
        private string msgId = "";
        private string offsetMsgId = "";
        private string tags = "";
        private string keys = "";
        private string storeHost = LOCAL_ADDRESS;
        private string clientHost = LOCAL_ADDRESS;
        private long storeTime;
        private int retryTimes;
        private int bodyLength;
        private MessageType msgType;
        private LocalTransactionState transactionState;
        private String transactionId;
        private bool fromTransactionCheck;

        public MessageType getMsgType()
        {
            return msgType;
        }


        public void setMsgType(MessageType msgType)
        {
            this.msgType = msgType;
        }


        public String getOffsetMsgId()
        {
            return offsetMsgId;
        }


        public void setOffsetMsgId(String offsetMsgId)
        {
            this.offsetMsgId = offsetMsgId;
        }

        public String getTopic()
        {
            return topic;
        }


        public void setTopic(String topic)
        {
            this.topic = topic;
        }


        public String getMsgId()
        {
            return msgId;
        }


        public void setMsgId(String msgId)
        {
            this.msgId = msgId;
        }


        public String getTags()
        {
            return tags;
        }


        public void setTags(String tags)
        {
            this.tags = tags;
        }


        public String getKeys()
        {
            return keys;
        }


        public void setKeys(String keys)
        {
            this.keys = keys;
        }


        public String getStoreHost()
        {
            return storeHost;
        }


        public void setStoreHost(String storeHost)
        {
            this.storeHost = storeHost;
        }


        public String getClientHost()
        {
            return clientHost;
        }


        public void setClientHost(String clientHost)
        {
            this.clientHost = clientHost;
        }


        public long getStoreTime()
        {
            return storeTime;
        }


        public void setStoreTime(long storeTime)
        {
            this.storeTime = storeTime;
        }


        public int getRetryTimes()
        {
            return retryTimes;
        }


        public void setRetryTimes(int retryTimes)
        {
            this.retryTimes = retryTimes;
        }


        public int getBodyLength()
        {
            return bodyLength;
        }


        public void setBodyLength(int bodyLength)
        {
            this.bodyLength = bodyLength;
        }

        public LocalTransactionState getTransactionState()
        {
            return transactionState;
        }

        public void setTransactionState(LocalTransactionState transactionState)
        {
            this.transactionState = transactionState;
        }

        public String getTransactionId()
        {
            return transactionId;
        }

        public void setTransactionId(String transactionId)
        {
            this.transactionId = transactionId;
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
