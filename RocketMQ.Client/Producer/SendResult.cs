using System;

namespace RocketMQ.Client
{
    public class SendResult
    {
        private SendStatus sendStatus;
        private String msgId;
        private MessageQueue messageQueue;
        private long queueOffset;
        private String transactionId;
        private string offsetMsgId;
        private string regionId;
        private bool traceOn = true;

        public SendResult()
        {
        }

        public SendResult(SendStatus sendStatus, string msgId, string offsetMsgId, MessageQueue messageQueue,
            long queueOffset)
        {
            this.sendStatus = sendStatus;
            this.msgId = msgId;
            this.offsetMsgId = offsetMsgId;
            this.messageQueue = messageQueue;
            this.queueOffset = queueOffset;
        }

        public SendResult(SendStatus sendStatus, string msgId, MessageQueue messageQueue,
            long queueOffset, string transactionId,
            string offsetMsgId, string regionId)
        {
            this.sendStatus = sendStatus;
            this.msgId = msgId;
            this.messageQueue = messageQueue;
            this.queueOffset = queueOffset;
            this.transactionId = transactionId;
            this.offsetMsgId = offsetMsgId;
            this.regionId = regionId;
        }

        public static string encoderSendResultToJson(Object obj)
        {
            return JSON.toJSONString(obj);
        }

        public static SendResult decoderSendResultFromJson(string json)
        {
            return JSON.parseObject<SendResult>(json/*, SendResult.class*/);
        }

        public bool isTraceOn()
        {
            return traceOn;
        }

        public void setTraceOn(bool traceOn)
        {
            this.traceOn = traceOn;
        }

        public string getRegionId()
        {
            return regionId;
        }

        public void setRegionId(string regionId)
        {
            this.regionId = regionId;
        }

        public string getMsgId()
        {
            return msgId;
        }

        public void setMsgId(string msgId)
        {
            this.msgId = msgId;
        }

        public SendStatus getSendStatus()
        {
            return sendStatus;
        }

        public void setSendStatus(SendStatus sendStatus)
        {
            this.sendStatus = sendStatus;
        }

        public MessageQueue getMessageQueue()
        {
            return messageQueue;
        }

        public void setMessageQueue(MessageQueue messageQueue)
        {
            this.messageQueue = messageQueue;
        }

        public long getQueueOffset()
        {
            return queueOffset;
        }

        public void setQueueOffset(long queueOffset)
        {
            this.queueOffset = queueOffset;
        }

        public string getTransactionId()
        {
            return transactionId;
        }

        public void setTransactionId(string transactionId)
        {
            this.transactionId = transactionId;
        }

        public string getOffsetMsgId()
        {
            return offsetMsgId;
        }

        public void setOffsetMsgId(string offsetMsgId)
        {
            this.offsetMsgId = offsetMsgId;
        }

        public override string ToString()
        {
            return "SendResult [sendStatus=" + sendStatus + ", msgId=" + msgId + ", offsetMsgId=" + offsetMsgId + ", messageQueue=" + messageQueue
                + ", queueOffset=" + queueOffset + "]";
        }
    }
}
