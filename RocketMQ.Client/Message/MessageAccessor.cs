using System;
using System.Collections.Generic;

namespace RocketMQ.Client
{
    public class MessageAccessor
    {
        public static void clearProperty(Message msg, string name)
        {
            msg.clearProperty(name);
        }

        public static void setProperties(Message msg, Dictionary<String, String> properties)
        {
            msg.setProperties(properties);
        }

        public static void setTransferFlag(Message msg, string unit)
        {
            putProperty(msg, MessageConst.PROPERTY_TRANSFER_FLAG, unit);
        }

        public static void putProperty(Message msg, string name, string value)
        {
            msg.putProperty(name, value);
        }

        public static string getTransferFlag(Message msg)
        {
            return msg.getProperty(MessageConst.PROPERTY_TRANSFER_FLAG);
        }

        public static void setCorrectionFlag(Message msg, string unit)
        {
            putProperty(msg, MessageConst.PROPERTY_CORRECTION_FLAG, unit);
        }

        public static string getCorrectionFlag(Message msg)
        {
            return msg.getProperty(MessageConst.PROPERTY_CORRECTION_FLAG);
        }

        public static void setOriginMessageId(Message msg, string originMessageId)
        {
            putProperty(msg, MessageConst.PROPERTY_ORIGIN_MESSAGE_ID, originMessageId);
        }

        public static string getOriginMessageId(Message msg)
        {
            return msg.getProperty(MessageConst.PROPERTY_ORIGIN_MESSAGE_ID);
        }

        public static void setMQ2Flag(Message msg, string flag)
        {
            putProperty(msg, MessageConst.PROPERTY_MQ2_FLAG, flag);
        }

        public static string getMQ2Flag(Message msg)
        {
            return msg.getProperty(MessageConst.PROPERTY_MQ2_FLAG);
        }

        public static void setReconsumeTime(Message msg, string reconsumeTimes)
        {
            putProperty(msg, MessageConst.PROPERTY_RECONSUME_TIME, reconsumeTimes);
        }

        public static string getReconsumeTime(Message msg)
        {
            return msg.getProperty(MessageConst.PROPERTY_RECONSUME_TIME);
        }

        public static void setMaxReconsumeTimes(Message msg, string maxReconsumeTimes)
        {
            putProperty(msg, MessageConst.PROPERTY_MAX_RECONSUME_TIMES, maxReconsumeTimes);
        }

        public static string getMaxReconsumeTimes(Message msg)
        {
            return msg.getProperty(MessageConst.PROPERTY_MAX_RECONSUME_TIMES);
        }

        public static void setConsumeStartTimeStamp(Message msg, string propertyConsumeStartTimeStamp)
        {
            putProperty(msg, MessageConst.PROPERTY_CONSUME_START_TIMESTAMP, propertyConsumeStartTimeStamp);
        }

        public static string getConsumeStartTimeStamp(Message msg)
        {
            return msg.getProperty(MessageConst.PROPERTY_CONSUME_START_TIMESTAMP);
        }

        public static Message cloneMessage(Message msg)
        {
            Message newMsg = new Message(msg.getTopic(), msg.getBody());
            newMsg.setFlag(msg.getFlag());
            newMsg.setProperties(msg.getProperties());
            return newMsg;
        }
    }
}
