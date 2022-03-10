using System;
using System.Collections.Generic;
using System.Text;

namespace RocketMQ.Client
{
    public interface IMQAdmin
    {
        /**
         * Creates an topic
         *
         * @param key accesskey
         * @param newTopic topic name
         * @param queueNum topic's queue number
         */
        void createTopic(String key, String newTopic, int queueNum);

        /**
         * Creates an topic
         *
         * @param key accesskey
         * @param newTopic topic name
         * @param queueNum topic's queue number
         * @param topicSysFlag topic system flag
         */
        void createTopic(String key, String newTopic, int queueNum, int topicSysFlag);

        /**
         * Gets the message queue offset according to some time in milliseconds<br>
         * be cautious to call because of more IO overhead
         *
         * @param mq Instance of MessageQueue
         * @param timestamp from when in milliseconds.
         * @return offset
         */
        long searchOffset(MessageQueue mq, long timestamp);

        /**
         * Gets the max offset
         *
         * @param mq Instance of MessageQueue
         * @return the max offset
         */
        long maxOffset(MessageQueue mq);

        /**
         * Gets the minimum offset
         *
         * @param mq Instance of MessageQueue
         * @return the minimum offset
         */
        long minOffset(MessageQueue mq);

        /**
         * Gets the earliest stored message time
         *
         * @param mq Instance of MessageQueue
         * @return the time in microseconds
         */
        long earliestMsgStoreTime(MessageQueue mq);

        /**
         * Query message according to message id
         *
         * @param offsetMsgId message id
         * @return message
         */
        MessageExt viewMessage(String offsetMsgId);

        /**
         * Query messages
         *
         * @param topic message topic
         * @param key message key index word
         * @param maxNum max message number
         * @param begin from when
         * @param end to when
         * @return Instance of QueryResult
         */
        QueryResult queryMessage(String topic, String key, int maxNum, long begin, long end);

        /**
         * @return The {@code MessageExt} of given msgId
         */
        MessageExt viewMessage(String topic, String msgId);
    }
}
