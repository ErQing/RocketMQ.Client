using System;

namespace RocketMQ.Client
{
    public class Validators
    {
        public static readonly int CHARACTER_MAX_LENGTH = 255;
        public static readonly int TOPIC_MAX_LENGTH = 127;

        /**
         * Validate group
         */
        ///<exception cref="MQClientException"/>
        public static void checkGroup(String group)
        {
            if (UtilAll.isBlank(group))
            {
                throw new MQClientException("the specified group is blank", null);
            }

            if (group.Length > CHARACTER_MAX_LENGTH)
            {
                throw new MQClientException("the specified group is longer than group max length 255.", null);
            }


            if (TopicValidator.isTopicOrGroupIllegal(group))
            {
                throw new MQClientException(string.Format(
                        "the specified group[%s] contains illegal characters, allowing only %s", group,
                        "^[%|a-zA-Z0-9_-]+$"), null);
            }
        }

        ///<exception cref="MQClientException"/>
        public static void checkMessage(Message msg, DefaultMQProducer defaultMQProducer)
        {
            if (null == msg)
            {
                throw new MQClientException(ResponseCode.MESSAGE_ILLEGAL, "the message is null");
            }
            // topic
            Validators.checkTopic(msg.getTopic());
            Validators.isNotAllowedSendTopic(msg.getTopic());

            // body
            if (null == msg.getBody())
            {
                throw new MQClientException(ResponseCode.MESSAGE_ILLEGAL, "the message body is null");
            }

            if (0 == msg.getBody().Length)
            {
                throw new MQClientException(ResponseCode.MESSAGE_ILLEGAL, "the message body length is zero");
            }

            if (msg.getBody().Length > defaultMQProducer.getMaxMessageSize())
            {
                throw new MQClientException(ResponseCode.MESSAGE_ILLEGAL,
                    "the message body size over max value, MAX: " + defaultMQProducer.getMaxMessageSize());
            }
        }

        ///<exception cref="MQClientException"/>
        public static void checkTopic(String topic)
        {
            if (UtilAll.isBlank(topic))
            {
                throw new MQClientException("The specified topic is blank", null);
            }

            if (topic.Length > TOPIC_MAX_LENGTH)
            {
                throw new MQClientException(
                    String.Format("The specified topic is longer than topic max length %d.", TOPIC_MAX_LENGTH), null);
            }

            if (TopicValidator.isTopicOrGroupIllegal(topic))
            {
                throw new MQClientException(String.Format(
                        "The specified topic[%s] contains illegal characters, allowing only %s", topic,
                        "^[%|a-zA-Z0-9_-]+$"), null);
            }
        }

        ///<exception cref="MQClientException"/>
        public static void isSystemTopic(String topic)
        {
            if (TopicValidator.isSystemTopic(topic))
            {
                throw new MQClientException(
                        String.Format("The topic[%s] is conflict with system topic.", topic), null);
            }
        }

        ///<exception cref="MQClientException"/>
        public static void isNotAllowedSendTopic(String topic)
        {
            if (TopicValidator.isNotAllowedSendTopic(topic))
            {
                throw new MQClientException(
                        String.Format("Sending message to topic[%s] is forbidden.", topic), null);
            }
        }
    }
}
