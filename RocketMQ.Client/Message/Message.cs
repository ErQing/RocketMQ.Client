using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Text;

namespace RocketMQ.Client
{
    public class Message
    {
        private string topic;
        private int flag;
        private Dictionary<string, string> properties;
        private byte[] body;
        private string transactionId;

        public Message()
        {
        }

        public Message(string topic, byte[] body) : this(topic, "", "", 0, body, true)
        {

        }

        public Message(string topic, string tags, string keys, int flag, byte[] body, bool waitStoreMsgOK)
        {
            this.topic = topic;
            this.flag = flag;
            this.body = body;

            if (tags != null && tags.Length > 0)
            {
                this.setTags(tags);
            }

            if (keys != null && keys.Length > 0)
            {
                this.setKeys(keys);
            }

            this.setWaitStoreMsgOK(waitStoreMsgOK);
        }

        public Message(string topic, string tags, byte[] body) : this(topic, tags, "", 0, body, true)
        {

        }

        public Message(string topic, string tags, string keys, byte[] body) : this(topic, tags, keys, 0, body, true)
        {

        }

        public void setKeys(string keys)
        {
            this.putProperty(MessageConst.PROPERTY_KEYS, keys);
        }

        internal void putProperty(string name, string value)
        {
            if (null == this.properties)
            {
                this.properties = new Dictionary<string, string>();
            }

            this.properties.Add(name, value);
        }

        internal void clearProperty(string name)
        {
            if (null != this.properties)
            {
                this.properties.Remove(name);
            }
        }

        public void putUserProperty(string name, string value)
        {
            if (MessageConst.string_HASH_SET.Contains(name))
            {
                throw new Exception($"The Property {name} is used by system, input another please");
            }

            if (string.IsNullOrEmpty(value) || string.IsNullOrEmpty(name))
            //if (value == null || value.trim().isEmpty()  || name == null || name.trim().isEmpty())
            {
                throw new ArgumentException("The name or value of property can not be null or blank string!"
                );
            }

            this.putProperty(name, value);
        }

        public string getUserProperty(string name)
        {
            return this.getProperty(name);
        }

        public string getProperty(string name)
        {
            if (null == this.properties)
            {
                this.properties = new Dictionary<string, string>();
            }
            properties.TryGetValue(name, out string res);
            return res;
        }

        public string getTopic()
        {
            return topic;
        }

        public void setTopic(string topic)
        {
            this.topic = topic;
        }

        public string getTags()
        {
            return this.getProperty(MessageConst.PROPERTY_TAGS);
        }

        public void setTags(string tags)
        {
            this.putProperty(MessageConst.PROPERTY_TAGS, tags);
        }

        public string getKeys()
        {
            return this.getProperty(MessageConst.PROPERTY_KEYS);
        }

        public void setKeys(ICollection<string> keys)
        {
            StringBuilder sb = new StringBuilder();
            foreach (string k in keys)
            {
                sb.Append(k);
                sb.Append(MessageConst.KEY_SEPARATOR);
            }

            this.setKeys(sb.ToString().Trim());
        }

        public int getDelayTimeLevel()
        {
            string t = this.getProperty(MessageConst.PROPERTY_DELAY_TIME_LEVEL);
            if (t != null)
            {
                return int.Parse(t);
            }

            return 0;
        }

        public void setDelayTimeLevel(int level)
        {
            this.putProperty(MessageConst.PROPERTY_DELAY_TIME_LEVEL, level.ToString());
        }

        public bool isWaitStoreMsgOK()
        {
            string result = this.getProperty(MessageConst.PROPERTY_WAIT_STORE_MSG_OK);
            if (null == result)
            {
                return true;
            }

            return bool.Parse(result);
        }

        public void setWaitStoreMsgOK(bool waitStoreMsgOK)
        {
            this.putProperty(MessageConst.PROPERTY_WAIT_STORE_MSG_OK, waitStoreMsgOK.ToString());
        }

        public void setInstanceId(string instanceId)
        {
            this.putProperty(MessageConst.PROPERTY_INSTANCE_ID, instanceId);
        }

        public int getFlag()
        {
            return flag;
        }

        public void setFlag(int flag)
        {
            this.flag = flag;
        }

        public byte[] getBody()
        {
            return body;
        }

        public void setBody(byte[] body)
        {
            this.body = body;
        }

        public Dictionary<string, string> getProperties()
        {
            return properties;
        }

        internal void setProperties(Dictionary<string, string> properties)
        {
            this.properties = properties;
        }

        public string getBuyerId()
        {
            return getProperty(MessageConst.PROPERTY_BUYER_ID);
        }

        public void setBuyerId(string buyerId)
        {
            putProperty(MessageConst.PROPERTY_BUYER_ID, buyerId);
        }

        public string getTransactionId()
        {
            return transactionId;
        }

        public void setTransactionId(string transactionId)
        {
            this.transactionId = transactionId;
        }

        public override string ToString()
        {
            return "Message{" +
                "topic='" + topic + '\'' +
                ", flag=" + flag +
                ", properties=" + properties +
                ", body=" + body.ToString() +
                ", transactionId='" + transactionId + '\'' +
                '}';
        }
    }
}
