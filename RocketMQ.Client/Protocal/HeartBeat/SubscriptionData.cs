using System;
using System.Collections.Generic;

namespace RocketMQ.Client
{
    public class SubscriptionData : IComparable<SubscriptionData>
    {
        public readonly static string SUB_ALL = "*";
        public bool classFilterMode { get; set; } = false;
        public String topic { get; set; }
        public String subString { get; set; }
        public HashSet<String> tagsSet { get; set; } = new HashSet<String>();
        public HashSet<int> codeSet { get; set; } = new HashSet<int>();
        public long subVersion { get; set; } = Sys.currentTimeMillis();
        public String expressionType { get; set; } = ExpressionType.TAG;

        //@JSONField(serialize = false)
        public String filterClassSource { get; set; }

        public SubscriptionData()
        {

        }

        public SubscriptionData(String topic, String subString)
        {
            //super();
            this.topic = topic;
            this.subString = subString;
        }
        public override int GetHashCode()
        {
            int prime = 31;
            int result = 1;
            result = prime * result + (classFilterMode ? 1231 : 1237);
            result = prime * result + ((codeSet == null) ? 0 : codeSet.GetHashCode());
            result = prime * result + ((subString == null) ? 0 : subString.GetHashCode());
            result = prime * result + ((tagsSet == null) ? 0 : tagsSet.GetHashCode());
            result = prime * result + ((topic == null) ? 0 : topic.GetHashCode());
            result = prime * result + ((expressionType == null) ? 0 : expressionType.GetHashCode());
            return result;
        }

        public override bool Equals(Object obj)
        {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (GetType() != obj.GetType())
                return false;
            SubscriptionData other = (SubscriptionData)obj;
            if (classFilterMode != other.classFilterMode)
                return false;
            if (codeSet == null)
            {
                if (other.codeSet != null)
                    return false;
            }
            else if (!codeSet.Equals(other.codeSet))
                return false;
            if (subString == null)
            {
                if (other.subString != null)
                    return false;
            }
            else if (!subString.Equals(other.subString))
                return false;
            if (subVersion != other.subVersion)
                return false;
            if (tagsSet == null)
            {
                if (other.tagsSet != null)
                    return false;
            }
            else if (!tagsSet.Equals(other.tagsSet))
                return false;
            if (topic == null)
            {
                if (other.topic != null)
                    return false;
            }
            else if (!topic.Equals(other.topic))
                return false;
            if (expressionType == null)
            {
                if (other.expressionType != null)
                    return false;
            }
            else if (!expressionType.Equals(other.expressionType))
                return false;
            return true;
        }

        public override String ToString()
        {
            return "SubscriptionData [classFilterMode=" + classFilterMode + ", topic=" + topic + ", subString="
                + subString + ", tagsSet=" + tagsSet + ", codeSet=" + codeSet + ", subVersion=" + subVersion
                + ", expressionType=" + expressionType + "]";
        }

        public int CompareTo(SubscriptionData other)
        {
            String thisValue = this.topic + "@" + this.subString;
            String otherValue = other.topic + "@" + other.subString;
            return thisValue.CompareTo(otherValue);
        }

    }
}
