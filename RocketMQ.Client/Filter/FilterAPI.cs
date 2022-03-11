using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RocketMQ.Client
{
    public class FilterAPI
    {
        //public static URL classFile(String className)
        //{
        //    string javaSource = simpleClassName(className) + ".java";
        //    URL url = FilterAPI./*class.*/getClassLoader().getResource(javaSource);
        //    return url;
        //}

        public static string simpleClassName(string className)
        {
            string simple = className;
            int index = className.LastIndexOf(".");
            if (index >= 0)
            {
                simple = className.Substring(index + 1);
            }
            return simple;
        }

        ///<exception cref="Exception"/>
        public static SubscriptionData buildSubscriptionData(String topic, string subString)
        {
            SubscriptionData subscriptionData = new SubscriptionData();
            subscriptionData.topic = topic;
            subscriptionData.subString = subString;

            if (null == subString || subString.Equals(SubscriptionData.SUB_ALL) || subString.Length == 0)
            {
                subscriptionData.subString = SubscriptionData.SUB_ALL;
            }
            else
            {
                String[] tags = subString.Split("\\|\\|");
                if (tags.Length > 0)
                {
                    foreach (String tag in tags)
                    {
                        if (tag.Length > 0)
                        {
                            string trimString = tag.Trim();
                            if (trimString.Length > 0)
                            {
                                subscriptionData.tagsSet.Add(trimString);
                                subscriptionData.codeSet.Add(trimString.GetHashCode());
                            }
                        }
                    }
                }
                else
                {
                    throw new Exception("subString split error");
                }
            }

            return subscriptionData;
        }

        ///<exception cref="Exception"/>
        public static SubscriptionData build(String topic, string subString, string type)
        {
            if (ExpressionType.TAG.Equals(type) || type == null)
            {
                return buildSubscriptionData(topic, subString);
            }

            if (subString == null || subString.Length < 1)
            {
                throw new ArgumentException("Expression can't be null! " + type);
            }

            SubscriptionData subscriptionData = new SubscriptionData();
            subscriptionData.topic = topic;
            subscriptionData.subString = subString;
            subscriptionData.expressionType = type;

            return subscriptionData;
        }
    }
}
