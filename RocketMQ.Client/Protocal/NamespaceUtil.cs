using System;
using System.Text;

namespace RocketMQ.Client
{
    public class NamespaceUtil
    {
        public static readonly char NAMESPACE_SEPARATOR = '%';
        public static readonly String STRING_BLANK = "";
        public static readonly int RETRY_PREFIX_LENGTH = MixAll.RETRY_GROUP_TOPIC_PREFIX.Length;
        public static readonly int DLQ_PREFIX_LENGTH = MixAll.DLQ_GROUP_TOPIC_PREFIX.Length;

        /**
         * Unpack namespace from resource, just like:
         * (1) MQ_INST_XX%Topic_XXX --> Topic_XXX
         * (2) %RETRY%MQ_INST_XX%GID_XXX --> %RETRY%GID_XXX
         *
         * @param resourceWithNamespace, topic/groupId with namespace.
         * @return topic/groupId without namespace.
         */
        public static String withoutNamespace(String resourceWithNamespace)
        {
            if (string.IsNullOrEmpty(resourceWithNamespace) || isSystemResource(resourceWithNamespace))
            {
                return resourceWithNamespace;
            }

            StringBuilder stringBuilder = new StringBuilder();
            if (isRetryTopic(resourceWithNamespace))
            {
                stringBuilder.Append(MixAll.RETRY_GROUP_TOPIC_PREFIX);
            }
            if (isDLQTopic(resourceWithNamespace))
            {
                stringBuilder.Append(MixAll.DLQ_GROUP_TOPIC_PREFIX);
            }

            String resourceWithoutRetryAndDLQ = withOutRetryAndDLQ(resourceWithNamespace);
            int index = resourceWithoutRetryAndDLQ.IndexOf(NAMESPACE_SEPARATOR);
            if (index > 0)
            {
                String resourceWithoutNamespace = resourceWithoutRetryAndDLQ.Substring(index + 1);
                return stringBuilder.Append(resourceWithoutNamespace).ToString();
            }

            return resourceWithNamespace;
        }

        /**
         * If resource contains the namespace, unpack namespace from resource, just like:
         * (1) (MQ_INST_XX1%Topic_XXX1, MQ_INST_XX1) --> Topic_XXX1
         * (2) (MQ_INST_XX2%Topic_XXX2, NULL) --> MQ_INST_XX2%Topic_XXX2
         * (3) (%RETRY%MQ_INST_XX1%GID_XXX1, MQ_INST_XX1) --> %RETRY%GID_XXX1
         * (4) (%RETRY%MQ_INST_XX2%GID_XXX2, MQ_INST_XX3) --> %RETRY%MQ_INST_XX2%GID_XXX2
         *
         * @param resourceWithNamespace, topic/groupId with namespace.
         * @param namespace, namespace to be unpacked.
         * @return topic/groupId without namespace.
         */
        public static String withoutNamespace(String resourceWithNamespace, String nameSpace)
        {
            if (string.IsNullOrEmpty(resourceWithNamespace) || string.IsNullOrEmpty(nameSpace))
            {
                return resourceWithNamespace;
            }

            String resourceWithoutRetryAndDLQ = withOutRetryAndDLQ(resourceWithNamespace);
            if (resourceWithoutRetryAndDLQ.StartsWith(nameSpace + NAMESPACE_SEPARATOR))
            {
                return withoutNamespace(resourceWithNamespace);
            }

            return resourceWithNamespace;
        }

        public static String wrapNamespace(String nameSpace, String resourceWithOutNamespace)
        {
            if (string.IsNullOrEmpty(nameSpace) || string.IsNullOrEmpty(resourceWithOutNamespace))
            {
                return resourceWithOutNamespace;
            }

            if (isSystemResource(resourceWithOutNamespace) || isAlreadyWithNamespace(resourceWithOutNamespace, nameSpace))
            {
                return resourceWithOutNamespace;
            }

            String resourceWithoutRetryAndDLQ = withOutRetryAndDLQ(resourceWithOutNamespace);
            StringBuilder stringBuilder = new StringBuilder();

            if (isRetryTopic(resourceWithOutNamespace))
            {
                stringBuilder.Append(MixAll.RETRY_GROUP_TOPIC_PREFIX);
            }

            if (isDLQTopic(resourceWithOutNamespace))
            {
                stringBuilder.Append(MixAll.DLQ_GROUP_TOPIC_PREFIX);
            }

            return stringBuilder.Append(nameSpace).Append(NAMESPACE_SEPARATOR).Append(resourceWithoutRetryAndDLQ).ToString();

        }

        public static bool isAlreadyWithNamespace(String resource, String nameSpace)
        {
            if (string.IsNullOrEmpty(nameSpace) || string.IsNullOrEmpty(resource) || isSystemResource(resource))
            {
                return false;
            }

            String resourceWithoutRetryAndDLQ = withOutRetryAndDLQ(resource);

            return resourceWithoutRetryAndDLQ.StartsWith(nameSpace + NAMESPACE_SEPARATOR);
        }

        public static String wrapNamespaceAndRetry(String nameSpace, String consumerGroup)
        {
            if (string.IsNullOrEmpty(consumerGroup))
            {
                return null;
            }

            return new StringBuilder()
                .Append(MixAll.RETRY_GROUP_TOPIC_PREFIX)
                .Append(wrapNamespace(nameSpace, consumerGroup))
                .ToString();
        }

        public static String getNamespaceFromResource(String resource)
        {
            if (string.IsNullOrEmpty(resource) || isSystemResource(resource))
            {
                return STRING_BLANK;
            }
            String resourceWithoutRetryAndDLQ = withOutRetryAndDLQ(resource);
            int index = resourceWithoutRetryAndDLQ.IndexOf(NAMESPACE_SEPARATOR);

            return index > 0 ? resourceWithoutRetryAndDLQ.JavaSubstring(0, index) : STRING_BLANK;
        }

        private static String withOutRetryAndDLQ(String originalResource)
        {
            if (string.IsNullOrEmpty(originalResource))
            {
                return STRING_BLANK;
            }
            if (isRetryTopic(originalResource))
            {
                return originalResource.Substring(RETRY_PREFIX_LENGTH);
            }

            if (isDLQTopic(originalResource))
            {
                return originalResource.Substring(DLQ_PREFIX_LENGTH);
            }

            return originalResource;
        }

        private static bool isSystemResource(String resource)
        {
            if (string.IsNullOrEmpty(resource))
            {
                return false;
            }

            if (TopicValidator.isSystemTopic(resource) || MixAll.isSysConsumerGroup(resource))
            {
                return true;
            }

            return false;
        }

        public static bool isRetryTopic(String resource)
        {
            return !string.IsNullOrEmpty(resource) && resource.StartsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX);
        }

        public static bool isDLQTopic(String resource)
        {
            return !string.IsNullOrEmpty(resource) && resource.StartsWith(MixAll.DLQ_GROUP_TOPIC_PREFIX);
        }
    }
}
