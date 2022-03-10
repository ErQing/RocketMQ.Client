using System.Text;

namespace RocketMQ.Client
{
    public class FAQUrl
    {
        public static readonly string APPLY_TOPIC_URL =
        "http://rocketmq.apache.org/docs/faq/";

        public static readonly string NAME_SERVER_ADDR_NOT_EXIST_URL =
            "http://rocketmq.apache.org/docs/faq/";

        public static readonly string GROUP_NAME_DUPLICATE_URL =
            "http://rocketmq.apache.org/docs/faq/";

        public static readonly string CLIENT_PARAMETER_CHECK_URL =
            "http://rocketmq.apache.org/docs/faq/";

        public static readonly string SUBSCRIPTION_GROUP_NOT_EXIST =
            "http://rocketmq.apache.org/docs/faq/";

        public static readonly string CLIENT_SERVICE_NOT_OK =
            "http://rocketmq.apache.org/docs/faq/";

        // FAQ: No route info of this topic, TopicABC
        public static readonly string NO_TOPIC_ROUTE_INFO =
            "http://rocketmq.apache.org/docs/faq/";

        public static readonly string LOAD_JSON_EXCEPTION =
            "http://rocketmq.apache.org/docs/faq/";

        public static readonly string SAME_GROUP_DIFFERENT_TOPIC =
            "http://rocketmq.apache.org/docs/faq/";

        public static readonly string MQLIST_NOT_EXIST =
            "http://rocketmq.apache.org/docs/faq/";

        public static readonly string UNEXPECTED_EXCEPTION_URL =
            "http://rocketmq.apache.org/docs/faq/";

        public static readonly string SEND_MSG_FAILED =
            "http://rocketmq.apache.org/docs/faq/";

        public static readonly string UNKNOWN_HOST_EXCEPTION =
            "http://rocketmq.apache.org/docs/faq/";

        private static readonly string TIP_STRING_BEGIN = "\nSee ";
        private static readonly string TIP_STRING_END = " for further details.";

        public static string suggestTodo(string url)
        {
            StringBuilder sb = new StringBuilder();
            sb.Append(TIP_STRING_BEGIN);
            sb.Append(url);
            sb.Append(TIP_STRING_END);
            return sb.ToString();
        }

        public static string attachDefaultURL(string errorMessage)
        {
            if (errorMessage != null)
            {
                int index = errorMessage.IndexOf(TIP_STRING_BEGIN);
                if (-1 == index)
                {
                    StringBuilder sb = new StringBuilder();
                    sb.Append(errorMessage);
                    sb.Append("\n");
                    sb.Append("For more information, please visit the url, ");
                    sb.Append(UNEXPECTED_EXCEPTION_URL);
                    return sb.ToString();
                }
            }
            return errorMessage;
        }
    }
}
