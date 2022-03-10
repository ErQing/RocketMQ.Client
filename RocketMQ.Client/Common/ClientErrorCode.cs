namespace RocketMQ.Client
{
    public class ClientErrorCode
    {
        public static readonly int CONNECT_BROKER_EXCEPTION = 10001;
        public static readonly int ACCESS_BROKER_TIMEOUT = 10002;
        public static readonly int BROKER_NOT_EXIST_EXCEPTION = 10003;
        public static readonly int NO_NAME_SERVER_EXCEPTION = 10004;
        public static readonly int NOT_FOUND_TOPIC_EXCEPTION = 10005;
        public static readonly int REQUEST_TIMEOUT_EXCEPTION = 10006;
        public static readonly int CREATE_REPLY_MESSAGE_EXCEPTION = 10007;
    }
}
