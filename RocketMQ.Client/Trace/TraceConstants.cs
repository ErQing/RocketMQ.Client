namespace RocketMQ.Client
{
    public class TraceConstants
    {
        public static readonly string GROUP_NAME_PREFIX = "_INNER_TRACE_PRODUCER";
        public static readonly char CONTENT_SPLITOR = (char)1;
        public static readonly char FIELD_SPLITOR = (char)2;
        public static readonly string TRACE_INSTANCE_NAME = "PID_CLIENT_INNER_TRACE_PRODUCER";
        public static readonly string TRACE_TOPIC_PREFIX = TopicValidator.SYSTEM_TOPIC_PREFIX + "TRACE_DATA_";
        public static readonly string TO_PREFIX = "To_";
        public static readonly string FROM_PREFIX = "From_";
        public static readonly string END_TRANSACTION = "EndTransaction";
        public static readonly string ROCKETMQ_SERVICE = "rocketmq";
        public static readonly string ROCKETMQ_SUCCESS = "rocketmq.success";
        public static readonly string ROCKETMQ_TAGS = "rocketmq.tags";
        public static readonly string ROCKETMQ_KEYS = "rocketmq.keys";
        public static readonly string ROCKETMQ_SOTRE_HOST = "rocketmq.store_host";
        public static readonly string ROCKETMQ_BODY_LENGTH = "rocketmq.body_length";
        public static readonly string ROCKETMQ_MSG_ID = "rocketmq.mgs_id";
        public static readonly string ROCKETMQ_MSG_TYPE = "rocketmq.mgs_type";
        public static readonly string ROCKETMQ_REGION_ID = "rocketmq.region_id";
        public static readonly string ROCKETMQ_TRANSACTION_ID = "rocketmq.transaction_id";
        public static readonly string ROCKETMQ_TRANSACTION_STATE = "rocketmq.transaction_state";
        public static readonly string ROCKETMQ_IS_FROM_TRANSACTION_CHECK = "rocketmq.is_from_transaction_check";
        public static readonly string ROCKETMQ_RETRY_TIMERS = "rocketmq.retry_times";
    }
}
