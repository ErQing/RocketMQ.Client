using System.Collections.Generic;

namespace RocketMQ.Client
{
    public class MessageConst
    {
        public readonly static string PROPERTY_KEYS = "KEYS";
        public readonly static string PROPERTY_TAGS = "TAGS";
        public readonly static string PROPERTY_WAIT_STORE_MSG_OK = "WAIT";
        public readonly static string PROPERTY_DELAY_TIME_LEVEL = "DELAY";
        public readonly static string PROPERTY_RETRY_TOPIC = "RETRY_TOPIC";
        public readonly static string PROPERTY_REAL_TOPIC = "REAL_TOPIC";
        public readonly static string PROPERTY_REAL_QUEUE_ID = "REAL_QID";
        public readonly static string PROPERTY_TRANSACTION_PREPARED = "TRAN_MSG";
        public readonly static string PROPERTY_PRODUCER_GROUP = "PGROUP";
        public readonly static string PROPERTY_MIN_OFFSET = "MIN_OFFSET";
        public readonly static string PROPERTY_MAX_OFFSET = "MAX_OFFSET";
        public readonly static string PROPERTY_BUYER_ID = "BUYER_ID";
        public readonly static string PROPERTY_ORIGIN_MESSAGE_ID = "ORIGIN_MESSAGE_ID";
        public readonly static string PROPERTY_TRANSFER_FLAG = "TRANSFER_FLAG";
        public readonly static string PROPERTY_CORRECTION_FLAG = "CORRECTION_FLAG";
        public readonly static string PROPERTY_MQ2_FLAG = "MQ2_FLAG";
        public readonly static string PROPERTY_RECONSUME_TIME = "RECONSUME_TIME";
        public readonly static string PROPERTY_MSG_REGION = "MSG_REGION";
        public readonly static string PROPERTY_TRACE_SWITCH = "TRACE_ON";
        public readonly static string PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX = "UNIQ_KEY";
        public readonly static string PROPERTY_MAX_RECONSUME_TIMES = "MAX_RECONSUME_TIMES";
        public readonly static string PROPERTY_CONSUME_START_TIMESTAMP = "CONSUME_START_TIME";
        public readonly static string PROPERTY_TRANSACTION_PREPARED_QUEUE_OFFSET = "TRAN_PREPARED_QUEUE_OFFSET";
        public readonly static string PROPERTY_TRANSACTION_CHECK_TIMES = "TRANSACTION_CHECK_TIMES";
        public readonly static string PROPERTY_CHECK_IMMUNITY_TIME_IN_SECONDS = "CHECK_IMMUNITY_TIME_IN_SECONDS";
        public readonly static string PROPERTY_INSTANCE_ID = "INSTANCE_ID";
        public readonly static string PROPERTY_CORRELATION_ID = "CORRELATION_ID";
        public readonly static string PROPERTY_MESSAGE_REPLY_TO_CLIENT = "REPLY_TO_CLIENT";
        public readonly static string PROPERTY_MESSAGE_TTL = "TTL";
        public readonly static string PROPERTY_REPLY_MESSAGE_ARRIVE_TIME = "ARRIVE_TIME";
        public readonly static string PROPERTY_PUSH_REPLY_TIME = "PUSH_REPLY_TIME";
        public readonly static string PROPERTY_CLUSTER = "CLUSTER";
        public readonly static string PROPERTY_MESSAGE_TYPE = "MSG_TYPE";

        public readonly static string KEY_SEPARATOR = " ";

        public readonly static HashSet<string> string_HASH_SET = new HashSet<string>();

        static MessageConst()
        {
            string_HASH_SET.Add(PROPERTY_TRACE_SWITCH);
            string_HASH_SET.Add(PROPERTY_MSG_REGION);
            string_HASH_SET.Add(PROPERTY_KEYS);
            string_HASH_SET.Add(PROPERTY_TAGS);
            string_HASH_SET.Add(PROPERTY_WAIT_STORE_MSG_OK);
            string_HASH_SET.Add(PROPERTY_DELAY_TIME_LEVEL);
            string_HASH_SET.Add(PROPERTY_RETRY_TOPIC);
            string_HASH_SET.Add(PROPERTY_REAL_TOPIC);
            string_HASH_SET.Add(PROPERTY_REAL_QUEUE_ID);
            string_HASH_SET.Add(PROPERTY_TRANSACTION_PREPARED);
            string_HASH_SET.Add(PROPERTY_PRODUCER_GROUP);
            string_HASH_SET.Add(PROPERTY_MIN_OFFSET);
            string_HASH_SET.Add(PROPERTY_MAX_OFFSET);
            string_HASH_SET.Add(PROPERTY_BUYER_ID);
            string_HASH_SET.Add(PROPERTY_ORIGIN_MESSAGE_ID);
            string_HASH_SET.Add(PROPERTY_TRANSFER_FLAG);
            string_HASH_SET.Add(PROPERTY_CORRECTION_FLAG);
            string_HASH_SET.Add(PROPERTY_MQ2_FLAG);
            string_HASH_SET.Add(PROPERTY_RECONSUME_TIME);
            string_HASH_SET.Add(PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX);
            string_HASH_SET.Add(PROPERTY_MAX_RECONSUME_TIMES);
            string_HASH_SET.Add(PROPERTY_CONSUME_START_TIMESTAMP);
            string_HASH_SET.Add(PROPERTY_INSTANCE_ID);
            string_HASH_SET.Add(PROPERTY_CORRELATION_ID);
            string_HASH_SET.Add(PROPERTY_MESSAGE_REPLY_TO_CLIENT);
            string_HASH_SET.Add(PROPERTY_MESSAGE_TTL);
            string_HASH_SET.Add(PROPERTY_REPLY_MESSAGE_ARRIVE_TIME);
            string_HASH_SET.Add(PROPERTY_PUSH_REPLY_TIME);
            string_HASH_SET.Add(PROPERTY_CLUSTER);
            string_HASH_SET.Add(PROPERTY_MESSAGE_TYPE);
        }

    }
}
