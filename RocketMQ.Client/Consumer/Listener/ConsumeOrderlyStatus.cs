using System;

namespace RocketMQ.Client
{
    public enum ConsumeOrderlyStatus
    {
        UNKNOWN,
        /**
         * Success consumption
         */
        SUCCESS,
        /**
         * Rollback consumption(only for binlog consumption)
         */
        [Obsolete]//@Deprecated
        ROLLBACK,
        /**
         * Commit offset(only for binlog consumption)
         */
        [Obsolete]//@Deprecated
        COMMIT,
        /**
         * Suspend current queue a moment
         * 挂起当前队列一会儿
         */
        SUSPEND_CURRENT_QUEUE_A_MOMENT
    }
}
