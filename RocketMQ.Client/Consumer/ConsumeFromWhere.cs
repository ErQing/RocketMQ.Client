using System;

namespace RocketMQ.Client
{
    public enum ConsumeFromWhere
    {
        CONSUME_FROM_LAST_OFFSET,
        [Obsolete]//@Deprecated
        CONSUME_FROM_LAST_OFFSET_AND_FROM_MIN_WHEN_BOOT_FIRST,
        [Obsolete]//@Deprecated
        CONSUME_FROM_MIN_OFFSET,
        [Obsolete]//@Deprecated
        CONSUME_FROM_MAX_OFFSET,
        CONSUME_FROM_FIRST_OFFSET,
        CONSUME_FROM_TIMESTAMP,
    }
}
