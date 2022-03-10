namespace RocketMQ.Client
{
    public class PullSysFlag
    {
        private readonly static int FLAG_COMMIT_OFFSET = 0x1;
        private readonly static int FLAG_SUSPEND = 0x1 << 1;
        private readonly static int FLAG_SUBSCRIPTION = 0x1 << 2;
        private readonly static int FLAG_CLASS_FILTER = 0x1 << 3;
        private readonly static int FLAG_LITE_PULL_MESSAGE = 0x1 << 4;

        public static int buildSysFlag(bool commitOffset, bool suspend, bool subscription, bool classFilter)
        {
            int flag = 0;

            if (commitOffset)
            {
                flag |= FLAG_COMMIT_OFFSET;
            }

            if (suspend)
            {
                flag |= FLAG_SUSPEND;
            }

            if (subscription)
            {
                flag |= FLAG_SUBSCRIPTION;
            }

            if (classFilter)
            {
                flag |= FLAG_CLASS_FILTER;
            }

            return flag;
        }

        public static int buildSysFlag(bool commitOffset, bool suspend, bool subscription, bool classFilter, bool litePull)
        {
            int flag = buildSysFlag(commitOffset, suspend, subscription, classFilter);

            if (litePull)
            {
                flag |= FLAG_LITE_PULL_MESSAGE;
            }

            return flag;
        }

        public static int clearCommitOffsetFlag(int sysFlag)
        {
            return sysFlag & (~FLAG_COMMIT_OFFSET);
        }

        public static bool hasCommitOffsetFlag(int sysFlag)
        {
            return (sysFlag & FLAG_COMMIT_OFFSET) == FLAG_COMMIT_OFFSET;
        }

        public static bool hasSuspendFlag(int sysFlag)
        {
            return (sysFlag & FLAG_SUSPEND) == FLAG_SUSPEND;
        }

        public static bool hasSubscriptionFlag(int sysFlag)
        {
            return (sysFlag & FLAG_SUBSCRIPTION) == FLAG_SUBSCRIPTION;
        }

        public static bool hasClassFilterFlag(int sysFlag)
        {
            return (sysFlag & FLAG_CLASS_FILTER) == FLAG_CLASS_FILTER;
        }

        public static bool hasLitePullFlag(int sysFlag)
        {
            return (sysFlag & FLAG_LITE_PULL_MESSAGE) == FLAG_LITE_PULL_MESSAGE;
        }
    }
}
