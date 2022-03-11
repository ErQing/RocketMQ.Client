using System;

namespace RocketMQ.Client
{
    public class ProcessQueueInfo
    {
        private long commitOffset;

        private long cachedMsgMinOffset;
        private long cachedMsgMaxOffset;
        private int cachedMsgCount;
        private int cachedMsgSizeInMiB;

        private long transactionMsgMinOffset;
        private long transactionMsgMaxOffset;
        private int transactionMsgCount;

        private bool locked;
        private long tryUnlockTimes;
        private long lastLockTimestamp;

        private bool droped;
        private long lastPullTimestamp;
        private long lastConsumeTimestamp;

        public long getCommitOffset()
        {
            return commitOffset;
        }

        public void setCommitOffset(long commitOffset)
        {
            this.commitOffset = commitOffset;
        }

        public long getCachedMsgMinOffset()
        {
            return cachedMsgMinOffset;
        }

        public void setCachedMsgMinOffset(long cachedMsgMinOffset)
        {
            this.cachedMsgMinOffset = cachedMsgMinOffset;
        }

        public long getCachedMsgMaxOffset()
        {
            return cachedMsgMaxOffset;
        }

        public void setCachedMsgMaxOffset(long cachedMsgMaxOffset)
        {
            this.cachedMsgMaxOffset = cachedMsgMaxOffset;
        }

        public int getCachedMsgCount()
        {
            return cachedMsgCount;
        }

        public void setCachedMsgCount(int cachedMsgCount)
        {
            this.cachedMsgCount = cachedMsgCount;
        }

        public long getTransactionMsgMinOffset()
        {
            return transactionMsgMinOffset;
        }

        public void setTransactionMsgMinOffset(long transactionMsgMinOffset)
        {
            this.transactionMsgMinOffset = transactionMsgMinOffset;
        }

        public long getTransactionMsgMaxOffset()
        {
            return transactionMsgMaxOffset;
        }

        public void setTransactionMsgMaxOffset(long transactionMsgMaxOffset)
        {
            this.transactionMsgMaxOffset = transactionMsgMaxOffset;
        }

        public int getTransactionMsgCount()
        {
            return transactionMsgCount;
        }

        public void setTransactionMsgCount(int transactionMsgCount)
        {
            this.transactionMsgCount = transactionMsgCount;
        }

        public bool isLocked()
        {
            return locked;
        }

        public void setLocked(bool locked)
        {
            this.locked = locked;
        }

        public long getTryUnlockTimes()
        {
            return tryUnlockTimes;
        }

        public void setTryUnlockTimes(long tryUnlockTimes)
        {
            this.tryUnlockTimes = tryUnlockTimes;
        }

        public long getLastLockTimestamp()
        {
            return lastLockTimestamp;
        }

        public void setLastLockTimestamp(long lastLockTimestamp)
        {
            this.lastLockTimestamp = lastLockTimestamp;
        }

        public bool isDroped()
        {
            return droped;
        }

        public void setDroped(bool droped)
        {
            this.droped = droped;
        }

        public long getLastPullTimestamp()
        {
            return lastPullTimestamp;
        }

        public void setLastPullTimestamp(long lastPullTimestamp)
        {
            this.lastPullTimestamp = lastPullTimestamp;
        }

        public long getLastConsumeTimestamp()
        {
            return lastConsumeTimestamp;
        }

        public void setLastConsumeTimestamp(long lastConsumeTimestamp)
        {
            this.lastConsumeTimestamp = lastConsumeTimestamp;
        }

        public int getCachedMsgSizeInMiB()
        {
            return cachedMsgSizeInMiB;
        }

        public void setCachedMsgSizeInMiB(int cachedMsgSizeInMiB)
        {
            this.cachedMsgSizeInMiB = cachedMsgSizeInMiB;
        }

        public override string ToString()
        {
            return "ProcessQueueInfo [commitOffset=" + commitOffset + ", cachedMsgMinOffset="
                + cachedMsgMinOffset + ", cachedMsgMaxOffset=" + cachedMsgMaxOffset
                + ", cachedMsgCount=" + cachedMsgCount + ", cachedMsgSizeInMiB=" + cachedMsgSizeInMiB
                + ", transactionMsgMinOffset=" + transactionMsgMinOffset
                + ", transactionMsgMaxOffset=" + transactionMsgMaxOffset + ", transactionMsgCount="
                + transactionMsgCount + ", locked=" + locked + ", tryUnlockTimes=" + tryUnlockTimes
                + ", lastLockTimestamp=" + UtilAll.timeMillisToHumanString(lastLockTimestamp) + ", droped="
                + droped + ", lastPullTimestamp=" + UtilAll.timeMillisToHumanString(lastPullTimestamp)
                + ", lastConsumeTimestamp=" + UtilAll.timeMillisToHumanString(lastConsumeTimestamp) + "]";
        }

    }
}
