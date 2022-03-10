using System;
using System.Collections.Generic;

namespace RocketMQ.Client
{

    public class PullResult
    {
        private readonly PullStatus pullStatus;
        private readonly long nextBeginOffset;
        private readonly long minOffset;
        private readonly long maxOffset;
        private List<MessageExt> msgFoundList;

        public PullResult(PullStatus pullStatus, long nextBeginOffset, long minOffset, long maxOffset, List<MessageExt> msgFoundList)
        {
            this.pullStatus = pullStatus;
            this.nextBeginOffset = nextBeginOffset;
            this.minOffset = minOffset;
            this.maxOffset = maxOffset;
            this.msgFoundList = msgFoundList;
        }

        public PullStatus getPullStatus()
        {
            return pullStatus;
        }

        public long getNextBeginOffset()
        {
            return nextBeginOffset;
        }

        public long getMinOffset()
        {
            return minOffset;
        }

        public long getMaxOffset()
        {
            return maxOffset;
        }

        public List<MessageExt> getMsgFoundList()
        {
            return msgFoundList;
        }

        public void setMsgFoundList(List<MessageExt> msgFoundList)
        {
            this.msgFoundList = msgFoundList;
        }

        public override String ToString()
        {
            return "PullResult [pullStatus=" + pullStatus + ", nextBeginOffset=" + nextBeginOffset
                + ", minOffset=" + minOffset + ", maxOffset=" + maxOffset + ", msgFoundList="
                + (msgFoundList == null ? 0 : msgFoundList.Count) + "]";
        }

    }
}
