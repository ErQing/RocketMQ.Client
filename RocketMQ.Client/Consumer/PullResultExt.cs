using System.Collections.Generic;

namespace RocketMQ.Client
{
    public class PullResultExt : PullResult
    {
        private readonly long suggestWhichBrokerId;
        private byte[] messageBinary;

        public PullResultExt(PullStatus pullStatus, long nextBeginOffset, long minOffset, long maxOffset,
            List<MessageExt> msgFoundList, long suggestWhichBrokerId, byte[] messageBinary)
            : base(pullStatus, nextBeginOffset, minOffset, maxOffset, msgFoundList)
        {
            this.suggestWhichBrokerId = suggestWhichBrokerId;
            this.messageBinary = messageBinary;
        }

        public byte[] getMessageBinary()
        {
            return messageBinary;
        }

        public void setMessageBinary(byte[] messageBinary)
        {
            this.messageBinary = messageBinary;
        }

        public long getSuggestWhichBrokerId()
        {
            return suggestWhichBrokerId;
        }
    }
}
