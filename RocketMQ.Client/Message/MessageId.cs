using System.Net;

namespace RocketMQ.Client
{
    public class MessageId
    {
        private IPEndPoint address;
        private long offset;

        public MessageId(IPEndPoint address, long offset)
        {
            this.address = address;
            this.offset = offset;
        }

        public IPEndPoint getAddress()
        {
            return address;
        }

        public void setAddress(IPEndPoint address)
        {
            this.address = address;
        }

        public long getOffset()
        {
            return offset;
        }

        public void setOffset(long offset)
        {
            this.offset = offset;
        }
    }
}
