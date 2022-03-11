using DotNetty.Transport.Channels;
using System;

namespace RocketMQ.Client
{
    public class NettyEvent
    {
        private readonly NettyEventType type;
        private readonly string remoteAddr;
        private readonly IChannel channel;

        public NettyEvent(NettyEventType type, string remoteAddr, IChannel channel)
        {
            this.type = type;
            this.remoteAddr = remoteAddr;
            this.channel = channel;
        }

        public NettyEventType getType()
        {
            return type;
        }

        public string getRemoteAddr()
        {
            return remoteAddr;
        }

        public IChannel getChannel()
        {
            return channel;
        }

        public override string ToString()
        {
            return "NettyEvent [type=" + type + ", remoteAddr=" + remoteAddr + ", channel=" + channel + "]";
        }
    }
}
