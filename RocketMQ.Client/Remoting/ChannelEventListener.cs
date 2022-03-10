using DotNetty.Transport.Channels;

namespace RocketMQ.Client
{
    public interface ChannelEventListener
    {
        void onChannelConnect(string remoteAddr, IChannel channel);

        void onChannelClose(string remoteAddr, IChannel channel);

        void onChannelException(string remoteAddr, IChannel channel);

        void onChannelIdle(string remoteAddr, IChannel channel);
    }
}
