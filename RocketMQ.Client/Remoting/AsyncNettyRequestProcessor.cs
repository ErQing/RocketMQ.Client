using DotNetty.Transport.Channels;

namespace RocketMQ.Client
{
    public abstract class AsyncNettyRequestProcessor : NettyRequestProcessor
    {
        public void asyncProcessRequest(IChannelHandlerContext ctx, RemotingCommand request, RemotingResponseCallback responseCallback)
        {
            RemotingCommand response = processRequest(ctx, request);
            responseCallback.Callback(response);
        }

        public abstract RemotingCommand processRequest(IChannelHandlerContext ctx, RemotingCommand request);

        public abstract bool rejectRequest();
    }
}
