using DotNetty.Transport.Channels;
using System;

namespace RocketMQ.Client
{
    public interface NettyRequestProcessor
    {

        ///<exception cref="Exception"/>
        RemotingCommand processRequest(IChannelHandlerContext ctx, RemotingCommand request); 

        bool rejectRequest();
    }
}
