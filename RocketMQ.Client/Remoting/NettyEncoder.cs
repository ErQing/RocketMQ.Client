using DotNetty.Buffers;
using DotNetty.Codecs;
using DotNetty.Transport.Channels;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RocketMQ.Client
{
    public class NettyEncoder : MessageToByteEncoder<RemotingCommand>
    {
        //rivate static final InternalLogger log = InternalLoggerFactory.getLogger(RemotingHelper.ROCKETMQ_REMOTING);
        static NLog.Logger log = NLog.LogManager.GetCurrentClassLogger();

        //@Override
        protected override void Encode(IChannelHandlerContext ctx, RemotingCommand remotingCommand, IByteBuffer output)
        {
            try
            {
                ByteBuffer header = remotingCommand.encodeHeader();
                output.WriteBytes(header.Data);
                byte[] body = remotingCommand.getBody();
                if (body != null)
                {
                    output.WriteBytes(body);
                }
            }
            catch (Exception e)
            {
                log.Error("encode exception, " + RemotingHelper.parseChannelRemoteAddr(ctx.Channel), e.ToString());
                if (remotingCommand != null)
                {
                    log.Error(remotingCommand.ToString());
                }
                RemotingUtil.closeChannel(ctx.Channel);
            }
        }
    }
}
