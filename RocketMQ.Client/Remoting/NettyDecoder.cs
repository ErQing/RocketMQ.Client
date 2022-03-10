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
    public class NettyDecoder : LengthFieldBasedFrameDecoder
    {
        //private static final InternalLogger log = InternalLoggerFactory.getLogger(RemotingHelper.ROCKETMQ_REMOTING);
        static NLog.Logger log = NLog.LogManager.GetCurrentClassLogger();

        private static readonly int FRAME_MAX_LENGTH = int.Parse(Sys.getProperty("com.rocketmq.remoting.frameMaxLength", "16777216"));

        public NettyDecoder() : base(FRAME_MAX_LENGTH, 0, 4, 0, 4)
        {

        }

        protected override object Decode(IChannelHandlerContext ctx, IByteBuffer input)
        {
            IByteBuffer frame = null;
            try
            {
                frame = (IByteBuffer)base.Decode(ctx, input);
                if (null == frame)
                {
                    return null;
                }
                //ByteBuffer byteBuffer = frame.nioBuffer();
                ByteBuffer byteBuffer = ByteBuffer.wrap(frame.Array); //???
                return RemotingCommand.decode(byteBuffer);
            }
            catch (Exception e)
            {
                log.Error("decode exception, " + RemotingHelper.parseChannelRemoteAddr(ctx.Channel), e.ToString());
                RemotingUtil.closeChannel(ctx.Channel);
            }
            finally
            {
                if (null != frame)
                {
                    frame.Release();
                }
            }
            return null;
        }
    }
}
