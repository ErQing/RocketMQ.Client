using DotNetty.Transport.Channels;
using System;

namespace RocketMQ.Client
{
    public class RequestTask : IRunnable
    {
        private readonly Runnable runnable;
        private readonly ulong createTimestamp = Sys.currentTimeMillisUnsigned();
        private readonly IChannel channel;
        private readonly RemotingCommand request;
        private bool stopRun = false;

        public RequestTask(Runnable runnable, IChannel channel, RemotingCommand request)
        {
            this.runnable = runnable;
            this.channel = channel;
            this.request = request;
        }

        //@Override
        public override int GetHashCode()
        {
            int result = runnable != null ? runnable.GetHashCode() : 0;
            result = 31 * result + (int)(getCreateTimestamp() ^ (getCreateTimestamp() >> 32)); //ulong
            result = 31 * result + (channel != null ? channel.GetHashCode() : 0);
            result = 31 * result + (request != null ? request.GetHashCode() : 0);
            result = 31 * result + (isStopRun() ? 1 : 0);
            return result;
        }

        //@Override
        public override bool Equals(Object o)
        {
            if (this == o)
                return true;
            if (!(o is RequestTask))
                return false;

            RequestTask that = (RequestTask)o;

            if (getCreateTimestamp() != that.getCreateTimestamp())
                return false;
            if (isStopRun() != that.isStopRun())
                return false;
            if (channel != null ? !channel.Equals(that.channel) : that.channel != null)
                return false;
            return request != null ? request.getOpaque() == that.request.getOpaque() : that.request == null;

        }

        public long getCreateTimestamp()
        {
            return (long)createTimestamp;
        }

        public bool isStopRun()
        {
            return stopRun;
        }

        public void setStopRun(bool stopRun)
        {
            this.stopRun = stopRun;
        }

        //@Override
        public void run()
        {
            if (!this.stopRun)
                this.runnable.Run();
        }

        public void returnResponse(int code, String remark)
        {
            RemotingCommand response = RemotingCommand.createResponseCommand(code, remark);
            response.setOpaque(request.getOpaque());
            //this.channel.writeAndFlush(response);
            this.channel.WriteAndFlushAsync(response);
        }
    }
}
