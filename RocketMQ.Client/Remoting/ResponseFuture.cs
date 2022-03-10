using DotNetty.Transport.Channels;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RocketMQ.Client
{
    public class ResponseFuture
    {
        private readonly int opaque;
        private readonly IChannel processChannel;
        private readonly long timeoutMillis;
        private readonly InvokeCallback invokeCallback;
        private readonly long beginTimestamp = Sys.currentTimeMillis();
        //private readonly CountDownLatch countDownLatch = new CountDownLatch(1);
        private TaskCompletionSource<RemotingCommand> countDownLatch = new TaskCompletionSource<RemotingCommand>();
        //private readonly SemaphoreReleaseOnlyOnce once;

        private readonly AtomicBoolean executeCallbackOnlyOnce = new AtomicBoolean(false);
        private volatile RemotingCommand responseCommand;
        private volatile bool sendRequestOK = true;
        private volatile Exception cause;

        public ResponseFuture(IChannel channel, int opaque, long timeoutMillis, InvokeCallback invokeCallback/*, SemaphoreReleaseOnlyOnce once*/)
        {
            this.opaque = opaque;
            this.processChannel = channel;
            this.timeoutMillis = timeoutMillis;
            this.invokeCallback = invokeCallback;
            //this.once = once;
        }

        public void executeInvokeCallback()
        {
            if (invokeCallback != null)
            {
                if (this.executeCallbackOnlyOnce.compareAndSet(false, true))
                {
                    invokeCallback.OperationComplete(this);
                }
            }
        }

        public void release()
        {
            //if (this.once != null)
            //{
            //    this.once.release();
            //}
        }

        public bool isTimeout()
        {
            long diff = Sys.currentTimeMillis() - this.beginTimestamp;
            return diff > this.timeoutMillis;
        }

        ///<exception cref="ThreadInterruptedException"/>
        //public RemotingCommand waitResponse(long timeoutMillis)
        //{
        //    this.countDownLatch.await(timeoutMillis, TimeUnit.MILLISECONDS);
        //    return this.responseCommand;
        //}

        public async Task<RemotingCommand> waitResponse(long timeoutMillis)
        {
            return await countDownLatch.Task.WaitAsync(TimeSpan.FromMilliseconds(timeoutMillis));
        }

        public void putResponse(RemotingCommand responseCommand)
        {
            this.responseCommand = responseCommand;
            //this.countDownLatch.countDown();
            countDownLatch.TrySetResult(responseCommand);
        }

        public long getBeginTimestamp()
        {
            return beginTimestamp;
        }

        public bool isSendRequestOK()
        {
            return sendRequestOK;
        }

        public void setSendRequestOK(bool sendRequestOK)
        {
            this.sendRequestOK = sendRequestOK;
        }

        public long getTimeoutMillis()
        {
            return timeoutMillis;
        }

        public InvokeCallback getInvokeCallback()
        {
            return invokeCallback;
        }

        public Exception getCause()
        {
            return cause;
        }

        public void setCause(Exception cause)
        {
            this.cause = cause;
        }

        public RemotingCommand getResponseCommand()
        {
            return responseCommand;
        }

        public void setResponseCommand(RemotingCommand responseCommand)
        {
            this.responseCommand = responseCommand;
        }

        public int getOpaque()
        {
            return opaque;
        }

        public IChannel getProcessChannel()
        {
            return processChannel;
        }

        public override String ToString()
        {
            return "ResponseFuture [responseCommand=" + responseCommand
                + ", sendRequestOK=" + sendRequestOK
                + ", cause=" + cause
                + ", opaque=" + opaque
                + ", processChannel=" + processChannel
                + ", timeoutMillis=" + timeoutMillis
                + ", invokeCallback=" + invokeCallback
                + ", beginTimestamp=" + beginTimestamp
                + ", countDownLatch=" + countDownLatch + "]";
        }
    }
}
