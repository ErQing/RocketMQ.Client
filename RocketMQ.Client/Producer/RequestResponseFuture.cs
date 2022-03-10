using System;
using System.Threading;
using System.Threading.Tasks;

namespace RocketMQ.Client
{
    public class RequestResponseFuture
    {
        private readonly String correlationId;
        private readonly RequestCallback requestCallback;
        private readonly long beginTimestamp = Sys.currentTimeMillis();
        private readonly Message requestMsg = null;
        private long timeoutMillis;
        //private CountDownLatch countDownLatch = new CountDownLatch(1); //CountdownEvent
        private TaskCompletionSource<Message> countDownLatch = new TaskCompletionSource<Message>();
        private volatile Message responseMsg = null;
        private volatile bool sendRequestOk = true;
        private volatile Exception cause = null;

        public RequestResponseFuture(String correlationId, long timeoutMillis, RequestCallback requestCallback)
        {
            this.correlationId = correlationId;
            this.timeoutMillis = timeoutMillis;
            this.requestCallback = requestCallback;
        }

        public void executeRequestCallback()
        {
            if (requestCallback != null)
            {
                if (sendRequestOk && cause == null)
                {
                    requestCallback.onSuccess(responseMsg);
                }
                else
                {
                    requestCallback.onException(cause);
                }
            }
        }

        public bool isTimeout()
        {
            long diff = Sys.currentTimeMillis() - this.beginTimestamp;
            return diff > this.timeoutMillis;
        }

        ///<exception cref="ThreadInterruptedException"/>
        public async Task<Message> waitResponseMessage(long timeout)
        {
            return await countDownLatch.Task.WaitAsync(TimeSpan.FromMilliseconds(timeout));
            //return this.responseMsg;
        }

        public void putResponseMessage(Message responseMsg)
        {
            this.responseMsg = responseMsg;
            //this.countDownLatch.countDown();
            countDownLatch.TrySetResult(responseMsg);
        }

        public String getCorrelationId()
        {
            return correlationId;
        }

        public long getTimeoutMillis()
        {
            return timeoutMillis;
        }

        public void setTimeoutMillis(long timeoutMillis)
        {
            this.timeoutMillis = timeoutMillis;
        }

        public RequestCallback getRequestCallback()
        {
            return requestCallback;
        }

        public long getBeginTimestamp()
        {
            return beginTimestamp;
        }

        public TaskCompletionSource<Message> getCountDownLatch()
        {
            return countDownLatch;
        }

        public void setCountDownLatch(TaskCompletionSource<Message> countDownLatch)
        {
            this.countDownLatch = countDownLatch;
        }

        public Message getResponseMsg()
        {
            return responseMsg;
        }

        public void setResponseMsg(Message responseMsg)
        {
            this.responseMsg = responseMsg;
        }

        public bool isSendRequestOk()
        {
            return sendRequestOk;
        }

        public void setSendRequestOk(bool sendRequestOk)
        {
            this.sendRequestOk = sendRequestOk;
        }

        public Message getRequestMsg()
        {
            return requestMsg;
        }

        public Exception getCause()
        {
            return cause;
        }

        public void setCause(Exception cause)
        {
            this.cause = cause;
        }
    }
}
