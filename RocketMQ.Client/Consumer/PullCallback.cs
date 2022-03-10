using System;

namespace RocketMQ.Client
{
    //public interface PullCallback
    //{
    //    void onSuccess(PullResult pullResult);

    //    void onException(Exception e);
    //}

    public class PullCallback
    {
        public Action<PullResult> OnSuccess { get; set; }
        public Action<Exception> OnException { get; set; }
    }

}
