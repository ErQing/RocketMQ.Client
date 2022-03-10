using System;
using System.Collections.Concurrent;

namespace RocketMQ.Client
{
    public class LinkedBlockingQueue<T> : ConcurrentQueue<T>
    {
        internal DefaultLitePullConsumerImpl.ConsumeRequest poll(long v, TimeUnit mILLISECONDS)
        {
            throw new NotImplementedException();
        }
    }
}
