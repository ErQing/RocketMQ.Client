using System;
using System.Collections.Concurrent;

namespace RocketMQ.Client
{
    public class BlockingQueue<T> : BlockingCollection<T>
    {

        private BlockingQueue(ConcurrentQueue<T> queue) 
            : base(queue)
        {
            
        }

        public static BlockingQueue<T> Create()
        {
            return new BlockingQueue<T>(new ConcurrentQueue<T>());
        }

        public T Poll(long timeout, TimeUnit mILLISECONDS)
        {
            this.TryTake(out var res, TimeSpan.FromMilliseconds(timeout));
            return res;
        }

        public void Put(T item)
        {
            Add(item);
        }

    }
}
