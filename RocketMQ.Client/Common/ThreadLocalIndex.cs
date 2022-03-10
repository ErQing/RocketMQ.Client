using System;
using System.Threading;

namespace RocketMQ.Client
{
    public class ThreadLocalIndex
    {
        private readonly ThreadLocal<int> threadLocalIndex = new ThreadLocal<int>();
        private readonly Random random = new Random();

        public int incrementAndGet()
        {
            int index = this.threadLocalIndex.Value;
            if (null == index)
            {
                //index = Math.Abs(random.nextInt());
                index = Math.Abs(random.Next(32));
                this.threadLocalIndex.Value = index;
            }

            this.threadLocalIndex.Value = ++index;
            return Math.Abs(index);
        }

        public override String ToString()
        {
            return "ThreadLocalIndex{" +
                "threadLocalIndex=" + threadLocalIndex.Value +
                '}';
        }
    }
}
