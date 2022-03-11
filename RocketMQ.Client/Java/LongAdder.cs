using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RocketMQ.Client
{

    /// <summary>
    /// 暂时用AtomicLong来实现
    /// </summary>
    public class LongAdder : AtomicLong
    {
        public long Sum()
        {
            return Get();
        }

        public void Add(int delta)
        {
            AddAndGet(delta);
        }

    }
}
