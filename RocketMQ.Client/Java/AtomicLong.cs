﻿namespace RocketMQ.Client
{
    using System.Threading;

    public class AtomicLong
    {

        private long _value;

        public AtomicLong()
            : this(0)
        {

        }

        public AtomicLong(long value)
        {
            _value = value;
        }

        public long Get()
        {
            return Interlocked.Read(ref _value);
        }

        public long LongValue()
        {
            return Get();
        }

        public void Set(long value)
        {
            Interlocked.Exchange(ref _value, value);
        }

        public long GetAndSet(long value)
        {
            return Interlocked.Exchange(ref _value, value);
        }

        public bool CompareAndSet(long expected, long result)
        {
            return Interlocked.CompareExchange(ref _value, result, expected) == expected;
        }

        public long IncrementAndGet()
        {
            return Interlocked.Add(ref _value, 1);
        }

        public long AddAndGet(long delta)
        {
            return Interlocked.Add(ref _value, delta);
        }

        public long GetAndAdd(long delta)
        {
            for (; ; )
            {
                long current = Get();
                long next = current + delta;
                if (CompareAndSet(current, next))
                {
                    return current;
                }
            }
        }

        public long GetAndIncrement(long delta)
        {
            return GetAndAdd(delta);
        }

        public long GetAndIncrement()
        {
            return GetAndAdd(1);
        }

        public long Increment()
        {
            return GetAndAdd(1);
        }

        public long Decrement()
        {
            return GetAndAdd(-1);
        }

        public long PreIncrement()
        {
            return Interlocked.Increment(ref _value);
        }

        public long PreDecrement()
        {
            return Interlocked.Decrement(ref _value);
        }

        public static implicit operator long(AtomicLong value)
        {
            return value.Get();
        }

    }
}
