using System;
using System.Text;

namespace RocketMQ.Client
{
    public class DataVersion : RemotingSerializable
    {
        public long timestamp { get; set; } = Sys.currentTimeMillis();
        public AtomicLong counter { get; } = new AtomicLong(0);

        public void assignNewOne(DataVersion dataVersion)
        {
            this.timestamp = dataVersion.timestamp;
            this.counter.Set(dataVersion.counter.Get());
        }

        public void nextVersion()
        {
            this.timestamp = Sys.currentTimeMillis();
            this.counter.IncrementAndGet();
        }
        public override bool Equals(Object o)
        {
            if (this == o)
                return true;
            if (o == null || GetType() != o.GetType())
                return false;

            DataVersion that = (DataVersion)o;

            if (timestamp != that.timestamp)
            {
                return false;
            }

            if (counter != null && that.counter != null)
            {
                return counter.LongValue() == that.counter.LongValue();
            }

            return (null == counter) && (null == that.counter);
        }

        /// <summary>
        /// use ulong instead >>> or <<<
        /// https://stackoverflow.com/questions/1880172/equivalent-of-java-triple-shift-operator-in-c
        /// </summary>
        /// <returns></returns>
        public override int GetHashCode()
        {
            //int result = (int)(timestamp ^ (timestamp >>> 32)); 
            int result = (int)(timestamp ^ (timestamp.UnsignedRightShift(32))); 
            if (null != counter)
            {
                long l = counter.Get();
                //result = 31 * result + (int)(l ^ (l >>> 32));
                result = 31 * result + (int)(l ^ (l.UnsignedRightShift(32)));
            }
            return result;
        }

        public override string ToString()
        {
            StringBuilder sb = new StringBuilder("DataVersion[");
            sb.Append("timestamp=").Append(timestamp);
            sb.Append(", counter=").Append(counter);
            sb.Append(']');
            return sb.ToString();
        }
    }
}
