using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RocketMQ.Client
{
    public class ConsumeStats : RemotingSerializable
    {
        public Dictionary<MessageQueue, OffsetWrapper> offsetTable { get; set; } = new Dictionary<MessageQueue, OffsetWrapper>();
        public double consumeTps { get; set; } = 0;

        public long computeTotalDiff()
        {
            long diffTotal = 0L;

            //Iterator<Entry<MessageQueue, OffsetWrapper>> it = this.offsetTable.entrySet().iterator();
            //while (it.hasNext())
            foreach (var entry in this.offsetTable)
            {
                //Entry<MessageQueue, OffsetWrapper> next = it.next();
                long diff = entry.Value.brokerOffset - entry.Value.consumerOffset;
                diffTotal += diff;
            }

            return diffTotal;
        }
    }
}
