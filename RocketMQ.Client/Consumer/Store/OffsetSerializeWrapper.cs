using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RocketMQ.Client//.Consumer.Store
{
    public class OffsetSerializeWrapper : RemotingSerializable
    {
        private ConcurrentDictionary<MessageQueue, AtomicLong> offsetTable =
        new ConcurrentDictionary<MessageQueue, AtomicLong>();

        public ConcurrentDictionary<MessageQueue, AtomicLong> getOffsetTable()
        {
            return offsetTable;
        }

        public void setOffsetTable(ConcurrentDictionary<MessageQueue, AtomicLong> offsetTable)
        {
            this.offsetTable = offsetTable;
        }
    }
}
