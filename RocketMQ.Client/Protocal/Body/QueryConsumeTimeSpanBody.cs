using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RocketMQ.Client//.Protocal.Header
{
    public class QueryConsumeTimeSpanBody : RemotingSerializable
    {
        public List<QueueTimeSpan> consumeTimeSpanSet { get; set; } = new List<QueueTimeSpan>();
    }
}
