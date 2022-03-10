using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RocketMQ.Client
{
    public class LockBatchResponseBody : RemotingSerializable
    {
        public HashSet<MessageQueue> lockOKMQSet { get; set; } = new HashSet<MessageQueue>();
    }
}
