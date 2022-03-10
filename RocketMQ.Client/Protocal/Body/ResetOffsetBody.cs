using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RocketMQ.Client//.Protocal.Body
{
    public class ResetOffsetBody : RemotingSerializable
    {
        public HashMap<MessageQueue, long> offsetTable { get; set; }
    }
}
