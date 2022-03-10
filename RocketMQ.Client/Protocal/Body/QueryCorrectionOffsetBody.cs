using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RocketMQ.Client//.Protocal.Body
{
    public class QueryCorrectionOffsetBody : RemotingSerializable
    {
        public Dictionary<int, long> correctionOffsets { get; set; } = new Dictionary<int, long>();
    }
}
