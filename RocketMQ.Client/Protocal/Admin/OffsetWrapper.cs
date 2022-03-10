using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RocketMQ.Client
{
    public class OffsetWrapper
    {
        public long brokerOffset{ get; set; }
        public long consumerOffset{ get; set; }
        public long lastTimestamp{ get; set; }
    }
}
