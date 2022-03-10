using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RocketMQ.Client
{
    public class ConsumeStatus
    {
        public double pullRT { get; set; }
        public double pullTPS { get; set; }
        public double consumeRT { get; set; }
        public double consumeOKTPS { get; set; }
        public double consumeFailedTPS { get; set; }
        public long consumeFailedMsgs { get; set; }
    }
}
