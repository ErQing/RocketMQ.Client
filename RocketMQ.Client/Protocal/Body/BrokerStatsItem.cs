using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RocketMQ.Client//.Protocal.Body
{
    public class BrokerStatsItem
    {
        public long sum { get; set; }
        public double tps { get; set; }
        public double avgpt { get; set; }
    }
}
