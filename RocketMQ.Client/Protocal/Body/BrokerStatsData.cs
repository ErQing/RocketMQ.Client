using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RocketMQ.Client//.Protocal.Body
{
    public class BrokerStatsData : RemotingSerializable
    {
        public BrokerStatsItem statsMinute { get; set; }
        public BrokerStatsItem statsHour { get; set; }
        public BrokerStatsItem statsDay { get; set; }      
    }
}
