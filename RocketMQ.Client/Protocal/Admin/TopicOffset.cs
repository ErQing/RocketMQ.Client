using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RocketMQ.Client//.Protocal.Admin
{
    public class TopicOffset
    {
        public long minOffset{ get; set; }
        public long maxOffset{ get; set; }
        public long lastUpdateTimestamp{ get; set; }
    }
}
