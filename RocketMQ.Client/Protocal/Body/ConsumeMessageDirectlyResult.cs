using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RocketMQ.Client
{
    public class ConsumeMessageDirectlyResult : RemotingSerializable
    {
        public bool order { get; set; } = false; 
        public bool autoCommit { get; set; } = true;
        public CMResult consumeResult { get; set; }
        public String remark { get; set; }
        public long spentTimeMills { get; set; }
        //@Override
        public override String ToString()
        {
            return "ConsumeMessageDirectlyResult [order=" + order + ", autoCommit=" + autoCommit
                + ", consumeResult=" + consumeResult + ", remark=" + remark + ", spentTimeMills="
                + spentTimeMills + "]";
        }
    }
}
