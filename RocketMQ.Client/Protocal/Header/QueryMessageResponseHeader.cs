using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RocketMQ.Client//.Protocal.Header
{
    public class QueryMessageResponseHeader : CommandCustomHeader
    {
        [CFNotNull]
        public long indexLastUpdateTimestamp { get; set; }
        [CFNotNull]
        public long indexLastUpdatePhyoffset { get; set; }

        //@Override
        public void checkFields() //throws RemotingCommandException
        {
        }
    }
}
