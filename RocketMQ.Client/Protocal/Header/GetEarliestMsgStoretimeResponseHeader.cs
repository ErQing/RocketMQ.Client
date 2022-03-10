using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RocketMQ.Client
{
    public class GetEarliestMsgStoretimeResponseHeader : CommandCustomHeader
    {
        [CFNotNull]
        public long timestamp { get; set; }

        //@Override
        public void checkFields() //throws RemotingCommandException
        {
        }
    }
}
