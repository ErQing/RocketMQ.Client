using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RocketMQ.Client
{
    public class GetEarliestMsgStoretimeRequestHeader : CommandCustomHeader
    {
        [CFNotNull]
        public string topic { get; set; }
        [CFNotNull]
        public int queueId { get; set; }

        //@Override
        public void checkFields() //throws RemotingCommandException
        {
        }
    }
}
