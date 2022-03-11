using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RocketMQ.Client//.Protocal.Header
{
    public class GetConsumeStatsRequestHeader : CommandCustomHeader
    {
        [CFNotNull]
        public string consumerGroup { get; set; }
        public string topic { get; set; }

        //@Override
        public void checkFields() //throws RemotingCommandException
        {
        }
    }
}
