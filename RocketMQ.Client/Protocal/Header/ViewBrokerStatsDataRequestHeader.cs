using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RocketMQ.Client//.Protocal.Header
{
    public class ViewBrokerStatsDataRequestHeader : CommandCustomHeader
    {
        [CFNotNull]
        public String statsName { get; set; }
        [CFNotNull]
        public String statsKey { get; set; }

        //@Override
        public void checkFields() //throws RemotingCommandException
        {

        }
    }
}
