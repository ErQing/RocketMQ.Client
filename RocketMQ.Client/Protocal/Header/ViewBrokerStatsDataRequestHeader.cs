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
        public string statsName { get; set; }
        [CFNotNull]
        public string statsKey { get; set; }

        //@Override
        public void checkFields() //throws RemotingCommandException
        {

        }
    }
}
