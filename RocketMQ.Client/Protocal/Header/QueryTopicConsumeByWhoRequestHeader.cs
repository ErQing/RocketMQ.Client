using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RocketMQ.Client//.Protocal.Header
{
    public class QueryTopicConsumeByWhoRequestHeader : CommandCustomHeader
    {
        [CFNotNull]
        public String topic { get; set; }

        //@Override
        public void checkFields() //throws RemotingCommandException
        {

        }
    }
}
