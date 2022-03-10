using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RocketMQ.Client//.Protocal.Header
{
    public class GetConsumerConnectionListRequestHeader : CommandCustomHeader
    {
        [CFNotNull]
        //[CFNotNull]
        public String consumerGroup { get; set; }

        //@Override
        public void checkFields() //throws RemotingCommandException
        {
            // To change body of implemented methods use File | Settings | File
            // Templates.
        }
    }
}
