using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RocketMQ.Client//.Protocal.Header
{
    public class RegisterMessageFilterClassRequestHeader : CommandCustomHeader
    {

        [CFNotNull]
        public String consumerGroup{ get; set; }
        [CFNotNull]
        public String topic{ get; set; }
        [CFNotNull]
        public String className{ get; set; }
        [CFNotNull]
        public int classCRC{ get; set; }

        //@Override
        public void checkFields() //throws RemotingCommandException
        {
        }       
    }
}
