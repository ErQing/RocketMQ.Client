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
        public string consumerGroup{ get; set; }
        [CFNotNull]
        public string topic{ get; set; }
        [CFNotNull]
        public string className{ get; set; }
        [CFNotNull]
        public int classCRC{ get; set; }

        //@Override
        public void checkFields() //throws RemotingCommandException
        {
        }       
    }
}
