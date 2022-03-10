using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RocketMQ.Client
{
    public class UnregisterClientRequestHeader : CommandCustomHeader
    {
        [CFNotNull]
        public String clientID{ get; set; }

        [CFNullable]
        public String producerGroup{ get; set; }
        [CFNullable]
        public String consumerGroup{ get; set; }
        public void checkFields() //throws RemotingCommandException
        {

        }
    }
}
