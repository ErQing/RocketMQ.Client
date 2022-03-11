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
        public string clientID{ get; set; }

        [CFNullable]
        public string producerGroup{ get; set; }
        [CFNullable]
        public string consumerGroup{ get; set; }
        public void checkFields() //throws RemotingCommandException
        {

        }
    }
}
