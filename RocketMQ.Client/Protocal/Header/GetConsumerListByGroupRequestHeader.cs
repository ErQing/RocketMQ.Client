using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RocketMQ.Client
{
    public class GetConsumerListByGroupRequestHeader : CommandCustomHeader
    {
        [CFNotNull]
        public string consumerGroup { get; set; }

        public void checkFields() { }       
    }
}
