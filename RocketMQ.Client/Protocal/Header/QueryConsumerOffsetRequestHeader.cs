using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RocketMQ.Client
{
    public class QueryConsumerOffsetRequestHeader : CommandCustomHeader
    {
        [CFNotNull]
        public String consumerGroup{ get; set; }
        [CFNotNull]
        public String topic{ get; set; }
        [CFNotNull]
        public int queueId{ get; set; }

        //@Override
        public void checkFields()
        {
        }
    }
}
