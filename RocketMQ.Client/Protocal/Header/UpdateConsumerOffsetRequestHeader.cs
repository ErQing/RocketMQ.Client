using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RocketMQ.Client
{
    public class UpdateConsumerOffsetRequestHeader : CommandCustomHeader
    {
        [CFNotNull]
        public String consumerGroup { get; set; }
        [CFNotNull]
        public String topic { get; set; }
        [CFNotNull]
        public int queueId { get; set; }
        [CFNotNull]
        public long commitOffset { get; set; }

        public void checkFields() //throws RemotingCommandException
        {
        }
    }
}
