using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RocketMQ.Client
{
    public class NotifyConsumerIdsChangedRequestHeader : CommandCustomHeader
    {
        [CFNotNull]
        public string consumerGroup { get; set; }

        //@Override
        public void checkFields()
        {
        }
    }
}
