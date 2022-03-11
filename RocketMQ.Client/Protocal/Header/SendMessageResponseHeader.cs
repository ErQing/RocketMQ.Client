using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RocketMQ.Client
{
    public class SendMessageResponseHeader : CommandCustomHeader
    {
        [CFNotNull]
        public string msgId{ get; set; }
        [CFNotNull]
        public int queueId{ get; set; }
        [CFNotNull]
        public long queueOffset{ get; set; }
        public string transactionId{ get; set; }

        public void checkFields()
        {
        }
    }
}
