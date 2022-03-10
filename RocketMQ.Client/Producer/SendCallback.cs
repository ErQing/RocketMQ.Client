using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RocketMQ.Client
{
    public class SendCallback
    {
        public Action<SendResult> OnSuccess { get; set; }

        public Action<Exception> OnException { get; set; }
    }
}
