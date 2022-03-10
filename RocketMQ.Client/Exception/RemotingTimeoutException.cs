using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RocketMQ.Client
{
    public class RemotingTimeoutException : RemotingException
    {
        //private static final long serialVersionUID = 4106899185095245979L;

        public RemotingTimeoutException(String message)
            : base(message)
        {

        }

        public RemotingTimeoutException(String addr, long timeoutMillis)
            : this(addr, timeoutMillis, null)
        {
        }

        public RemotingTimeoutException(String addr, long timeoutMillis, Exception cause)
            : base("wait response on the channel <" + addr + "> timeout, " + timeoutMillis + "(ms)", cause)
        {
            
        }
    }
}
