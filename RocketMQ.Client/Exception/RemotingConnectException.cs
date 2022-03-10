using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RocketMQ.Client
{
    public class RemotingConnectException : RemotingException
    {
        //private static readonly long serialVersionUID = -5565366231695911316L;

        public RemotingConnectException(String addr) : this(addr, null)
        {
            
        }

        public RemotingConnectException(String addr, Exception cause)
            : base("connect to " + addr + " failed", cause)
        {
            
        }
    }
}
