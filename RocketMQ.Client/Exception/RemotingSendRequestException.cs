using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RocketMQ.Client
{
    public class RemotingSendRequestException : RemotingException
    {
        //private static readonly long serialVersionUID = 5391285827332471674L;

        public RemotingSendRequestException(String addr) 
            : this(addr, null)
        {
            
        }

        public RemotingSendRequestException(String addr, Exception cause)
            : base("send request to <" + addr + "> failed", cause)
        {
            
        }
    }
}
