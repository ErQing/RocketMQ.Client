using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RocketMQ.Client
{
    public class RemotingException : Exception
    {
        //private static final long serialVersionUID = -5690687334570505110L;

        public RemotingException(String message)
            :base(message)
        {
            
        }

        public RemotingException(String message, Exception cause)
            :base(message, cause)
        {
            
        }
    }
}
