using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RocketMQ.Client
{
    public class RemotingCommandException : RemotingException
    {
        //private static readonly long serialVersionUID = -6061365915274953096L;

        public RemotingCommandException(String message) 
            :base(message, null)
        {
            
        }

        public RemotingCommandException(String message, Exception cause)
            : base(message, cause)
        {
            
        }
    }
}
