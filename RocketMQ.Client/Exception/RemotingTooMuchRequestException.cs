using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RocketMQ.Client
{
    public class RemotingTooMuchRequestException : RemotingException
    {
        //private static final long serialVersionUID = 4326919581254519654L;
        public RemotingTooMuchRequestException(String message)
            : base(message)
        {
            
        }
    }
}
