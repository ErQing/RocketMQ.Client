using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RocketMQ.Client
{
    public class RuntimeException : Exception
    {
        public RuntimeException(String message)
            : base(message, null)
        {

        }

        public RuntimeException(String message, Exception cause)
            : base(message, cause)
        {

        }

    }
}
