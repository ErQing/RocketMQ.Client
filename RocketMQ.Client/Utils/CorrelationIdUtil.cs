using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RocketMQ.Client
{
    public class CorrelationIdUtil
    {
        public static String createCorrelationId()
        {
            //return UUID.randomUUID().toString();
            return System.Guid.NewGuid().ToString();
        }
    }
}
