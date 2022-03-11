using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RocketMQ.Client//.Protocal.Header
{
    public class ResetOffsetRequestHeader : CommandCustomHeader
    {
        [CFNotNull]
        public string topic { get; set; }
        [CFNotNull]
        public string group { get; set; }
        [CFNotNull]
        public long timestamp { get; set; }
        [CFNotNull]
        public bool isForce { get; set; }
        //@Override
        public void checkFields() //throws RemotingCommandException
        {

        }
    }
}
