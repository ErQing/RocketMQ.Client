using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RocketMQ.Client//.Protocal.Header
{
    public class PutKVConfigRequestHeader : CommandCustomHeader
    {
        [CFNotNull]
        public String nameSpace{ get; set; }
        [CFNotNull]
        public String key{ get; set; }
        [CFNotNull]
        public String value{ get; set; }

        //@Override
        public void checkFields() //throws RemotingCommandException
        {
        }       
    }
}
