using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RocketMQ.Client//.Protocal.Header
{
    public class GetKVConfigRequestHeader : CommandCustomHeader
    {
        [CFNotNull]
        public string nameSpace { get; set; }
        [CFNotNull]
        public string key { get; set; }

        //@Override
        public void checkFields() //throws RemotingCommandException
        {
        }
    }
}
