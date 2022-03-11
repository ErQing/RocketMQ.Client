using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RocketMQ.Client//.Protocal.Header
{
    public class ResumeCheckHalfMessageRequestHeader : CommandCustomHeader
    {
        [CFNullable]
        public string msgId { get; set; }

        //@Override
        public void checkFields() //throws RemotingCommandException
        {

        }
        //@Override
        public override string ToString()
        {
            return "ResumeCheckHalfMessageRequestHeader [msgId=" + msgId + "]";
        }
    }
}
