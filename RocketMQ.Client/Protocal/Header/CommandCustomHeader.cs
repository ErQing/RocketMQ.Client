using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RocketMQ.Client
{
    public interface CommandCustomHeader
    {
        ///<exception cref="RemotingCommandException"/>
        void checkFields();
    }
}
