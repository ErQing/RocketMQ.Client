using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RocketMQ.Client
{
    public enum MessageType
    {
        Normal_Msg,
        Trans_Msg_Half,
        Trans_msg_Commit,
        Delay_Msg
    }
}
