using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RocketMQ.Client//.Protocal.Body
{
    public class GroupList : RemotingSerializable
    {
        public HashSet<String> groupList { get; set; }
    }
}
