using System;
using System.Collections.Generic;

namespace RocketMQ.Client//.Protocal.Body
{
    public class KVTable : RemotingSerializable
    {
        public Dictionary<String, String> table { get; set; }
    }
}
