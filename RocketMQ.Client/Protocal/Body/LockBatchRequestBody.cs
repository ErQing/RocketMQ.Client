using System;
using System.Collections.Generic;

namespace RocketMQ.Client
{
    public class LockBatchRequestBody : RemotingSerializable
    {
        public string consumerGroup { get; set; }
        public string clientId { get; set; }
        public HashSet<MessageQueue> mqSet { get; set; } = new HashSet<MessageQueue>();
    }
}
