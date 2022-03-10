using System;
using System.Collections.Generic;

namespace RocketMQ.Client
{
    public class LockBatchRequestBody : RemotingSerializable
    {
        public String consumerGroup { get; set; }
        public String clientId { get; set; }
        public HashSet<MessageQueue> mqSet { get; set; } = new HashSet<MessageQueue>();
    }
}
