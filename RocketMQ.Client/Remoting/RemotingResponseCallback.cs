using System;

namespace RocketMQ.Client
{
    public class RemotingResponseCallback
    {
        public Action<RemotingCommand> Callback { get; set; }
    }
}
