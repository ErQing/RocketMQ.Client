using System;

namespace RocketMQ.Client
{
    public class DeleteSubscriptionGroupRequestHeader : CommandCustomHeader
    {
        [CFNotNull]
        public String groupName { get; set; }

        public bool removeOffset { get; set; }

        public void checkFields() { }

    }
}
