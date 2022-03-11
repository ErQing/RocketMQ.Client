using System;

namespace RocketMQ.Client
{
    public class DeleteSubscriptionGroupRequestHeader : CommandCustomHeader
    {
        [CFNotNull]
        public string groupName { get; set; }

        public bool removeOffset { get; set; }

        public void checkFields() { }

    }
}
