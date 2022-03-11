using System;

namespace RocketMQ.Client
{
    public class GetMaxOffsetRequestHeader : CommandCustomHeader
    {
        [CFNotNull]
        public string topic { get; set; }
        [CFNotNull]
        public int queueId { get; set; }

        //@Override
        public void checkFields()
        {
        }
    }
}
