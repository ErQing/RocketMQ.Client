using System;

namespace RocketMQ.Client
{
    public class CloneGroupOffsetRequestHeader : CommandCustomHeader
    {
        [CFNotNull]
        public string srcGroup { get; set; }
        [CFNotNull]
        public string destGroup { get; set; }
        public string topic { get; set; }
        public bool offline { get; set; }

        public void checkFields() { }
    }
}
