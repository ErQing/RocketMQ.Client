using System;

namespace RocketMQ.Client
{
    class DeleteKVConfigRequestHeader : CommandCustomHeader
    {
        [CFNotNull]
        public String nameSpace { get; set; }
        [CFNotNull]
        public String key { get; set; }

        public void checkFields() { }
    }
}
