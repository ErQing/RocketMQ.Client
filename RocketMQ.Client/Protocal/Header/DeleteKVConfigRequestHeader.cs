using System;

namespace RocketMQ.Client
{
    class DeleteKVConfigRequestHeader : CommandCustomHeader
    {
        [CFNotNull]
        public string nameSpace { get; set; }
        [CFNotNull]
        public string key { get; set; }

        public void checkFields() { }
    }
}
