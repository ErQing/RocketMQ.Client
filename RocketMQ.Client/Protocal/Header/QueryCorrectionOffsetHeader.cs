using System;

namespace RocketMQ.Client//.Protocal.Header
{
    public class QueryCorrectionOffsetHeader : CommandCustomHeader
    {
        public String filterGroups { get; set; }
        [CFNotNull]
        public String compareGroup { get; set; }
        [CFNotNull]
        public String topic { get; set; }

        public void checkFields() { }
    }
}
