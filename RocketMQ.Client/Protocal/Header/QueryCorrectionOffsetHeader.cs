using System;

namespace RocketMQ.Client//.Protocal.Header
{
    public class QueryCorrectionOffsetHeader : CommandCustomHeader
    {
        public string filterGroups { get; set; }
        [CFNotNull]
        public string compareGroup { get; set; }
        [CFNotNull]
        public string topic { get; set; }

        public void checkFields() { }
    }
}
