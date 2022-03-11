using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RocketMQ.Client
{
    public class ConsumeQueueData
    {
        public long physicOffset { get; set; }
        public int physicSize { get; set; }
        public long tagsCode { get; set; }
        public string extendDataJson { get; set; }
        public string bitMap { get; set; }
        public bool eval { get; set; }
        public string msg { get; set; }
        public override string ToString()
        {
            return "ConsumeQueueData{" +
                "physicOffset=" + physicOffset +
                ", physicSize=" + physicSize +
                ", tagsCode=" + tagsCode +
                ", extendDataJson='" + extendDataJson + '\'' +
                ", bitMap='" + bitMap + '\'' +
                ", eval=" + eval +
                ", msg='" + msg + '\'' +
                '}';
        }
    }
}
