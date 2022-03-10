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
        public String extendDataJson { get; set; }
        public String bitMap { get; set; }
        public bool eval { get; set; }
        public String msg { get; set; }
        public override String ToString()
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
