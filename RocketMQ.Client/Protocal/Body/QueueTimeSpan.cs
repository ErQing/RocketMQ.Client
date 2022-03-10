using System;

namespace RocketMQ.Client
{
    public class QueueTimeSpan
    {
        public MessageQueue messageQueue { get; set; }
        public long minTimeStamp { get; set; }
        public long maxTimeStamp { get; set; }
        public long consumeTimeStamp { get; set; }
        public long delayTime { get; set; }               

        public String getMinTimeStampStr  => UtilAll.formatDate(new DateTime(minTimeStamp), UtilAll.YYYY_MM_DD_HH_MM_SS_SSS);

        public String getMaxTimeStampStr => UtilAll.formatDate(new DateTime(maxTimeStamp), UtilAll.YYYY_MM_DD_HH_MM_SS_SSS);

        public String getConsumeTimeStampStr => UtilAll.formatDate(new DateTime(consumeTimeStamp), UtilAll.YYYY_MM_DD_HH_MM_SS_SSS);
    }
}
