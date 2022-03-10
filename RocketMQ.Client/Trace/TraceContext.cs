using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RocketMQ.Client
{
    public class TraceContext : IComparable<TraceContext>
    {
        private TraceType traceType;
        private long timeStamp = Sys.currentTimeMillis();
        private String regionId = "";
        private String regionName = "";
        private String groupName = "";
        private int costTime = 0;
        private bool isSuccess = true;
        private String requestId = MessageClientIDSetter.createUniqID();
        private int contextCode = 0;
        private List<TraceBean> traceBeans;

        public int getContextCode()
        {
            return contextCode;
        }

        public void setContextCode(int contextCode)
        {
            this.contextCode = contextCode;
        }

        public List<TraceBean> getTraceBeans()
        {
            return traceBeans;
        }

        public void setTraceBeans(List<TraceBean> traceBeans)
        {
            this.traceBeans = traceBeans;
        }

        public String getRegionId()
        {
            return regionId;
        }

        public void setRegionId(String regionId)
        {
            this.regionId = regionId;
        }

        public TraceType getTraceType()
        {
            return traceType;
        }

        public void setTraceType(TraceType traceType)
        {
            this.traceType = traceType;
        }

        public long getTimeStamp()
        {
            return timeStamp;
        }

        public void setTimeStamp(long timeStamp)
        {
            this.timeStamp = timeStamp;
        }

        public String getGroupName()
        {
            return groupName;
        }

        public void setGroupName(String groupName)
        {
            this.groupName = groupName;
        }

        public int getCostTime()
        {
            return costTime;
        }

        public void setCostTime(int costTime)
        {
            this.costTime = costTime;
        }

        public bool GetIsSuccess()
        {
            return isSuccess;
        }

        public void setSuccess(bool success)
        {
            isSuccess = success;
        }

        public String getRequestId()
        {
            return requestId;
        }

        public void setRequestId(String requestId)
        {
            this.requestId = requestId;
        }

        public String getRegionName()
        {
            return regionName;
        }

        public void setRegionName(String regionName)
        {
            this.regionName = regionName;
        }

        //@Override
        public int CompareTo(TraceContext o)
        {
            return (int)(this.timeStamp - o.getTimeStamp());
        }

        //@Override
        public override String ToString()
        {
            StringBuilder sb = new StringBuilder(1024);
            sb.Append(traceType).Append("_").Append(groupName)
                .Append("_").Append(regionId).Append("_").Append(isSuccess).Append("_");
            if (traceBeans != null && traceBeans.Count > 0)
            {
                foreach (TraceBean bean in traceBeans)
                {
                    sb.Append(bean.getMsgId() + "_" + bean.getTopic() + "_");
                }
            }
            return "TraceContext{" + sb.ToString() + '}';
        }
    }
}
