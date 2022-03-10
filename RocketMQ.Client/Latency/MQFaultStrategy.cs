using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RocketMQ.Client
{
    public class MQFaultStrategy
    {
        //private final static InternalLogger log = ClientLogger.getLog();
        static NLog.Logger log = NLog.LogManager.GetCurrentClassLogger();
        private readonly LatencyFaultTolerance<String> latencyFaultTolerance = new LatencyFaultToleranceImpl();

        private bool sendLatencyFaultEnable = false;

        private long[] latencyMax = { 50L, 100L, 550L, 1000L, 2000L, 3000L, 15000L };
        private long[] notAvailableDuration = { 0L, 0L, 30000L, 60000L, 120000L, 180000L, 600000L };

        public long[] getNotAvailableDuration()
        {
            return notAvailableDuration;
        }

        public void setNotAvailableDuration(long[] notAvailableDuration)
        {
            this.notAvailableDuration = notAvailableDuration;
        }

        public long[] getLatencyMax()
        {
            return latencyMax;
        }

        public void setLatencyMax(long[] latencyMax)
        {
            this.latencyMax = latencyMax;
        }

        public bool isSendLatencyFaultEnable()
        {
            return sendLatencyFaultEnable;
        }

        public void setSendLatencyFaultEnable(bool sendLatencyFaultEnable)
        {
            this.sendLatencyFaultEnable = sendLatencyFaultEnable;
        }

        public MessageQueue selectOneMessageQueue(TopicPublishInfo tpInfo, String lastBrokerName)
        {
            if (this.sendLatencyFaultEnable)
            {
                try
                {
                    int index = tpInfo.getSendWhichQueue().incrementAndGet();
                    for (int i = 0; i < tpInfo.getMessageQueueList().Count; i++)
                    {
                        int pos = Math.Abs(index++) % tpInfo.getMessageQueueList().Count;
                        if (pos < 0)
                            pos = 0;
                        MessageQueue mq = tpInfo.getMessageQueueList()[pos];
                        if (latencyFaultTolerance.isAvailable(mq.getBrokerName()))
                            return mq;
                    }

                    String notBestBroker = latencyFaultTolerance.pickOneAtLeast();
                    int writeQueueNums = tpInfo.getQueueIdByBroker(notBestBroker);
                    if (writeQueueNums > 0)
                    {
                        MessageQueue mq = tpInfo.selectOneMessageQueue();
                        if (notBestBroker != null)
                        {
                            mq.setBrokerName(notBestBroker);
                            mq.setQueueId(tpInfo.getSendWhichQueue().incrementAndGet() % writeQueueNums);
                        }
                        return mq;
                    }
                    else
                    {
                        latencyFaultTolerance.remove(notBestBroker);
                    }
                }
                catch (Exception e)
                {
                    log.Error("Error occurred when selecting message queue", e.ToString());
                }

                return tpInfo.selectOneMessageQueue();
            }

            return tpInfo.selectOneMessageQueue(lastBrokerName);
        }

        public void updateFaultItem(String brokerName, long currentLatency, bool isolation)
        {
            if (this.sendLatencyFaultEnable)
            {
                long duration = computeNotAvailableDuration(isolation ? 30000 : currentLatency);
                this.latencyFaultTolerance.updateFaultItem(brokerName, currentLatency, duration);
            }
        }

        private long computeNotAvailableDuration(long currentLatency)
        {
            for (int i = latencyMax.Length - 1; i >= 0; i--)
            {
                if (currentLatency >= latencyMax[i])
                    return this.notAvailableDuration[i];
            }

            return 0;
        }
    }
}
