using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RocketMQ.Client
{
    public class ConsumerStatsManager
    {
        ///private static readonly InternalLogger log = ClientLogger.getLog();
        static NLog.Logger log = NLog.LogManager.GetCurrentClassLogger();

        private static readonly string TOPIC_AND_GROUP_CONSUME_OK_TPS = "CONSUME_OK_TPS";
        private static readonly string TOPIC_AND_GROUP_CONSUME_FAILED_TPS = "CONSUME_FAILED_TPS";
        private static readonly string TOPIC_AND_GROUP_CONSUME_RT = "CONSUME_RT";
        private static readonly string TOPIC_AND_GROUP_PULL_TPS = "PULL_TPS";
        private static readonly string TOPIC_AND_GROUP_PULL_RT = "PULL_RT";

        private readonly StatsItemSet topicAndGroupConsumeOKTPS;
        private readonly StatsItemSet topicAndGroupConsumeRT;
        private readonly StatsItemSet topicAndGroupConsumeFailedTPS;
        private readonly StatsItemSet topicAndGroupPullTPS;
        private readonly StatsItemSet topicAndGroupPullRT;

        public ConsumerStatsManager(ScheduledExecutorService scheduledExecutorService)
        {
            this.topicAndGroupConsumeOKTPS =
                new StatsItemSet(TOPIC_AND_GROUP_CONSUME_OK_TPS, scheduledExecutorService, log);

            this.topicAndGroupConsumeRT =
                new StatsItemSet(TOPIC_AND_GROUP_CONSUME_RT, scheduledExecutorService, log);

            this.topicAndGroupConsumeFailedTPS =
                new StatsItemSet(TOPIC_AND_GROUP_CONSUME_FAILED_TPS, scheduledExecutorService, log);

            this.topicAndGroupPullTPS = new StatsItemSet(TOPIC_AND_GROUP_PULL_TPS, scheduledExecutorService, log);

            this.topicAndGroupPullRT = new StatsItemSet(TOPIC_AND_GROUP_PULL_RT, scheduledExecutorService, log);
        }

        public void start()
        {
        }

        public void shutdown()
        {
        }

        public void incPullRT(String group, string topic, long rt)
        {
            this.topicAndGroupPullRT.addRTValue(topic + "@" + group, (int)rt, 1);
        }

        public void incPullTPS(String group, string topic, long msgs)
        {
            this.topicAndGroupPullTPS.addValue(topic + "@" + group, (int)msgs, 1);
        }

        public void incConsumeRT(String group, string topic, long rt)
        {
            this.topicAndGroupConsumeRT.addRTValue(topic + "@" + group, (int)rt, 1);
        }

        public void incConsumeOKTPS(String group, string topic, long msgs)
        {
            this.topicAndGroupConsumeOKTPS.addValue(topic + "@" + group, (int)msgs, 1);
        }

        public void incConsumeFailedTPS(String group, string topic, long msgs)
        {
            this.topicAndGroupConsumeFailedTPS.addValue(topic + "@" + group, (int)msgs, 1);
        }

        public ConsumeStatus consumeStatus(String group, string topic)
        {
            ConsumeStatus cs = new ConsumeStatus();
            {
                StatsSnapshot ss = this.getPullRT(group, topic);
                if (ss != null)
                {
                    cs.pullRT = (ss.getAvgpt());
                }
            }

            {
                StatsSnapshot ss = this.getPullTPS(group, topic);
                if (ss != null)
                {
                    cs.pullTPS = (ss.getTps());
                }
            }

            {
                StatsSnapshot ss = this.getConsumeRT(group, topic);
                if (ss != null)
                {
                    cs.consumeRT = (ss.getAvgpt());
                }
            }

            {
                StatsSnapshot ss = this.getConsumeOKTPS(group, topic);
                if (ss != null)
                {
                    cs.consumeOKTPS = (ss.getTps());
                }
            }

            {
                StatsSnapshot ss = this.getConsumeFailedTPS(group, topic);
                if (ss != null)
                {
                    cs.consumeFailedTPS = (ss.getTps());
                }
            }

            {
                StatsSnapshot ss = this.topicAndGroupConsumeFailedTPS.getStatsDataInHour(topic + "@" + group);
                if (ss != null)
                {
                    cs.consumeFailedMsgs = (ss.getSum());
                }
            }

            return cs;
        }

        private StatsSnapshot getPullRT(String group, string topic)
        {
            return this.topicAndGroupPullRT.getStatsDataInMinute(topic + "@" + group);
        }

        private StatsSnapshot getPullTPS(String group, string topic)
        {
            return this.topicAndGroupPullTPS.getStatsDataInMinute(topic + "@" + group);
        }

        private StatsSnapshot getConsumeRT(String group, string topic)
        {
            StatsSnapshot statsData = this.topicAndGroupConsumeRT.getStatsDataInMinute(topic + "@" + group);
            if (0 == statsData.getSum())
            {
                statsData = this.topicAndGroupConsumeRT.getStatsDataInHour(topic + "@" + group);
            }

            return statsData;
        }

        private StatsSnapshot getConsumeOKTPS(String group, string topic)
        {
            return this.topicAndGroupConsumeOKTPS.getStatsDataInMinute(topic + "@" + group);
        }

        private StatsSnapshot getConsumeFailedTPS(String group, string topic)
        {
            return this.topicAndGroupConsumeFailedTPS.getStatsDataInMinute(topic + "@" + group);
        }
    }
}
