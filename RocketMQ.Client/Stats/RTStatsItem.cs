using System;

namespace RocketMQ.Client
{
    public class RTStatsItem : StatsItem
    {
        public RTStatsItem(String statsName, String statsKey, ScheduledExecutorService scheduledExecutorService, NLog.Logger log)
            :base(statsName, statsKey, scheduledExecutorService, log)
        {
            
        }

        /**
         *   For Response Time stat Item, the print detail should be a little different, TPS and SUM makes no sense.
         *   And we give a name "AVGRT" rather than AVGPT for value getAvgpt()
          */
        //@Override
        protected override String statPrintDetail(StatsSnapshot ss)
        {
            return String.Format("TIMES: %d AVGRT: %.2f", ss.getTimes(), ss.getAvgpt());
        }

    }
}
