using System;
using System.Collections.Generic;

namespace RocketMQ.Client
{
    public class StatsItem
    {
        private readonly LongAdder value = new LongAdder();

        private readonly LongAdder times = new LongAdder();

        private readonly LinkedList<CallSnapshot> csListMinute = new LinkedList<CallSnapshot>();

        private readonly LinkedList<CallSnapshot> csListHour = new LinkedList<CallSnapshot>();

        private readonly LinkedList<CallSnapshot> csListDay = new LinkedList<CallSnapshot>();

        private readonly String statsName;
        private readonly String statsKey;
        private readonly ScheduledExecutorService scheduledExecutorService;
        //private readonly InternalLogger log;
        private readonly NLog.Logger log;

        public StatsItem(String statsName, String statsKey, ScheduledExecutorService scheduledExecutorService, NLog.Logger log)
        {
            this.statsName = statsName;
            this.statsKey = statsKey;
            this.scheduledExecutorService = scheduledExecutorService;
            this.log = log;
        }

        private static StatsSnapshot computeStatsData(LinkedList<CallSnapshot> csList)
        {
            StatsSnapshot statsSnapshot = new StatsSnapshot();
            lock (csList)
            {
                double tps = 0;
                double avgpt = 0;
                long sum = 0;
                long timesDiff = 0;
                if (!csList.isEmpty())
                {
                    CallSnapshot first = csList.First.Value;
                    CallSnapshot last = csList.First.Value;
                    sum = last.getValue() - first.getValue();
                    tps = (sum * 1000.0d) / (last.getTimestamp() - first.getTimestamp());

                    timesDiff = last.getTimes() - first.getTimes();
                    if (timesDiff > 0)
                    {
                        avgpt = (sum * 1.0d) / timesDiff;
                    }
                }
                statsSnapshot.setSum(sum);
                statsSnapshot.setTps(tps);
                statsSnapshot.setAvgpt(avgpt);
                statsSnapshot.setTimes(timesDiff);
            }
            return statsSnapshot;
        }

        public StatsSnapshot getStatsDataInMinute()
        {
            return computeStatsData(this.csListMinute);
        }

        public StatsSnapshot getStatsDataInHour()
        {
            return computeStatsData(this.csListHour);
        }

        public StatsSnapshot getStatsDataInDay()
        {
            return computeStatsData(this.csListDay);
        }

        public void init()
        {

            this.scheduledExecutorService.ScheduleAtFixedRate(new Runnable()
            {
                //public void run()
                Run = () =>
            {
                try
                {
                    samplingInSeconds();
                }
                catch (Exception ignored)
                {
                }
            }
            }, 0, 10);

            this.scheduledExecutorService.ScheduleAtFixedRate(new Runnable()
            {
                //public void run()
                Run = () =>
                {
                    try
                    {
                        samplingInMinutes();
                    }
                    catch (Exception ignored)
                    {
                    }
                }
            }, 0, 10);

            this.scheduledExecutorService.ScheduleAtFixedRate(new Runnable()
            {
                //public void run()
                Run = () =>
                {
                    try
                    {
                        samplingInHour();
                    }
                    catch (Exception ignored)
                    {
                    }
                }
            }, 0, 1);

            this.scheduledExecutorService.ScheduleAtFixedRate(new Runnable()
            {
                //public void run()
                Run = () =>
                {
                    try
                    {
                        printAtMinutes();
                    }
                    catch (Exception ignored)
                    {
                    }
                }
            }, Math.Abs(UtilAll.computeNextMinutesTimeMillis() - Sys.currentTimeMillis()), 1000 * 60);

            this.scheduledExecutorService.ScheduleAtFixedRate(new Runnable()
            {
                //public void run()
                Run = () =>
                {
                    try
                    {
                        printAtHour();
                    }
                    catch (Exception ignored)
                    {
                    }
                }
            }, Math.Abs(UtilAll.computeNextHourTimeMillis() - Sys.currentTimeMillis()), 1000 * 60 * 60);

            this.scheduledExecutorService.ScheduleAtFixedRate(new Runnable()
            {
                //public void run()
                Run = () =>
                {
                    try
                    {
                        printAtDay();
                    }
                    catch (Exception ignored)
                    {
                    }
                }
            }, Math.Abs(UtilAll.computeNextMorningTimeMillis() - Sys.currentTimeMillis()) - 2000, 1000 * 60 * 60 * 24);
        }

        public void samplingInSeconds()
        {
            lock (this.csListMinute)
            {
                if (this.csListMinute.Count == 0)
                {
                    this.csListMinute.AddLast(new CallSnapshot(Sys.currentTimeMillis() - 10 * 1000, 0, 0));
                }
                this.csListMinute.AddLast(new CallSnapshot(Sys.currentTimeMillis(), this.times.sum(), this.value.sum()));
                if (this.csListMinute.Count > 7)
                {
                    this.csListMinute.RemoveFirst();
                }
            }
        }

        public void samplingInMinutes()
        {
            lock (this.csListHour)
            {
                if (this.csListHour.Count == 0)
                {
                    this.csListHour.AddLast(new CallSnapshot(Sys.currentTimeMillis() - 10 * 60 * 1000, 0, 0));
                }
                this.csListHour.AddLast(new CallSnapshot(Sys.currentTimeMillis(), this.times.sum(), this.value.sum()));
                if (this.csListHour.Count > 7)
                {
                    this.csListHour.RemoveFirst();
                }
            }
        }

        public void samplingInHour()
        {
            lock (this.csListDay)
            {
                if (this.csListDay.Count == 0)
                {
                    this.csListDay.AddLast(new CallSnapshot(Sys.currentTimeMillis() - 1 * 60 * 60 * 1000, 0, 0));
                }
                this.csListDay.AddLast(new CallSnapshot(Sys.currentTimeMillis(), this.times.sum(), this.value.sum()));
                if (this.csListDay.Count > 25)
                {
                    this.csListDay.RemoveFirst();
                }
            }
        }

        public void printAtMinutes()
        {
            StatsSnapshot ss = computeStatsData(this.csListMinute);
            log.Info(String.Format("[%s] [%s] Stats In One Minute, ", this.statsName, this.statsKey) + statPrintDetail(ss));
        }

        public void printAtHour()
        {
            StatsSnapshot ss = computeStatsData(this.csListHour);
            log.Info(String.Format("[%s] [%s] Stats In One Hour, ", this.statsName, this.statsKey) + statPrintDetail(ss));

        }

        public void printAtDay()
        {
            StatsSnapshot ss = computeStatsData(this.csListDay);
            log.Info(String.Format("[%s] [%s] Stats In One Day, ", this.statsName, this.statsKey) + statPrintDetail(ss));
        }

        protected virtual String statPrintDetail(StatsSnapshot ss)
        {
            return String.Format("SUM: %d TPS: %.2f AVGPT: %.2f",
                    ss.getSum(),
                    ss.getTps(),
                    ss.getAvgpt());
        }

        public LongAdder getValue()
        {
            return value;
        }

        public String getStatsKey()
        {
            return statsKey;
        }

        public String getStatsName()
        {
            return statsName;
        }

        public LongAdder getTimes()
        {
            return times;
        }
    }

    class CallSnapshot
    {
        private readonly long timestamp;
        private readonly long times;

        private readonly long value;

        public CallSnapshot(long timestamp, long times, long value)
        {
            //super();
            this.timestamp = timestamp;
            this.times = times;
            this.value = value;
        }

        public long getTimestamp()
        {
            return timestamp;
        }

        public long getTimes()
        {
            return times;
        }

        public long getValue()
        {
            return value;
        }
    }


}
