using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RocketMQ.Client
{
    public class StatsItemSet
    {
        //private readonly ConcurrentDictionary<String/* key */, StatsItem> statsItemTable =
        //new ConcurrentDictionary<String, StatsItem>(128);

        private readonly ConcurrentDictionary<String/* key */, StatsItem> statsItemTable =
        new ConcurrentDictionary<String, StatsItem>();

        private readonly String statsName;
    private readonly ScheduledExecutorService scheduledExecutorService;
    //private readonly InternalLogger log;
        private readonly NLog.Logger log;

        public StatsItemSet(String statsName, ScheduledExecutorService scheduledExecutorService, NLog.Logger log)
        {
            this.statsName = statsName;
            this.scheduledExecutorService = scheduledExecutorService;
            this.log = log;
            this.init();
        }

        public void init()
        {

        //    this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
        //    @Override
        //    public void run()
        //    {
        //        try
        //        {
        //            samplingInSeconds();
        //        }
        //        catch (Throwable ignored)
        //        {
        //        }
        //    }
        //}, 0, 10, TimeUnit.SECONDS);

        //this.scheduledExecutorService.scheduleAtFixedRate(new Runnable()
        //{
        //    @Override
        //    public void run()
        //    {
        //        try
        //        {
        //            samplingInMinutes();
        //        }
        //        catch (Throwable ignored)
        //        {
        //        }
        //    }
        //}, 0, 10, TimeUnit.MINUTES);

        //this.scheduledExecutorService.scheduleAtFixedRate(new Runnable()
        //{
        //    @Override
        //    public void run()
        //    {
        //        try
        //        {
        //            samplingInHour();
        //        }
        //        catch (Throwable ignored)
        //        {
        //        }
        //    }
        //}, 0, 1, TimeUnit.HOURS);

        //this.scheduledExecutorService.scheduleAtFixedRate(new Runnable()
        //{
        //    @Override
        //    public void run()
        //    {
        //        try
        //        {
        //            printAtMinutes();
        //        }
        //        catch (Throwable ignored)
        //        {
        //        }
        //    }
        //}, Math.abs(UtilAll.computeNextMinutesTimeMillis() - System.currentTimeMillis()), 1000 * 60, TimeUnit.MILLISECONDS);

        //this.scheduledExecutorService.scheduleAtFixedRate(new Runnable()
        //{
        //    @Override
        //    public void run()
        //    {
        //        try
        //        {
        //            printAtHour();
        //        }
        //        catch (Throwable ignored)
        //        {
        //        }
        //    }
        //}, Math.abs(UtilAll.computeNextHourTimeMillis() - System.currentTimeMillis()), 1000 * 60 * 60, TimeUnit.MILLISECONDS);

        //this.scheduledExecutorService.scheduleAtFixedRate(new Runnable()
        //{
        //    @Override
        //    public void run()
        //    {
        //        try
        //        {
        //            printAtDay();
        //        }
        //        catch (Throwable ignored)
        //        {
        //        }
        //    }
        //}, Math.abs(UtilAll.computeNextMorningTimeMillis() - System.currentTimeMillis()), 1000 * 60 * 60 * 24, TimeUnit.MILLISECONDS);
    }

    private void samplingInSeconds()
    {
        //Iterator<Entry<String, StatsItem>> it = this.statsItemTable.entrySet().iterator();
        //while (it.hasNext())
        foreach(var enty in this.statsItemTable)
        {
                //Entry<String, StatsItem> next = it.next();
                enty.Value.samplingInSeconds();
        }
    }

    private void samplingInMinutes()
    {
            //Iterator<Entry<String, StatsItem>> it = this.statsItemTable.entrySet().iterator();
            //while (it.hasNext())
            foreach (var enty in this.statsItemTable)
            {
                //Entry<String, StatsItem> next = it.next();
                enty.Value.samplingInMinutes();
        }
    }

    private void samplingInHour()
    {
            //Iterator<Entry<String, StatsItem>> it = this.statsItemTable.entrySet().iterator();
            //while (it.hasNext())
            foreach (var enty in this.statsItemTable)
            {
                //Entry<String, StatsItem> next = it.next();
                enty.Value.samplingInHour();
        }
    }

    private void printAtMinutes()
    {
            //Iterator<Entry<String, StatsItem>> it = this.statsItemTable.entrySet().iterator();
            //while (it.hasNext())
            foreach (var enty in this.statsItemTable)
            {
                //Entry<String, StatsItem> next = it.next();
                enty.Value.printAtMinutes();
        }
    }

    private void printAtHour()
    {
            //Iterator<Entry<String, StatsItem>> it = this.statsItemTable.entrySet().iterator();
            //while (it.hasNext())
            foreach (var enty in this.statsItemTable)
            {
                //Entry<String, StatsItem> next = it.next();
                enty.Value.printAtHour();
        }
    }

    private void printAtDay()
    {
            //Iterator<Entry<String, StatsItem>> it = this.statsItemTable.entrySet().iterator();
            //while (it.hasNext())
            foreach (var enty in this.statsItemTable)
            {
                //Entry<String, StatsItem> next = it.next();
                enty.Value.printAtDay();
        }
    }

    public void addValue(String statsKey, int incValue, int incTimes)
    {
        StatsItem statsItem = this.getAndCreateStatsItem(statsKey);
        statsItem.getValue().Add(incValue);
        statsItem.getTimes().Add(incTimes);
    }

    public void addRTValue(String statsKey, int incValue, int incTimes)
    {
        StatsItem statsItem = this.getAndCreateRTStatsItem(statsKey);
        statsItem.getValue().Add(incValue);
        statsItem.getTimes().Add(incTimes);
    }

    public void delValue(String statsKey)
    {
        StatsItem statsItem = this.statsItemTable.get(statsKey);
        if (null != statsItem)
        {
            this.statsItemTable.remove(statsKey);
        }
    }

    public void delValueByPrefixKey(String statsKey, String separator)
    {
            //Iterator<Entry<String, StatsItem>> it = this.statsItemTable.entrySet().iterator();
            //while (it.hasNext())
            foreach (var enty in this.statsItemTable)
            {
            //Entry<String, StatsItem> next = it.next();
            if (enty.Key.StartsWith(statsKey + separator))
            {
                    //it.remove();
                    statsItemTable.TryRemove(enty.Key, out _);
            }
        }
    }

    public void delValueByInfixKey(String statsKey, String separator)
    {
            //Iterator<Entry<String, StatsItem>> it = this.statsItemTable.entrySet().iterator();
            //while (it.hasNext())
            foreach (var enty in this.statsItemTable)
            {
            //Entry<String, StatsItem> next = it.next();
            if (enty.Key.Contains(separator + statsKey + separator))
            {
                //it.remove();
                    statsItemTable.TryRemove(enty.Key, out _);
                }
        }
    }

    public void delValueBySuffixKey(String statsKey, String separator)
    {
            //Iterator<Entry<String, StatsItem>> it = this.statsItemTable.entrySet().iterator();
            //while (it.hasNext())
            foreach (var enty in this.statsItemTable)
            {
            //Entry<String, StatsItem> next = it.next();
            if (enty.Key.EndsWith(separator + statsKey))
            {
                    //it.remove();
                    statsItemTable.TryRemove(enty.Key, out _);
                }
        }
    }

    public StatsItem getAndCreateStatsItem(String statsKey)
    {
        return getAndCreateItem(statsKey, false);
    }

    public StatsItem getAndCreateRTStatsItem(String statsKey)
    {
        return getAndCreateItem(statsKey, true);
    }

    public StatsItem getAndCreateItem(String statsKey, bool rtItem)
    {
        StatsItem statsItem = this.statsItemTable.get(statsKey);
        if (null == statsItem)
        {
            if (rtItem)
            {
                statsItem = new RTStatsItem(this.statsName, statsKey, this.scheduledExecutorService, this.log);
            }
            else
            {
                statsItem = new StatsItem(this.statsName, statsKey, this.scheduledExecutorService, this.log);
            }
            StatsItem prev = this.statsItemTable.putIfAbsent(statsKey, statsItem);

            if (null != prev)
            {
                statsItem = prev;
                // statsItem.init();
            }
        }

        return statsItem;
    }

    public StatsSnapshot getStatsDataInMinute(String statsKey)
    {
        StatsItem statsItem = this.statsItemTable.get(statsKey);
        if (null != statsItem)
        {
            return statsItem.getStatsDataInMinute();
        }
        return new StatsSnapshot();
    }

    public StatsSnapshot getStatsDataInHour(String statsKey)
    {
        StatsItem statsItem = this.statsItemTable.get(statsKey);
        if (null != statsItem)
        {
            return statsItem.getStatsDataInHour();
        }
        return new StatsSnapshot();
    }

    public StatsSnapshot getStatsDataInDay(String statsKey)
    {
        StatsItem statsItem = this.statsItemTable.get(statsKey);
        if (null != statsItem)
        {
            return statsItem.getStatsDataInDay();
        }
        return new StatsSnapshot();
    }

    public StatsItem getStatsItem(String statsKey)
    {
        return this.statsItemTable.get(statsKey);
    }
}
}
