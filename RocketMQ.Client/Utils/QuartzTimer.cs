using Quartz;
using Quartz.Impl;
using Quartz.Logging;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace RocketMQ.Client
{
    public sealed class QuartzTimer
    {
        static readonly NLog.Logger LOGGER = NLog.LogManager.GetCurrentClassLogger();
        static IScheduler scheduler;

        class ConsoleLogProvider : ILogProvider
        {
            public Logger GetLogger(string name)
            {
                return (level, func, exception, parameters) =>
                {
                    if(func != null)
                    {
                        if (level < LogLevel.Warn)
                        { }//LOGGER.Debug(func(), parameters);
                        else if (level == LogLevel.Warn)
                            LOGGER.Warn(func(), parameters);
                        else if (level > LogLevel.Warn)
                            LOGGER.Error(func(), parameters);
                    }
                    return true;
                };
            }

            public IDisposable OpenMappedContext(string key, object value, bool destructure = false)
            {
                throw new NotImplementedException();
            }

            public IDisposable OpenNestedContext(string message)
            {
                throw new NotImplementedException();
            }
        }

        static QuartzTimer()
        {
            InitScheduler();
        }

        private static async void InitScheduler()
        {
            if (scheduler != null)
                return;
            LogProvider.SetCurrentLogProvider(new ConsoleLogProvider());
            var factory = new StdSchedulerFactory();
            scheduler = await factory.GetScheduler();
            await scheduler.Start();
        }

        static long idNow;
        static long NewID()
        {
            return Interlocked.Increment(ref idNow);
        }

        /// <summary>
        /// 所有Job需要通过此接口来创建，避免id重复
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <returns></returns>
        public static IJobDetail CreateJob<T>() where T : IJob
        {
            return JobBuilder.Create<T>().WithIdentity(NewID() + "").Build();
        }

        public static IJobDetail CreateJob<T>(ref long id) where T : IJob
        {
            id = NewID();
            return JobBuilder.Create<T>().WithIdentity(id + "").Build();
        }


        public static void Remove(JobKey key)
        {
            scheduler.DeleteJob(key);
        }

        public static JobKey Schedule(long delay, IJobDetail jobDetail)
        {
            if (jobDetail == null)
                LOGGER.Error("不能添加空的Job");
            var delayTime = DateTimeOffset.Now.AddMilliseconds(delay);
            var trigger = TriggerBuilder.Create().StartAt(delayTime).Build();
            scheduler.ScheduleJob(jobDetail, trigger);
            return jobDetail.Key;
        }

        public static JobKey Schedule(DateTime time, IJobDetail jobDetail)
        {
            if (jobDetail == null)
                LOGGER.Error("不能添加空的Job");
            var delta = time - DateTime.Now;
            var delayTime = DateTimeOffset.Now.Add(delta);
            var trigger = TriggerBuilder.Create().StartAt(delayTime).Build();
            scheduler.ScheduleJob(jobDetail, trigger);
            return jobDetail.Key;
        }

        public static JobKey ScheduleAtFixedRate(long delay, long period, IJobDetail jobDetail)
        {
            if (jobDetail == null)
                LOGGER.Error("不能添加空的Job");
            var delayTime = DateTimeOffset.Now.AddMilliseconds(delay);
            var trigger = TriggerBuilder.Create().StartAt(delayTime)
                .WithSimpleSchedule(x => x.WithInterval(TimeSpan.FromMilliseconds(period)).RepeatForever()).Build();
            scheduler.ScheduleJob(jobDetail, trigger);
            return jobDetail.Key;
        }

        /// <summary>
        /// 每天
        /// </summary>
        public static JobKey ScheduleDaily(int hour, int minute, IJobDetail jobDetail)
        {
            if (jobDetail == null)
                LOGGER.Error("不能添加空的Job");
            var trigger = TriggerBuilder.Create().StartNow().WithSchedule(CronScheduleBuilder.DailyAtHourAndMinute(hour, minute)).Build();
            scheduler.ScheduleJob(jobDetail, trigger);
            return jobDetail.Key;
        }

        /// <summary>
        /// 每周
        /// </summary>
        public static JobKey ScheduleWeekly(DayOfWeek[] days, int hour, int minute, IJobDetail jobDetail)
        {
            var trigger = TriggerBuilder.Create().StartNow().WithSchedule(CronScheduleBuilder.AtHourAndMinuteOnGivenDaysOfWeek(hour, minute, days)).Build();
            scheduler.ScheduleJob(jobDetail, trigger);
            return jobDetail.Key;
        }

        /// <summary>
        /// 每月
        /// </summary>
        public static JobKey ScheduleMonthly(int dayOfMonth, int hour, int minute, IJobDetail jobDetail)
        {
            if (jobDetail == null)
                LOGGER.Error("不能添加空的Job");
            var trigger = TriggerBuilder.Create().StartNow().WithSchedule(CronScheduleBuilder.MonthlyOnDayAndHourAndMinute(dayOfMonth, hour, minute)).Build();
            scheduler.ScheduleJob(jobDetail, trigger);
            return jobDetail.Key;
        }

        public static Task Stop()
        {
            return scheduler.Shutdown();
        }

    }
}
