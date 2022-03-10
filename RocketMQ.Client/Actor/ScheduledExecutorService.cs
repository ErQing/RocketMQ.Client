using Quartz;
using System;
using System.Collections.Concurrent;
using System.Threading.Tasks;

namespace RocketMQ.Client
{
    public class ScheduledExecutorService : ExecutorService
    {

        static readonly NLog.Logger LOGGER = NLog.LogManager.GetCurrentClassLogger();
        private const string ownerKey = "owner";
        //private readonly ConcurrentDictionary<JobKey, IRunnable> scheduleDic = new ConcurrentDictionary<JobKey, IRunnable>();
        private readonly ConcurrentDictionary<JobKey, Action> scheduleDic = new ConcurrentDictionary<JobKey, Action>();

        /// <param name="parallelism">并行度</param>
        /// <param name="capacity">队列容量，-1为无限</param>
        public ScheduledExecutorService(int parallelism = 1, int capacity = -1) 
            : base(parallelism, capacity)
        {
            
        }

        private void DoShedule(JobKey key)
        {
            scheduleDic.TryRemove(key, out Action schedule);
            if (schedule != null)
                Submit(schedule);
            else
                LOGGER.Error($"执行任务时找不到schedule:{key}");
        }

        //[DisallowConcurrentExecution]
        class ScheduleJob : IJob
        {
            public Task Execute(IJobExecutionContext context)
            {
                var owner = (ScheduledExecutorService)context.MergedJobDataMap["owner"];
                if (owner != null)
                    owner.DoShedule(context.JobDetail.Key);
                else
                    LOGGER.Error("执行schedule时找不到owner");
                return Task.CompletedTask;
            }
        }

        public static void Remove(JobKey key)
        {
            QuartzTimer.Remove(key);
        }

        private IJobDetail CreateJob()
        {
            var job = QuartzTimer.CreateJob<ScheduleJob>();
            job.JobDataMap.Add(ownerKey, this);
            return job;
        }
        public JobKey Schedule(IRunnable runnable, long delay)
        {
            var job = CreateJob();
            bool flag = scheduleDic.TryAdd(job.Key, runnable.run);
            if (!flag)
            {
                //理论上不可能走到此分支，返回的JobKey永远重复
                LOGGER.Error($"添加失败[重复key]{job.Key.Name}");
                return job.Key;
            }
            return QuartzTimer.Schedule(delay, job);
        }

        public JobKey Schedule(Runnable runnable, long delay)
        {
            var job = CreateJob();
            bool flag = scheduleDic.TryAdd(job.Key, runnable.Run);
            if (!flag)
            {
                //理论上不可能走到此分支，返回的JobKey永远重复
                LOGGER.Error($"添加失败[重复key]{job.Key.Name}");
                return job.Key;
            }
            return QuartzTimer.Schedule(delay, job);
        }

        public JobKey Schedule(Action schedule, long delay)
        {
            var job = CreateJob();
            bool flag = scheduleDic.TryAdd(job.Key, schedule);
            if (!flag)
            {
                //理论上不可能走到此分支，返回的JobKey永远重复
                LOGGER.Error($"添加失败[重复key]{job.Key.Name}");
                return job.Key;
            }
            return QuartzTimer.Schedule(delay, job);
        }

        public JobKey Schedule(Action schedule, DateTime time)
        {
            var job = CreateJob();
            bool flag = scheduleDic.TryAdd(job.Key, schedule);
            if (!flag)
            {
                LOGGER.Error($"添加失败[重复key]{job.Key.Name}");
                return job.Key;
            }
            return QuartzTimer.Schedule(time, job);
        }

        public JobKey ScheduleAtFixedRate(Runnable runnable, long delay, long period)
        {
            var job = CreateJob();
            bool flag = scheduleDic.TryAdd(job.Key, runnable.Run);
            if (!flag)
            {
                LOGGER.Error($"添加失败[重复key]{job.Key.Name}");
                return job.Key;
            }
            return QuartzTimer.ScheduleAtFixedRate(delay, period, job);
        }

        public JobKey ScheduleAtFixedRate(Action schedule, long delay, long period)
        {
            var job = CreateJob();
            bool flag = scheduleDic.TryAdd(job.Key, schedule);
            if (!flag)
            {
                LOGGER.Error($"添加失败[重复key]{job.Key.Name}");
                return job.Key;
            }
            return QuartzTimer.ScheduleAtFixedRate(delay, period, job);
        }

        public JobKey ScheduleAtFixedRate(Action schedule, TimeSpan delay, TimeSpan period)
        {
            var job = CreateJob();
            bool flag = scheduleDic.TryAdd(job.Key, schedule);
            if (!flag)
            {
                LOGGER.Error($"添加失败[重复key]{job.Key.Name}");
                return job.Key;
            }
            return QuartzTimer.ScheduleAtFixedRate((long)delay.TotalMilliseconds, (long)period.TotalMilliseconds, job);
        }

        /// <summary>
        /// 每天
        /// </summary>
        public JobKey AddDailySchedule(Action schedule, int hour, int minute)
        {
            var job = CreateJob();
            bool flag = scheduleDic.TryAdd(job.Key, schedule);
            if (!flag)
            {
                LOGGER.Error($"添加失败[重复key]{job.Key.Name}");
                return job.Key;
            }
            return QuartzTimer.ScheduleDaily(hour, minute, job);
        }

        /// <summary>
        /// 每周
        /// </summary>
        public JobKey AddWeeklySchedule(Action schedule, DayOfWeek[] days, int hour, int minute)
        {
            var job = CreateJob();
            bool flag = scheduleDic.TryAdd(job.Key, schedule);
            if (!flag)
            {
                LOGGER.Error($"添加失败[重复key]{job.Key.Name}");
                return job.Key;
            }
            return QuartzTimer.ScheduleWeekly(days, hour, minute, job);
        }

        /// <summary>
        /// 每月
        /// </summary>
        public JobKey AddMonthlySchedule(Action schedule, int dayOfMonth, int hour, int minute)
        {
            var job = CreateJob();
            bool flag = scheduleDic.TryAdd(job.Key, schedule);
            if (!flag)
            {
                LOGGER.Error($"添加失败[重复key]{job.Key.Name}");
                return job.Key;
            }
            return QuartzTimer.ScheduleMonthly(dayOfMonth, hour, minute, job);
        }

    }
}
