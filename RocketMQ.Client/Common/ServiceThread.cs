using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace RocketMQ.Client
{
    public abstract class ServiceThread : IRunnable
    {
        public enum Model
        {
            OnDemand,  //按需执行
            Looping       //循环执行
        }
        //private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.COMMON_LOGGER_NAME);
        static NLog.Logger log = NLog.LogManager.GetCurrentClassLogger();
        private static readonly long JOIN_TIME = 90 * 1000;
        //protected readonly Thread thread;
        protected volatile bool hasNotified = false;
        protected volatile bool stopped = false;
        private readonly Actor actor;
        public Actor Executor { get { return actor; } }
        protected abstract Model ExecuteModel { get; }


        public ServiceThread()
        {
            actor = new Actor();
        }

        public ServiceThread(int capacity)
        {
            //this.thread = new Thread(this, this.getServiceName());
            actor = new Actor(1, capacity);
        }

        public int GetWaitingCount()
        {
            return actor.GetWaitingCount();
        }

        public abstract string getServiceName();

        public void start()
        {
            //this.thread.start();
            log.Info(this.getServiceName() + " service started");
            if (ExecuteModel == Model.Looping)
                actor.SendAsync(run, false, -1);
        }

        public abstract void run();

        //public void shutdown()
        //{
        //    this.shutdown(false);
        //}

        /// <summary>
        /// 是否等待队列的消息执行完成
        /// </summary>
        /// <param name="waitCmp"></param>
        /// <returns></returns>
        public async Task Shutdown(bool waitCmp = false)
        {
            stopped = true;
            if (waitCmp)
                await actor.Shutdown().WaitAsync(TimeSpan.FromMilliseconds(JOIN_TIME));
            else
                _ = actor.Shutdown();
            log.Info(this.getServiceName() + " service end");
        }

        //public void shutdown(bool interrupt)
        //{
        //    this.stopped = true;
        //    log.Info("shutdown thread " + this.getServiceName() + " interrupt " + interrupt);
        //    lock(this) 
        //    {
        //        if (!this.hasNotified)
        //        {
        //            this.hasNotified = true;
        //            this.notify();
        //        }
        //    }

        //    try
        //    {
        //        if (interrupt)
        //        {
        //            this.thread.interrupt();
        //        }
        //        long beginTime = Sys.currentTimeMillis();
        //        this.thread.join(this.getJointime());
        //        long elapsedTime = Sys.currentTimeMillis() - beginTime;
        //        log.Info("join thread " + this.getServiceName() + " elapsed time(ms) " + elapsedTime + " " + this.getJointime());
        //    }
        //    catch (ThreadInterruptedException e)
        //    {
        //        log.Error("Interrupted", e);
        //    }
        //}

        public long getJointime()
        {
            return JOIN_TIME;
        }

        public bool isStopped()
        {
            return stopped;
        }

        public void wakeup()
        {
            throw new NotImplementedException();
        }

        protected void waitForRunning(long interval)
        {
            throw new NotImplementedException();
        }

    }
}
