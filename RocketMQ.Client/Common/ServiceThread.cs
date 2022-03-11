using System;
using System.Threading;
using System.Threading.Tasks;

namespace RocketMQ.Client
{
    public abstract class ServiceThread : IRunnable
    {
        //private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.COMMON_LOGGER_NAME);
        static NLog.Logger log = NLog.LogManager.GetCurrentClassLogger();
        private static readonly int JOIN_TIME = 90 * 1000;
        protected Thread thread;
        protected volatile bool stopped = false;
        private readonly Actor actor;
        public Actor Executor { get { return actor; } }

        //Make it able to restart the thread
        private readonly AtomicBoolean started = new AtomicBoolean(false);
        protected bool isDaemon { get; set; } = false;
        protected readonly CountdownEvent waitPoint = new CountdownEvent(1);
        protected volatile AtomicBoolean hasNotified = new AtomicBoolean(false);

        public int GetWaitingCount()
        {
            return actor.GetWaitingCount();
        }

        public abstract string getServiceName();

        public void start()
        {
            log.Info("Try to start service thread:{} started:{} lastThread:{}", getServiceName(), started.get(), thread);
            if (!started.CompareAndSet(false, true))
            {
                return;
            }
            stopped = false;
            this.thread = new Thread(run);
            this.thread.Name = getServiceName();
            //this.thread.setDaemon(isDaemon);
            //this.thread.start();
            thread.IsBackground = isDaemon;
            thread.Start();
        }

        public abstract void run();

        public void shutdown()
        {
            this.shutdown(false);
        }

        public virtual void shutdown(bool interrupt)
        {
            log.Info("Try to shutdown service thread:{} started:{} lastThread:{}", getServiceName(), started.get(), thread);
            if (!started.CompareAndSet(true, false))
            {
                return;
            }
            this.stopped = true;
            log.Info("shutdown thread " + this.getServiceName() + " interrupt " + interrupt);

            if (hasNotified.CompareAndSet(false, true))
            {
                //waitPoint.countDown(); // notify
                waitPoint.Signal(); // notify
            }

            try
            {
                if (interrupt)
                {
                    this.thread.Interrupt();
                }

                long beginTime = Sys.currentTimeMillis();
                //if (!this.thread.isDaemon())
                if (!this.thread.IsBackground)
                {
                    this.thread.Join(this.getJointime());
                }
                long elapsedTime = Sys.currentTimeMillis() - beginTime;
                log.Info("join thread " + this.getServiceName() + " elapsed time(ms) " + elapsedTime + " "
                    + this.getJointime());
            }
            catch (ThreadInterruptedException e)
            {
                log.Error("Interrupted", e);
            }
        }

        public int getJointime()
        {
            return JOIN_TIME;
        }

        public void makeStop()
        {
            if (!started.get())
            {
                return;
            }
            this.stopped = true;
            log.Info("makestop thread " + this.getServiceName());
        }

        public void wakeup()
        {
            if (hasNotified.CompareAndSet(false, true))
            {
                //waitPoint.countDown(); // notify
                waitPoint.Signal(); // notify
            }
        }

        protected void waitForRunning(int interval)
        {
            if (hasNotified.CompareAndSet(true, false))
            {
                this.onWaitEnd();
                return;
            }

            //entry to wait
            waitPoint.Reset();
            try
            {
                waitPoint.Wait(interval);
            }
            catch (ThreadInterruptedException e)
            {
                log.Error("Interrupted", e);
            }
            finally
            {
                hasNotified.Set(false);
                this.onWaitEnd();
            }
        }

        protected void onWaitEnd()
        {
        }

        public bool isStopped()
        {
            return stopped;
        }

    }
}
