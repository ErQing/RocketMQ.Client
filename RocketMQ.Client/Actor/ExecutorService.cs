using System;
using System.Threading.Tasks;

namespace RocketMQ.Client
{
    public class ExecutorService
    {
        protected Actor worker;

        public ExecutorService(int parallelism = 1, int capacity = -1)
        {
            worker = new Actor(parallelism, capacity);
        }

        public void Submit(IRunnable work)
        {
            _ = worker.SendAsync(work.run);
        }

        public void Submit(Runnable work)
        {
            _ = worker.SendAsync(work.Run);
        }

        public void Submit(Action work)
        {
            _ = worker.SendAsync(work);
        }

        /// <summary>
        /// Initiates an orderly shutdown in which previously submitted
        /// tasks are executed, but no new tasks will be accepted.
        /// Invocation has no additional effect if already shut down.
        /// </summary>
        public async Task Shutdown()
        {
            await worker.Shutdown();
        }

        public void setCorePoolSize(int corePoolSize)
        {
            throw new NotImplementedException();
        }

        internal int getCorePoolSize()
        {
            throw new NotImplementedException();
        }

        public int GetWaitingCount()
        {
            return worker.GetWaitingCount();
        }

    }
}
