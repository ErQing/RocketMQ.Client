using System;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace RocketMQ.Client
{
    /// <summary>
    /// IsBackground = true
    /// </summary>
    public class Actor
    {
        readonly static NLog.Logger LOGGER = NLog.LogManager.GetCurrentClassLogger();
        public const int TIME_OUT = 10000;
        private bool reentrant = true;   //是否可重入
        /// <summary>
        /// 当前调用链id
        /// </summary>
        internal long curCallChainId;   
        private static long idCounter = 1;
        //public virtual long ActorId { get; set; }
        public Actor(int parallelism = 1, int capacity=-1, bool reentrant=true)
        {
            this.reentrant = reentrant;
            actionBlock = new ActionBlock<WorkWrapper>(InnerRun, 
                new ExecutionDataflowBlockOptions() { MaxDegreeOfParallelism = parallelism, BoundedCapacity = capacity });
        }

        readonly ActionBlock<WorkWrapper> actionBlock;

        static async Task InnerRun(WorkWrapper wrapper)
        {
            if (wrapper.TimeOut == -1)
            {
                await wrapper.DoTask();
            }
            else
            {
                var task = wrapper.DoTask();
                var res = await task.WaitAsync(TimeSpan.FromMilliseconds(wrapper.TimeOut));
                if (res)
                {
                    LOGGER.Fatal("wrapper执行超时:" + wrapper.GetTrace());
                    //强制设状态-取消该操作
                    wrapper.ForceSetResult();
                }
            }
        }

        internal long IsNeedEnqueue()
        {
            long callChainId = RuntimeContext.Current;
            if (callChainId > 0)
            {
                if (callChainId == curCallChainId)
                    return -1;
                return callChainId;
            }
            return NewChainId();
        }

        internal long NewChainId()
        {
            return Interlocked.Increment(ref idCounter);
        }

        public Task SendAsync(Action work, bool isAwait = true, int timeOut = TIME_OUT)
        {
            bool needEnqueue;
            if (!isAwait || !reentrant)
                needEnqueue = true;
            else
                needEnqueue = IsNeedEnqueue() > 0;
            if (needEnqueue)
            {
                ActionWrapper at = new ActionWrapper(work);
                at.Owner = this;
                at.TimeOut = timeOut;
                at.CallChainId = Interlocked.Increment(ref idCounter); 
                actionBlock.SendAsync(at);
                return at.Tcs.Task;
            }
            else
            {
                work();
                return Task.CompletedTask;
            }
        }

        public Task<T> SendAsync<T>(Func<T> work, bool isAwait = true, int timeOut = TIME_OUT)
        {
            bool needEnqueue;
            if (!isAwait || !reentrant)
                needEnqueue = true;
            else
                needEnqueue = IsNeedEnqueue() > 0;
            if (needEnqueue)
            {
                FuncWrapper<T> at = new FuncWrapper<T>(work);
                at.Owner = this;
                at.TimeOut = timeOut;
                at.CallChainId = Interlocked.Increment(ref idCounter); 
                actionBlock.SendAsync(at);
                return at.Tcs.Task;
            }
            else
            {
                return Task.FromResult(work());
            }
        }

        public Task SendAsync(Func<Task> work, bool isAwait = true, int timeOut = TIME_OUT)
        {
            bool needEnqueue;
            if (!isAwait || !reentrant)
                needEnqueue = true;
            else
                needEnqueue = IsNeedEnqueue() > 0;
            if (needEnqueue)
            {
                ActionAsyncWrapper at = new ActionAsyncWrapper(work);
                at.Owner = this;
                at.TimeOut = timeOut;
                at.CallChainId = Interlocked.Increment(ref idCounter); 
                actionBlock.SendAsync(at);
                return at.Tcs.Task;
            }
            else
            {
                return work();
            }
        }

        public Task<T> SendAsync<T>(Func<Task<T>> work, bool isAwait = true, int timeOut = TIME_OUT)
        {
            bool needEnqueue;
            if (!isAwait || !reentrant)
                needEnqueue = true;
            else
                needEnqueue = IsNeedEnqueue() > 0;
            if (needEnqueue)
            {
                FuncAsyncWrapper<T> at = new FuncAsyncWrapper<T>(work);
                at.Owner = this;
                at.TimeOut = timeOut;
                at.CallChainId = Interlocked.Increment(ref idCounter);
                actionBlock.SendAsync(at);
                return at.Tcs.Task;
            }
            else
            {
                return work();
            }
        }

        public Task Shutdown()
        {
            actionBlock.Complete();
            return actionBlock.Completion;
        }

        public int GetWaitingCount()
        {
            return actionBlock.InputCount;
        }

    }
}
