using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;

namespace RocketMQ.Client
{
    public class RequestFutureHolder
    {
        //private static InternalLogger log = ClientLogger.getLog();
        static NLog.Logger log = NLog.LogManager.GetCurrentClassLogger();
        private static readonly RequestFutureHolder INSTANCE = new RequestFutureHolder();
        private ConcurrentDictionary<String, RequestResponseFuture> requestFutureTable = new ConcurrentDictionary<String, RequestResponseFuture>();
        private readonly HashSet<DefaultMQProducerImpl> producerSet = new HashSet<DefaultMQProducerImpl>();
        private ScheduledExecutorService scheduledExecutorService = null;

        public ConcurrentDictionary<String, RequestResponseFuture> getRequestFutureTable()
        {
            return requestFutureTable;
        }

        private void scanExpiredRequest()
        {
            List<RequestResponseFuture> rfList = new List<RequestResponseFuture>();
            //Iterator<Map.Entry<String, RequestResponseFuture>> it = requestFutureTable.entrySet().iterator();
            //while (it.hasNext())
            //{
            //    Map.Entry<String, RequestResponseFuture> next = it.next();
            //    RequestResponseFuture rep = next.getValue();

            //    if (rep.isTimeout())
            //    {
            //        it.remove();
            //        rfList.add(rep);
            //        log.Warn("remove timeout request, CorrelationId={}" + rep.getCorrelationId());
            //    }
            //}
            //c# dictionary 可以遍历时删除
            foreach (var item in requestFutureTable)
            {
                if (item.Value.isTimeout())
                {
                    requestFutureTable.TryRemove(item.Key, out _);
                    rfList.Add(item.Value);
                    log.Warn("remove timeout request, CorrelationId={}" + item.Value.getCorrelationId());
                }
            }


            foreach (RequestResponseFuture rf in rfList)
            {
                try
                {
                    Exception cause = new RequestTimeoutException(ClientErrorCode.REQUEST_TIMEOUT_EXCEPTION, "request timeout, no reply message.");
                    rf.setCause(cause);
                    rf.executeRequestCallback();
                }
                catch (Exception e)
                {
                    log.Warn("scanResponseTable, operationComplete Exception", e.ToString());
                }
            }
        }




        class ScanExpiredRequest : IRunnable
        {
            public void run()
            {
                try
                {
                    RequestFutureHolder.getInstance().scanExpiredRequest();
                }
                catch (Exception e)
                {
                    log.Error("scan RequestFutureTable exception", e.ToString());
                }
            }
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public void startScheduledTask(DefaultMQProducerImpl producer)
        {
            this.producerSet.Add(producer);
            if (null == scheduledExecutorService)
            {
                //this.scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl("RequestHouseKeepingService"));
                //    this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
                //    @Override
                //    public void run()
                //    {
                //        try
                //        {
                //            RequestFutureHolder.getInstance().scanExpiredRequest();
                //        }
                //        catch (Throwable e)
                //        {
                //            log.Error("scan RequestFutureTable exception", e);
                //        }
                //    }
                //}, 1000 * 3, 1000, TimeUnit.MILLISECONDS);
                scheduledExecutorService = new ScheduledExecutorService();
                scheduledExecutorService.ScheduleAtFixedRate(new ScanExpiredRequest().run, 1000 * 3, 1000);
            }
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public void shutdown(DefaultMQProducerImpl producer)
        {
            this.producerSet.Remove(producer);
            if (this.producerSet.Count <= 0 && null != this.scheduledExecutorService)
            {
                ScheduledExecutorService executorService = this.scheduledExecutorService;
                this.scheduledExecutorService = null;
                executorService.Shutdown();
            }
        }

        private RequestFutureHolder() { }

        public static RequestFutureHolder getInstance()
        {
            return INSTANCE;
        }
    }
}
