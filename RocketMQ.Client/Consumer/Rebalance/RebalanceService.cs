namespace RocketMQ.Client
{
    public class RebalanceService : ServiceThread
    {
        static NLog.Logger log = NLog.LogManager.GetCurrentClassLogger();
        private static int waitInterval = int.Parse(Sys.getProperty("rocketmq.client.rebalance.waitInterval", "20000"));
        //private final InternalLogger log = ClientLogger.getLog();
        private readonly MQClientInstance mqClientFactory;

        public RebalanceService(MQClientInstance mqClientFactory)
        {
            this.mqClientFactory = mqClientFactory;
        }

        //@Override
        public override void run()
        {
            log.Info(this.getServiceName() + " service started");
            while (!this.isStopped())
            {
                this.waitForRunning(waitInterval);
                this.mqClientFactory.doRebalance();
            }
            log.Info(this.getServiceName() + " service end");
        }

        //@Override
        public override string getServiceName()
        {
            return typeof(RebalanceService).Name;
            //return RebalanceService.class.getSimpleName();
        }
    }
}
