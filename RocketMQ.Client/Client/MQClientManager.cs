using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RocketMQ.Client
{
    public class MQClientManager
    {
        //private final static InternalLogger log = ClientLogger.getLog();
        static NLog.Logger log = NLog.LogManager.GetCurrentClassLogger();
        private static MQClientManager instance = new MQClientManager();
        private AtomicInteger factoryIndexGenerator = new AtomicInteger();
        private ConcurrentDictionary<String/* clientId */, MQClientInstance> factoryTable =
            new ConcurrentDictionary<String, MQClientInstance>();

        private MQClientManager()
        {

        }

        public static MQClientManager getInstance()
        {
            return instance;
        }

        public MQClientInstance getOrCreateMQClientInstance(ClientConfig clientConfig)
        {
            return getOrCreateMQClientInstance(clientConfig, null);
        }

        public MQClientInstance getOrCreateMQClientInstance(ClientConfig clientConfig, RPCHook rpcHook)
        {
            String clientId = clientConfig.buildMQClientId();
            //TODO 双重判断解决多线程问题？？？？
            //MQClientInstance instance = this.factoryTable.get(clientId);
            //if (null == instance)
            //{
            //    instance =
            //        new MQClientInstance(clientConfig.cloneClientConfig(),
            //            this.factoryIndexGenerator.getAndIncrement(), clientId, rpcHook);
            //    MQClientInstance prev = this.factoryTable.putIfAbsent(clientId, instance);
            //    if (prev != null)
            //    {
            //        instance = prev;
            //        log.Warn("Returned Previous MQClientInstance for clientId:[{}]", clientId);
            //    }
            //    else
            //    {
            //        log.Info("Created new MQClientInstance for clientId:[{}]", clientId);
            //    }
            //}
            this.factoryTable.TryGetValue(clientId, out MQClientInstance instance);
            if (null == instance)
            {
                instance =
                    new MQClientInstance(clientConfig.cloneClientConfig(),
                        this.factoryIndexGenerator.getAndIncrement(), clientId, rpcHook);
                var flag = factoryTable.TryAdd(clientId, instance);
                if (flag)
                {
                    log.Warn("Returned Previous MQClientInstance for clientId:[{}]", clientId);
                }
                else
                {
                    instance = factoryTable[clientId];
                    log.Info("Created new MQClientInstance for clientId:[{}]", clientId);
                }
            }

            return instance;
        }

        public void removeClientFactory(String clientId)
        {
            //this.factoryTable.remove(clientId);
            this.factoryTable.TryRemove(clientId, out _);
        }
    }
}
