using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RocketMQ.Client
{
    public class TransactionMQProducer : DefaultMQProducer
    {
        private TransactionCheckListener transactionCheckListener;
        private int checkThreadPoolMinSize = 1;
        private int checkThreadPoolMaxSize = 1;
        private int checkRequestHoldMax = 2000;

        private ExecutorService executorService;

        private TransactionListener transactionListener;

        public TransactionMQProducer()
        {
        }

        public TransactionMQProducer(String producerGroup): this(null, producerGroup, null)
        {
            
        }

        public TransactionMQProducer(String nameSpace, String producerGroup) : this(nameSpace, producerGroup, null)
        {
       
    }

    public TransactionMQProducer(String producerGroup, RPCHook rpcHook) : this(null, producerGroup, rpcHook)
        {
    }

    public TransactionMQProducer(String nameSpace, String producerGroup, RPCHook rpcHook) : base(nameSpace, producerGroup, rpcHook)
    {
        
    }

    public TransactionMQProducer(String nameSpace, String producerGroup, RPCHook rpcHook, bool enableMsgTrace, String customizedTraceTopic) 
        :base(nameSpace, producerGroup, rpcHook, enableMsgTrace, customizedTraceTopic)
    {
        
    }

    public void start()
    {
        this.defaultMQProducerImpl.initTransactionEnv();
        base.start();
    }

    public void shutdown()
    {
        base.shutdown();
        this.defaultMQProducerImpl.destroyTransactionEnv();
    }

    /**
     * This method will be removed in the version 5.0.0, method <code>sendMessageInTransaction(Message,Object)</code>}
     * is recommended.
     */
    //@Override
    [Obsolete]//@Deprecated
    public TransactionSendResult sendMessageInTransaction(Message msg,
        LocalTransactionExecuter tranExecuter, Object arg) 
    {
        if (null == this.transactionCheckListener) {
            throw new MQClientException("localTransactionBranchCheckListener is null", null);
        }

        msg.setTopic(NamespaceUtil.wrapNamespace(this.getNamespace(), msg.getTopic()));
        return this.defaultMQProducerImpl.sendMessageInTransaction(msg, tranExecuter, arg);
    }

    public TransactionSendResult sendMessageInTransaction(Message msg,
        Object arg)
    {
        if (null == this.transactionListener) {
            throw new MQClientException("TransactionListener is null", null);
        }

        msg.setTopic(NamespaceUtil.wrapNamespace(this.getNamespace(), msg.getTopic()));
        return this.defaultMQProducerImpl.sendMessageInTransaction(msg, null, arg);
    }

    public TransactionCheckListener getTransactionCheckListener()
    {
        return transactionCheckListener;
    }

    /**
     * This method will be removed in the version 5.0.0 and set a custom thread pool is recommended.
     */
    [Obsolete]//@Deprecated
    public void setTransactionCheckListener(TransactionCheckListener transactionCheckListener)
    {
        this.transactionCheckListener = transactionCheckListener;
    }

    public int getCheckThreadPoolMinSize()
    {
        return checkThreadPoolMinSize;
    }

    /**
     * This method will be removed in the version 5.0.0 and set a custom thread pool is recommended.
     */
    [Obsolete]//@Deprecated
    public void setCheckThreadPoolMinSize(int checkThreadPoolMinSize)
    {
        this.checkThreadPoolMinSize = checkThreadPoolMinSize;
    }

    public int getCheckThreadPoolMaxSize()
    {
        return checkThreadPoolMaxSize;
    }

    /**
     * This method will be removed in the version 5.0.0 and set a custom thread pool is recommended.
     */
    [Obsolete]//@Deprecated
    public void setCheckThreadPoolMaxSize(int checkThreadPoolMaxSize)
    {
        this.checkThreadPoolMaxSize = checkThreadPoolMaxSize;
    }

    public int getCheckRequestHoldMax()
    {
        return checkRequestHoldMax;
    }

        /**
         * This method will be removed in the version 5.0.0 and set a custom thread pool is recommended.
         */
        [Obsolete]//@Deprecated
        public void setCheckRequestHoldMax(int checkRequestHoldMax)
    {
        this.checkRequestHoldMax = checkRequestHoldMax;
    }

    public ExecutorService getExecutorService()
    {
        return executorService;
    }

    public void setExecutorService(ExecutorService executorService)
    {
        this.executorService = executorService;
    }

    public TransactionListener getTransactionListener()
    {
        return transactionListener;
    }

    public void setTransactionListener(TransactionListener transactionListener)
    {
        this.transactionListener = transactionListener;
    }
    }
}
