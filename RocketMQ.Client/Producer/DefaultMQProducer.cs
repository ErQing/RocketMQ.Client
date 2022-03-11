using System;
using System.Collections.Generic;

namespace RocketMQ.Client
{
    public class DefaultMQProducer : ClientConfig, MQProducer
    {
        /**
         * Wrapping internal implementations for virtually all methods presented in this class.
         * TODO
         */
        //protected readonly transient DefaultMQProducerImpl defaultMQProducerImpl
            protected readonly DefaultMQProducerImpl defaultMQProducerImpl;
        //private readonly InternalLogger log = ClientLogger.getLog();
        static NLog.Logger log = NLog.LogManager.GetCurrentClassLogger();

        //    private readonly HashSet<int> retryResponseCodes = new CopyOnWriteArraySet<int>(Array.asList(
        //        ResponseCode.TOPIC_NOT_EXIST,
        //        ResponseCode.SERVICE_NOT_AVAILABLE,
        //        ResponseCode.SYSTEM_ERROR,
        //        ResponseCode.NO_PERMISSION,
        //        ResponseCode.NO_BUYER_ID,
        //        ResponseCode.NOT_IN_CURRENT_UNIT
        //));

        //CopyOnWriteArraySet是为了线程安全，但此处没有任何地方对该容器进行修改
        //可以直接使用HashSet，如果后续版本有修改，需修改实现方式 //???
        private readonly HashSet<int> retryResponseCodes = new HashSet<int>() { ResponseCode.TOPIC_NOT_EXIST,
                                                                         ResponseCode.SERVICE_NOT_AVAILABLE,
                                                                         ResponseCode.SYSTEM_ERROR,
                                                                         ResponseCode.NO_PERMISSION,
                                                                         ResponseCode.NO_BUYER_ID,
                                                                         ResponseCode.NOT_IN_CURRENT_UNIT };

            /**
             * Producer group conceptually aggregates all producer instances of exactly same role, which is particularly
             * important when transactional messages are involved. </p>
             *
             * For non-transactional messages, it does not matter as long as it's unique per process. </p>
             *
             * See {@linktourl http://rocketmq.apache.org/docs/core-concept/} for more discussion.
             */
        private string producerGroup;

        /**
         * Just for testing or demo program
         */
        private string createTopicKey = TopicValidator.AUTO_CREATE_TOPIC_KEY_TOPIC;

        /**
         * Number of queues to create per default topic.
         */
        private volatile int defaultTopicQueueNums = 4;

        /**
         * Timeout for sending messages.
         */
        private int sendMsgTimeout = 3000;

        /**
         * Compress message body threshold, namely, message body larger than 4k will be compressed on default.
         */
        private int compressMsgBodyOverHowmuch = 1024 * 4;

        /**
         * Maximum number of retry to perform internally before claiming sending failure in synchronous mode. </p>
         *
         * This may potentially cause message duplication which is up to application developers to resolve.
         */
        private int retryTimesWhenSendFailed = 2;

        /**
         * Maximum number of retry to perform internally before claiming sending failure in asynchronous mode. </p>
         *
         * This may potentially cause message duplication which is up to application developers to resolve.
         */
        private int retryTimesWhenSendAsyncFailed = 2;

        /**
         * Indicate whether to retry another broker on sending failure internally.
         */
        private bool retryAnotherBrokerWhenNotStoreOK = false;

        /**
         * Maximum allowed message size in bytes.
         */
        private int maxMessageSize = 1024 * 1024 * 4; // 4M

        /**
         * Interface of asynchronous transfer data
         */
        private TraceDispatcher traceDispatcher = null;

        /**
         * Default constructor.
         */
        public DefaultMQProducer() : this(null, MixAll.DEFAULT_PRODUCER_GROUP, null)
        {
            
        }

        /**
         * Constructor specifying the RPC hook.
         *
         * @param rpcHook RPC hook to execute per each remoting command execution.
         */
        public DefaultMQProducer(RPCHook rpcHook) : this(null, MixAll.DEFAULT_PRODUCER_GROUP, rpcHook)
        {

        }

        /**
         * Constructor specifying producer group.
         *
         * @param producerGroup Producer group, see the name-sake field.
         */
        public DefaultMQProducer(String producerGroup) : this(null, producerGroup, null)
        {

        }

        /**
         * Constructor specifying producer group.
         *
         * @param producerGroup Producer group, see the name-sake field.
         * @param rpcHook RPC hook to execute per each remoting command execution.
         * @param enableMsgTrace Switch flag instance for message trace.
         * @param customizedTraceTopic The name value of message trace topic.If you don't config,you can use the default
         * trace topic name.
         */
        public DefaultMQProducer(String producerGroup, RPCHook rpcHook, bool enableMsgTrace, string customizedTraceTopic)
            : this(null, producerGroup, rpcHook, enableMsgTrace, customizedTraceTopic)
        {

        }

        /**
         * Constructor specifying producer group.
         *
         * @param namespace Namespace for this MQ Producer instance.
         * @param producerGroup Producer group, see the name-sake field.
         */
        public DefaultMQProducer(String nameSpace, string producerGroup) : this(nameSpace, producerGroup, null)
        {

        }

        /**
         * Constructor specifying both producer group and RPC hook.
         *
         * @param producerGroup Producer group, see the name-sake field.
         * @param rpcHook RPC hook to execute per each remoting command execution.
         */
        public DefaultMQProducer(String producerGroup, RPCHook rpcHook) : this(null, producerGroup, rpcHook)
        {

        }

        /**
         * Constructor specifying namespace, producer group and RPC hook.
         *
         * @param namespace Namespace for this MQ Producer instance.
         * @param producerGroup Producer group, see the name-sake field.
         * @param rpcHook RPC hook to execute per each remoting command execution.
         */
        public DefaultMQProducer(String nameSpace, string producerGroup, RPCHook rpcHook)
        {
            this.nameSpace = nameSpace;
            this.producerGroup = producerGroup;
            defaultMQProducerImpl = new DefaultMQProducerImpl(this, rpcHook);
        }

        /**
         * Constructor specifying producer group and enabled msg trace flag.
         *
         * @param producerGroup Producer group, see the name-sake field.
         * @param enableMsgTrace Switch flag instance for message trace.
         */
        public DefaultMQProducer(String producerGroup, bool enableMsgTrace) : this(null, producerGroup, null, enableMsgTrace, null)
        {

        }

        /**
         * Constructor specifying producer group, enabled msgTrace flag and customized trace topic name.
         *
         * @param producerGroup Producer group, see the name-sake field.
         * @param enableMsgTrace Switch flag instance for message trace.
         * @param customizedTraceTopic The name value of message trace topic.If you don't config,you can use the default
         * trace topic name.
         */
        public DefaultMQProducer(String producerGroup, bool enableMsgTrace, string customizedTraceTopic)
                : this(null, producerGroup, null, enableMsgTrace, customizedTraceTopic)
        {

        }

        /**
         * Constructor specifying namespace, producer group, RPC hook, enabled msgTrace flag and customized trace topic
         * name.
         *
         * @param namespace Namespace for this MQ Producer instance.
         * @param producerGroup Producer group, see the name-sake field.
         * @param rpcHook RPC hook to execute per each remoting command execution.
         * @param enableMsgTrace Switch flag instance for message trace.
         * @param customizedTraceTopic The name value of message trace topic.If you don't config,you can use the default
         * trace topic name.
         */
        public DefaultMQProducer(String nameSpace, string producerGroup, RPCHook rpcHook,
            bool enableMsgTrace, string customizedTraceTopic)
        {
            this.nameSpace = nameSpace;
            this.producerGroup = producerGroup;
            defaultMQProducerImpl = new DefaultMQProducerImpl(this, rpcHook);
            //if client open the message trace feature
            if (enableMsgTrace)
            {
                try
                {
                    AsyncTraceDispatcher dispatcher = new AsyncTraceDispatcher(producerGroup, TraceDispatcher.Type.PRODUCE, customizedTraceTopic, rpcHook);
                    dispatcher.setHostProducer(this.defaultMQProducerImpl);
                    traceDispatcher = dispatcher;
                    this.defaultMQProducerImpl.registerSendMessageHook(
                        new SendMessageTraceHookImpl(traceDispatcher));
                    this.defaultMQProducerImpl.registerEndTransactionHook(
                        new EndTransactionTraceHookImpl(traceDispatcher));
                }
                catch (Exception e)
                {
                    log.Error("system mqtrace hook init failed ,maybe can't send msg trace data");
                }
            }
        }

        //@Override
        public void setUseTLS(bool useTLS)
        {
            base.setUseTLS(useTLS);
            if (traceDispatcher != null && traceDispatcher is AsyncTraceDispatcher) {
                ((AsyncTraceDispatcher)traceDispatcher).getTraceProducer().setUseTLS(useTLS);
            }
        }

        /**
         * Start this producer instance. </p>
         *
         * <strong> Much internal initializing procedures are carried out to make this instance prepared, thus, it's a must
         * to invoke this method before sending or querying messages. </strong> </p>
         *
         * @throws MQClientException if there is any unexpected error.
         */
        //@Override
        public void start()
        {
            this.setProducerGroup(withNamespace(this.producerGroup));
            this.defaultMQProducerImpl.start();
            if (null != traceDispatcher)
            {
                try
                {
                    traceDispatcher.start(this.getNamesrvAddr(), this.getAccessChannel());
                }
                catch (MQClientException e)
                {
                    log.Warn("trace dispatcher start failed ", e);
                }
            }
        }

        /**
         * This method shuts down this producer instance and releases related resources.
         */
        //@Override
        public void shutdown()
        {
            this.defaultMQProducerImpl.shutdown();
            if (null != traceDispatcher)
            {
                traceDispatcher.shutdown();
            }
        }

        /**
         * Fetch message queues of topic <code>topic</code>, to which we may send/publish messages.
         *
         * @param topic Topic to fetch.
         * @return List of message queues readily to send messages to
         * @throws MQClientException if there is any client error.
         */
        //@Override
        public List<MessageQueue> fetchPublishMessageQueues(String topic)
        {
            return this.defaultMQProducerImpl.fetchPublishMessageQueues(withNamespace(topic));
        }

        /**
         * Send message in synchronous mode. This method returns only when the sending procedure totally completes. </p>
         *
         * <strong>Warn:</strong> this method has internal retry-mechanism, that is, internal implementation will retry
         * {@link #retryTimesWhenSendFailed} times before claiming failure. As a result, multiple messages may potentially
         * delivered to broker(s). It's up to the application developers to resolve potential duplication issue.
         *
         * @param msg Message to send.
         * @return {@link SendResult} instance to inform senders details of the deliverable, say Message ID of the message,
         * {@link SendStatus} indicating broker storage/replication status, message queue sent to, etc.
         * @throws MQClientException if there is any client error.
         * @throws RemotingException if there is any network-tier error.
         * @throws MQBrokerException if there is any error with broker.
         * @throws InterruptedException if the sending thread is interrupted.
         */
        //@Override
        public SendResult send(Message msg)
        {
            msg.setTopic(withNamespace(msg.getTopic()));
            return this.defaultMQProducerImpl.send(msg);
        }

        /**
         * Same to {@link #send(Message)} with send timeout specified in addition.
         *
         * @param msg Message to send.
         * @param timeout send timeout.
         * @return {@link SendResult} instance to inform senders details of the deliverable, say Message ID of the message,
         * {@link SendStatus} indicating broker storage/replication status, message queue sent to, etc.
         * @throws MQClientException if there is any client error.
         * @throws RemotingException if there is any network-tier error.
         * @throws MQBrokerException if there is any error with broker.
         * @throws InterruptedException if the sending thread is interrupted.
         */
        //@Override
        public SendResult send(Message msg, long timeout)
        {
            msg.setTopic(withNamespace(msg.getTopic()));
            return this.defaultMQProducerImpl.send(msg, timeout);
        }

        /**
         * Send message to broker asynchronously. </p>
         *
         * This method returns immediately. On sending completion, <code>sendCallback</code> will be executed. </p>
         *
         * Similar to {@link #send(Message)}, internal implementation would potentially retry up to {@link
         * #retryTimesWhenSendAsyncFailed} times before claiming sending failure, which may yield message duplication and
         * application developers are the one to resolve this potential issue.
         *
         * @param msg Message to send.
         * @param sendCallback Callback to execute on sending completed, either successful or unsuccessful.
         * @throws MQClientException if there is any client error.
         * @throws RemotingException if there is any network-tier error.
         * @throws InterruptedException if the sending thread is interrupted.
         */
        //@Override
        public void send(Message msg,
            SendCallback sendCallback)
        {
            msg.setTopic(withNamespace(msg.getTopic()));
            this.defaultMQProducerImpl.send(msg, sendCallback);
        }

        /**
         * Same to {@link #send(Message, SendCallback)} with send timeout specified in addition.
         *
         * @param msg message to send.
         * @param sendCallback Callback to execute.
         * @param timeout send timeout.
         * @throws MQClientException if there is any client error.
         * @throws RemotingException if there is any network-tier error.
         * @throws InterruptedException if the sending thread is interrupted.
         */
        //@Override
        public void send(Message msg, SendCallback sendCallback, long timeout)
        {
            msg.setTopic(withNamespace(msg.getTopic()));
            this.defaultMQProducerImpl.send(msg, sendCallback, timeout);
        }

        /**
         * Similar to <a href="https://en.wikipedia.org/wiki/User_Datagram_Protocol">UDP</a>, this method won't wait for
         * acknowledgement from broker before return. Obviously, it has maximums throughput yet potentials of message loss.
         *
         * @param msg Message to send.
         * @throws MQClientException if there is any client error.
         * @throws RemotingException if there is any network-tier error.
         * @throws InterruptedException if the sending thread is interrupted.
         */
        //@Override
        public void sendOneway(Message msg)
        {
            msg.setTopic(withNamespace(msg.getTopic()));
            this.defaultMQProducerImpl.sendOneway(msg);
        }

        /**
         * Same to {@link #send(Message)} with target message queue specified in addition.
         *
         * @param msg Message to send.
         * @param mq Target message queue.
         * @return {@link SendResult} instance to inform senders details of the deliverable, say Message ID of the message,
         * {@link SendStatus} indicating broker storage/replication status, message queue sent to, etc.
         * @throws MQClientException if there is any client error.
         * @throws RemotingException if there is any network-tier error.
         * @throws MQBrokerException if there is any error with broker.
         * @throws InterruptedException if the sending thread is interrupted.
         */
        //@Override
        public SendResult send(Message msg, MessageQueue mq)
        {
            msg.setTopic(withNamespace(msg.getTopic()));
            return this.defaultMQProducerImpl.send(msg, queueWithNamespace(mq));
        }

        /**
         * Same to {@link #send(Message)} with target message queue and send timeout specified.
         *
         * @param msg Message to send.
         * @param mq Target message queue.
         * @param timeout send timeout.
         * @return {@link SendResult} instance to inform senders details of the deliverable, say Message ID of the message,
         * {@link SendStatus} indicating broker storage/replication status, message queue sent to, etc.
         * @throws MQClientException if there is any client error.
         * @throws RemotingException if there is any network-tier error.
         * @throws MQBrokerException if there is any error with broker.
         * @throws InterruptedException if the sending thread is interrupted.
         */
        //@Override
        public SendResult send(Message msg, MessageQueue mq, long timeout)
        {
            msg.setTopic(withNamespace(msg.getTopic()));
            return this.defaultMQProducerImpl.send(msg, queueWithNamespace(mq), timeout);
        }

        /**
         * Same to {@link #send(Message, SendCallback)} with target message queue specified.
         *
         * @param msg Message to send.
         * @param mq Target message queue.
         * @param sendCallback Callback to execute on sending completed, either successful or unsuccessful.
         * @throws MQClientException if there is any client error.
         * @throws RemotingException if there is any network-tier error.
         * @throws InterruptedException if the sending thread is interrupted.
         */
        //@Override
        public void send(Message msg, MessageQueue mq, SendCallback sendCallback)
        {
            msg.setTopic(withNamespace(msg.getTopic()));
            this.defaultMQProducerImpl.send(msg, queueWithNamespace(mq), sendCallback);
        }

        /**
         * Same to {@link #send(Message, SendCallback)} with target message queue and send timeout specified.
         *
         * @param msg Message to send.
         * @param mq Target message queue.
         * @param sendCallback Callback to execute on sending completed, either successful or unsuccessful.
         * @param timeout Send timeout.
         * @throws MQClientException if there is any client error.
         * @throws RemotingException if there is any network-tier error.
         * @throws InterruptedException if the sending thread is interrupted.
         */
        //@Override
        public void send(Message msg, MessageQueue mq, SendCallback sendCallback, long timeout)
        {
            msg.setTopic(withNamespace(msg.getTopic()));
            this.defaultMQProducerImpl.send(msg, queueWithNamespace(mq), sendCallback, timeout);
        }

        /**
         * Same to {@link #sendOneway(Message)} with target message queue specified.
         *
         * @param msg Message to send.
         * @param mq Target message queue.
         * @throws MQClientException if there is any client error.
         * @throws RemotingException if there is any network-tier error.
         * @throws InterruptedException if the sending thread is interrupted.
         */
        //@Override
        public void sendOneway(Message msg,
            MessageQueue mq)
        {
            msg.setTopic(withNamespace(msg.getTopic()));
            this.defaultMQProducerImpl.sendOneway(msg, queueWithNamespace(mq));
        }

        /**
         * Same to {@link #send(Message)} with message queue selector specified.
         *
         * @param msg Message to send.
         * @param selector Message queue selector, through which we get target message queue to deliver message to.
         * @param arg Argument to work along with message queue selector.
         * @return {@link SendResult} instance to inform senders details of the deliverable, say Message ID of the message,
         * {@link SendStatus} indicating broker storage/replication status, message queue sent to, etc.
         * @throws MQClientException if there is any client error.
         * @throws RemotingException if there is any network-tier error.
         * @throws MQBrokerException if there is any error with broker.
         * @throws InterruptedException if the sending thread is interrupted.
         */
        //@Override
        public SendResult send(Message msg, MessageQueueSelector selector, Object arg)
        {
            msg.setTopic(withNamespace(msg.getTopic()));
            return this.defaultMQProducerImpl.send(msg, selector, arg);
        }

        /**
         * Same to {@link #send(Message, MessageQueueSelector, Object)} with send timeout specified.
         *
         * @param msg Message to send.
         * @param selector Message queue selector, through which we get target message queue to deliver message to.
         * @param arg Argument to work along with message queue selector.
         * @param timeout Send timeout.
         * @return {@link SendResult} instance to inform senders details of the deliverable, say Message ID of the message,
         * {@link SendStatus} indicating broker storage/replication status, message queue sent to, etc.
         * @throws MQClientException if there is any client error.
         * @throws RemotingException if there is any network-tier error.
         * @throws MQBrokerException if there is any error with broker.
         * @throws InterruptedException if the sending thread is interrupted.
         */
        //@Override
        public SendResult send(Message msg, MessageQueueSelector selector, Object arg, long timeout)
        {
            msg.setTopic(withNamespace(msg.getTopic()));
            return this.defaultMQProducerImpl.send(msg, selector, arg, timeout);
        }

        /**
         * Same to {@link #send(Message, SendCallback)} with message queue selector specified.
         *
         * @param msg Message to send.
         * @param selector Message selector through which to get target message queue.
         * @param arg Argument used along with message queue selector.
         * @param sendCallback callback to execute on sending completion.
         * @throws MQClientException if there is any client error.
         * @throws RemotingException if there is any network-tier error.
         * @throws InterruptedException if the sending thread is interrupted.
         */
        //@Override
        public void send(Message msg, MessageQueueSelector selector, Object arg, SendCallback sendCallback)
        {
            msg.setTopic(withNamespace(msg.getTopic()));
            this.defaultMQProducerImpl.send(msg, selector, arg, sendCallback);
        }

        /**
         * Same to {@link #send(Message, MessageQueueSelector, Object, SendCallback)} with timeout specified.
         *
         * @param msg Message to send.
         * @param selector Message selector through which to get target message queue.
         * @param arg Argument used along with message queue selector.
         * @param sendCallback callback to execute on sending completion.
         * @param timeout Send timeout.
         * @throws MQClientException if there is any client error.
         * @throws RemotingException if there is any network-tier error.
         * @throws InterruptedException if the sending thread is interrupted.
         */
        //@Override
        public void send(Message msg, MessageQueueSelector selector, Object arg, SendCallback sendCallback, long timeout)
        {
            msg.setTopic(withNamespace(msg.getTopic()));
            this.defaultMQProducerImpl.send(msg, selector, arg, sendCallback, timeout);
        }

        /**
         * Send request message in synchronous mode. This method returns only when the consumer consume the request message and reply a message. </p>
         *
         * <strong>Warn:</strong> this method has internal retry-mechanism, that is, internal implementation will retry
         * {@link #retryTimesWhenSendFailed} times before claiming failure. As a result, multiple messages may potentially
         * delivered to broker(s). It's up to the application developers to resolve potential duplication issue.
         *
         * @param msg request message to send
         * @param timeout request timeout
         * @return reply message
         * @throws MQClientException if there is any client error.
         * @throws RemotingException if there is any network-tier error.
         * @throws MQBrokerException if there is any broker error.
         * @throws InterruptedException if the thread is interrupted.
         * @throws RequestTimeoutException if request timeout.
         */
        //@Override
        public Message request(Message msg, long timeout)
        {
            msg.setTopic(withNamespace(msg.getTopic()));
            return this.defaultMQProducerImpl.request(msg, timeout);
        }

        /**
         * Request asynchronously. </p>
         * This method returns immediately. On receiving reply message, <code>requestCallback</code> will be executed. </p>
         *
         * Similar to {@link #request(Message, long)}, internal implementation would potentially retry up to {@link
         * #retryTimesWhenSendAsyncFailed} times before claiming sending failure, which may yield message duplication and
         * application developers are the one to resolve this potential issue.
         *
         * @param msg request message to send
         * @param requestCallback callback to execute on request completion.
         * @param timeout request timeout
         * @throws MQClientException if there is any client error.
         * @throws RemotingException if there is any network-tier error.
         * @throws InterruptedException if the thread is interrupted.
         * @throws MQBrokerException if there is any broker error.
         */
        //@Override
        public void request(Message msg, RequestCallback requestCallback, long timeout)
        {
            msg.setTopic(withNamespace(msg.getTopic()));
            this.defaultMQProducerImpl.request(msg, requestCallback, timeout);
        }

        /**
         * Same to {@link #request(Message, long)}  with message queue selector specified.
         *
         * @param msg request message to send
         * @param selector message queue selector, through which we get target message queue to deliver message to.
         * @param arg argument to work along with message queue selector.
         * @param timeout timeout of request.
         * @return reply message
         * @throws MQClientException if there is any client error.
         * @throws RemotingException if there is any network-tier error.
         * @throws MQBrokerException if there is any broker error.
         * @throws InterruptedException if the thread is interrupted.
         * @throws RequestTimeoutException if request timeout.
         */
        //@Override
        public Message request(Message msg, MessageQueueSelector selector, Object arg,
            long timeout)
        {
            msg.setTopic(withNamespace(msg.getTopic()));
            return this.defaultMQProducerImpl.request(msg, selector, arg, timeout);
        }

        /**
         * Same to {@link #request(Message, RequestCallback, long)} with target message selector specified.
         *
         * @param msg requst message to send
         * @param selector message queue selector, through which we get target message queue to deliver message to.
         * @param arg argument to work along with message queue selector.
         * @param requestCallback callback to execute on request completion.
         * @param timeout timeout of request.
         * @throws MQClientException if there is any client error.
         * @throws RemotingException if there is any network-tier error.
         * @throws InterruptedException if the thread is interrupted.
         * @throws MQBrokerException if there is any broker error.
         */
        //@Override
        public void request(Message msg, MessageQueueSelector selector, Object arg,
            RequestCallback requestCallback, long timeout)
        {
            msg.setTopic(withNamespace(msg.getTopic()));
            this.defaultMQProducerImpl.request(msg, selector, arg, requestCallback, timeout);
        }

        /**
         * Same to {@link #request(Message, long)}  with target message queue specified in addition.
         *
         * @param msg request message to send
         * @param mq target message queue.
         * @param timeout request timeout
         * @throws MQClientException if there is any client error.
         * @throws RemotingException if there is any network-tier error.
         * @throws MQBrokerException if there is any broker error.
         * @throws InterruptedException if the thread is interrupted.
         * @throws RequestTimeoutException if request timeout.
         */
        //@Override
        public Message request(Message msg, MessageQueue mq, long timeout)
        {
            msg.setTopic(withNamespace(msg.getTopic()));
            return this.defaultMQProducerImpl.request(msg, mq, timeout);
        }

        /**
         * Same to {@link #request(Message, RequestCallback, long)} with target message queue specified.
         *
         * @param msg request message to send
         * @param mq target message queue.
         * @param requestCallback callback to execute on request completion.
         * @param timeout timeout of request.
         * @throws MQClientException if there is any client error.
         * @throws RemotingException if there is any network-tier error.
         * @throws InterruptedException if the thread is interrupted.
         * @throws MQBrokerException if there is any broker error.
         */
        //@Override
        public void request(Message msg, MessageQueue mq, RequestCallback requestCallback, long timeout)
        {
            msg.setTopic(withNamespace(msg.getTopic()));
            this.defaultMQProducerImpl.request(msg, mq, requestCallback, timeout);
        }

        /**
         * Same to {@link #sendOneway(Message)} with message queue selector specified.
         *
         * @param msg Message to send.
         * @param selector Message queue selector, through which to determine target message queue to deliver message
         * @param arg Argument used along with message queue selector.
         * @throws MQClientException if there is any client error.
         * @throws RemotingException if there is any network-tier error.
         * @throws InterruptedException if the sending thread is interrupted.
         */
        //@Override
        public void sendOneway(Message msg, MessageQueueSelector selector, Object arg)
        {
            msg.setTopic(withNamespace(msg.getTopic()));
            this.defaultMQProducerImpl.sendOneway(msg, selector, arg);
        }

        /**
         * This method is to send transactional messages.
         *
         * @param msg Transactional message to send.
         * @param tranExecuter local transaction executor.
         * @param arg Argument used along with local transaction executor.
         * @return Transaction result.
         * @throws MQClientException if there is any client error.
         */
        //@Override
        public TransactionSendResult sendMessageInTransaction(Message msg, LocalTransactionExecuter tranExecuter,
            Object arg)
        {
            throw new Exception("sendMessageInTransaction not implement, please use TransactionMQProducer class");
        }

        /**
         * This method is used to send transactional messages.
         *
         * @param msg Transactional message to send.
         * @param arg Argument used along with local transaction executor.
         * @return Transaction result.
         * @throws MQClientException
         */
        //@Override
        public TransactionSendResult sendMessageInTransaction(Message msg,
            Object arg)
        {
            throw new Exception("sendMessageInTransaction not implement, please use TransactionMQProducer class");
        }

        /**
         * This method will be removed in a certain version after April 5, 2020, so please do not use this method.
         *
         * @param key accesskey
         * @param newTopic topic name
         * @param queueNum topic's queue number
         * @throws MQClientException if there is any client error.
         */
        //@Deprecated
        //@Override
        [Obsolete]
        public void createTopic(String key, string newTopic, int queueNum)
        {
            createTopic(key, withNamespace(newTopic), queueNum, 0);
        }

        /**
         * Create a topic on broker. This method will be removed in a certain version after April 5, 2020, so please do not
         * use this method.
         *
         * @param key accesskey
         * @param newTopic topic name
         * @param queueNum topic's queue number
         * @param topicSysFlag topic system flag
         * @throws MQClientException if there is any client error.
         */
        //@Deprecated
        //@Override
        [Obsolete]
        public void createTopic(String key, string newTopic, int queueNum, int topicSysFlag)
        {
            this.defaultMQProducerImpl.createTopic(key, withNamespace(newTopic), queueNum, topicSysFlag);
        }

        /**
         * Search consume queue offset of the given time stamp.
         *
         * @param mq Instance of MessageQueue
         * @param timestamp from when in milliseconds.
         * @return Consume queue offset.
         * @throws MQClientException if there is any client error.
         */
        //@Override
        public long searchOffset(MessageQueue mq, long timestamp)
        {
            return this.defaultMQProducerImpl.searchOffset(queueWithNamespace(mq), timestamp);
        }

        /**
         * Query maximum offset of the given message queue.
         *
         * This method will be removed in a certain version after April 5, 2020, so please do not use this method.
         *
         * @param mq Instance of MessageQueue
         * @return maximum offset of the given consume queue.
         * @throws MQClientException if there is any client error.
         */
        //@Deprecated
        //@Override
        [Obsolete]
        public long maxOffset(MessageQueue mq)
        {
            return this.defaultMQProducerImpl.maxOffset(queueWithNamespace(mq));
        }

        /**
         * Query minimum offset of the given message queue.
         *
         * This method will be removed in a certain version after April 5, 2020, so please do not use this method.
         *
         * @param mq Instance of MessageQueue
         * @return minimum offset of the given message queue.
         * @throws MQClientException if there is any client error.
         */
        //@Deprecated
        //@Override
        [Obsolete]
        public long minOffset(MessageQueue mq)
        {
            return this.defaultMQProducerImpl.minOffset(queueWithNamespace(mq));
        }

        /**
         * Query earliest message store time.
         *
         * This method will be removed in a certain version after April 5, 2020, so please do not use this method.
         *
         * @param mq Instance of MessageQueue
         * @return earliest message store time.
         * @throws MQClientException if there is any client error.
         */
        //@Deprecated
        //@Override
        [Obsolete]
        public long earliestMsgStoreTime(MessageQueue mq)
        {
            return this.defaultMQProducerImpl.earliestMsgStoreTime(queueWithNamespace(mq));
        }

        /**
         * Query message of the given offset message ID.
         *
         * This method will be removed in a certain version after April 5, 2020, so please do not use this method.
         *
         * @param offsetMsgId message id
         * @return Message specified.
         * @throws MQBrokerException if there is any broker error.
         * @throws MQClientException if there is any client error.
         * @throws RemotingException if there is any network-tier error.
         * @throws InterruptedException if the sending thread is interrupted.
         */
        //@Deprecated
        //@Override
        [Obsolete]
        public MessageExt viewMessage(String offsetMsgId)
        {
            return this.defaultMQProducerImpl.viewMessage(offsetMsgId);
        }

        /**
         * Query message by key.
         *
         * This method will be removed in a certain version after April 5, 2020, so please do not use this method.
         *
         * @param topic message topic
         * @param key message key index word
         * @param maxNum max message number
         * @param begin from when
         * @param end to when
         * @return QueryResult instance contains matched messages.
         * @throws MQClientException if there is any client error.
         * @throws InterruptedException if the thread is interrupted.
         */
        //@Deprecated
        //@Override
        [Obsolete]
        public QueryResult queryMessage(String topic, string key, int maxNum, long begin, long end)
        {
            return this.defaultMQProducerImpl.queryMessage(withNamespace(topic), key, maxNum, begin, end);
        }

        /**
         * Query message of the given message ID.
         *
         * This method will be removed in a certain version after April 5, 2020, so please do not use this method.
         *
         * @param topic Topic
         * @param msgId Message ID
         * @return Message specified.
         * @throws MQBrokerException if there is any broker error.
         * @throws MQClientException if there is any client error.
         * @throws RemotingException if there is any network-tier error.
         * @throws InterruptedException if the sending thread is interrupted.
         */
        //@Deprecated
        //@Override
        [Obsolete]
        public MessageExt viewMessage(String topic, string msgId)
        {
            try
            {
                MessageId oldMsgId = MessageDecoder.decodeMessageId(msgId);
                return this.viewMessage(msgId);
            }
            catch (Exception e)
            {
            }
            return this.defaultMQProducerImpl.queryMessageByUniqKey(withNamespace(topic), msgId);
        }

        //@Override
        public SendResult send(
            ICollection<Message> msgs)
        {
            return this.defaultMQProducerImpl.send(batch(msgs));
        }

        //@Override
        public SendResult send(ICollection<Message> msgs,
            long timeout)
        {
            return this.defaultMQProducerImpl.send(batch(msgs), timeout);
        }

        //@Override
        public SendResult send(ICollection<Message> msgs,
            MessageQueue messageQueue)
        {
            return this.defaultMQProducerImpl.send(batch(msgs), messageQueue);
        }

        //@Override
        ///<exception cref = "MQClientException" />
        ///<exception cref = "RemotingException" />
        ///<exception cref = "MQBrokerException" />
        ///<exception cref = "ThreadInterruptedException" />
        public SendResult send(ICollection<Message> msgs, MessageQueue messageQueue,
            long timeout)
        {
            return this.defaultMQProducerImpl.send(batch(msgs), messageQueue, timeout);
        }

        //@Override
        /// <summary>
        /// 
        /// </summary>
        /// <param name="msgs"></param>
        /// <param name="sendCallback"></param>
        /// <exception cref="MQClientException">This exception is thrown if the archive already exists</exception>
        public void send(ICollection<Message> msgs, SendCallback sendCallback)
        {
            this.defaultMQProducerImpl.send(batch(msgs), sendCallback);
        }

        //@Override
        public void send(ICollection<Message> msgs, SendCallback sendCallback,
            long timeout)
        {
            this.defaultMQProducerImpl.send(batch(msgs), sendCallback, timeout);
        }

        //@Override
        public void send(ICollection<Message> msgs, MessageQueue mq,
            SendCallback sendCallback)
        {
            this.defaultMQProducerImpl.send(batch(msgs), queueWithNamespace(mq), sendCallback);
        }

        //@Override
        public void send(ICollection<Message> msgs, MessageQueue mq,
            SendCallback sendCallback, long timeout)
        {
            this.defaultMQProducerImpl.send(batch(msgs), queueWithNamespace(mq), sendCallback, timeout);
        }

        /**
         * Sets an Executor to be used for executing callback methods. If the Executor is not set, {@link
         * NettyRemotingClient#publicExecutor} will be used.
         *
         * @param callbackExecutor the instance of Executor
         */
        public void setCallbackExecutor(ExecutorService callbackExecutor)
        {
            this.defaultMQProducerImpl.setCallbackExecutor(callbackExecutor);
        }

        /**
         * Sets an Executor to be used for executing asynchronous send. If the Executor is not set, {@link
         * DefaultMQProducerImpl#defaultAsyncSenderExecutor} will be used.
         *
         * @param asyncSenderExecutor the instance of Executor
         */
        public void setAsyncSenderExecutor(ExecutorService asyncSenderExecutor)
        {
            this.defaultMQProducerImpl.setAsyncSenderExecutor(asyncSenderExecutor);
        }

        /**
         * Add response code for retrying.
         *
         * @param responseCode response code, {@link ResponseCode}
         */
        //public void addRetryResponseCode(int responseCode)
        //{
        //    this.retryResponseCodes.Add(responseCode);
        //}

        ///<exception cref="MQClientException"/>
        private MessageBatch batch(ICollection<Message> msgs)
        {
            MessageBatch msgBatch;
            try
            {
                msgBatch = MessageBatch.generateFromList(msgs);
                foreach (Message message in msgBatch)
                {
                    Validators.checkMessage(message, this);
                    MessageClientIDSetter.setUniqID(message);
                    message.setTopic(withNamespace(message.getTopic()));
                }
                msgBatch.setBody(msgBatch.encode());
            }
            catch (Exception e)
            {
                throw new MQClientException("Failed to initiate the MessageBatch", e);
            }
            msgBatch.setTopic(withNamespace(msgBatch.getTopic()));
            return msgBatch;
        }

        public string getProducerGroup()
        {
            return producerGroup;
        }

        public void setProducerGroup(String producerGroup)
        {
            this.producerGroup = producerGroup;
        }

        public string getCreateTopicKey()
        {
            return createTopicKey;
        }

        public void setCreateTopicKey(String createTopicKey)
        {
            this.createTopicKey = createTopicKey;
        }

        public int getSendMsgTimeout()
        {
            return sendMsgTimeout;
        }

        public void setSendMsgTimeout(int sendMsgTimeout)
        {
            this.sendMsgTimeout = sendMsgTimeout;
        }

        public int getCompressMsgBodyOverHowmuch()
        {
            return compressMsgBodyOverHowmuch;
        }

        public void setCompressMsgBodyOverHowmuch(int compressMsgBodyOverHowmuch)
        {
            this.compressMsgBodyOverHowmuch = compressMsgBodyOverHowmuch;
        }

        [Obsolete]//@Deprecated
        public DefaultMQProducerImpl getDefaultMQProducerImpl()
        {
            return defaultMQProducerImpl;
        }

        public bool isRetryAnotherBrokerWhenNotStoreOK()
        {
            return retryAnotherBrokerWhenNotStoreOK;
        }

        public void setRetryAnotherBrokerWhenNotStoreOK(bool retryAnotherBrokerWhenNotStoreOK)
        {
            this.retryAnotherBrokerWhenNotStoreOK = retryAnotherBrokerWhenNotStoreOK;
        }

        public int getMaxMessageSize()
        {
            return maxMessageSize;
        }

        public void setMaxMessageSize(int maxMessageSize)
        {
            this.maxMessageSize = maxMessageSize;
        }

        public int getDefaultTopicQueueNums()
        {
            return defaultTopicQueueNums;
        }

        public void setDefaultTopicQueueNums(int defaultTopicQueueNums)
        {
            this.defaultTopicQueueNums = defaultTopicQueueNums;
        }

        public int getRetryTimesWhenSendFailed()
        {
            return retryTimesWhenSendFailed;
        }

        public void setRetryTimesWhenSendFailed(int retryTimesWhenSendFailed)
        {
            this.retryTimesWhenSendFailed = retryTimesWhenSendFailed;
        }

        public bool isSendMessageWithVIPChannel()
        {
            return isVipChannelEnabled();
        }

        public void setSendMessageWithVIPChannel(bool sendMessageWithVIPChannel)
        {
            this.setVipChannelEnabled(sendMessageWithVIPChannel);
        }

        public long[] getNotAvailableDuration()
        {
            return this.defaultMQProducerImpl.getNotAvailableDuration();
        }

        public void setNotAvailableDuration(long[] notAvailableDuration)
        {
            this.defaultMQProducerImpl.setNotAvailableDuration(notAvailableDuration);
        }

        public long[] getLatencyMax()
        {
            return this.defaultMQProducerImpl.getLatencyMax();
        }

        public void setLatencyMax(long[] latencyMax)
        {
            this.defaultMQProducerImpl.setLatencyMax(latencyMax);
        }

        public bool isSendLatencyFaultEnable()
        {
            return this.defaultMQProducerImpl.isSendLatencyFaultEnable();
        }

        public void setSendLatencyFaultEnable(bool sendLatencyFaultEnable)
        {
            this.defaultMQProducerImpl.setSendLatencyFaultEnable(sendLatencyFaultEnable);
        }

        public int getRetryTimesWhenSendAsyncFailed()
        {
            return retryTimesWhenSendAsyncFailed;
        }

        public void setRetryTimesWhenSendAsyncFailed(int retryTimesWhenSendAsyncFailed)
        {
            this.retryTimesWhenSendAsyncFailed = retryTimesWhenSendAsyncFailed;
        }

        public TraceDispatcher getTraceDispatcher()
        {
            return traceDispatcher;
        }

        public HashSet<int> getRetryResponseCodes()
        {
            return retryResponseCodes;
        }

    }
}
