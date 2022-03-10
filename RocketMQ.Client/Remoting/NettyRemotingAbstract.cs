using DotNetty.Transport.Channels;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace RocketMQ.Client
{
    public abstract class NettyRemotingAbstract
    {
        /**
    * Remoting logger instance.
    */
        //private static readonly InternalLogger log = InternalLoggerFactory.getLogger(RemotingHelper.ROCKETMQ_REMOTING);
        static NLog.Logger log = NLog.LogManager.GetCurrentClassLogger();

        /**
         * Semaphore to limit maximum number of on-going one-way requests, which protects system memory footprint.
         */
        protected readonly SemaphoreSlim semaphoreOneway;

        /**
         * Semaphore to limit maximum number of on-going asynchronous requests, which protects system memory footprint.
         */
        protected readonly SemaphoreSlim semaphoreAsync;

        /**
         * This map caches all on-going requests.
         */
        //protected readonly ConcurrentDictionary<int /* opaque */, ResponseFuture> responseTable =
        //    new ConcurrentDictionary<int, ResponseFuture>(256);
        protected readonly ConcurrentDictionary<int /* opaque */, ResponseFuture> responseTable =
           new ConcurrentDictionary<int, ResponseFuture>();

        /**
         * This container holds all processors per request code, aka, for each incoming request, we may look up the
         * responding processor in this map to handle the request.
         */
        protected readonly Dictionary<int/* request code */, Pair<NettyRequestProcessor, ExecutorService>> processorTable =
            new Dictionary<int, Pair<NettyRequestProcessor, ExecutorService>>(64);

        /**
         * Executor to feed netty events to user defined {@link ChannelEventListener}.
         */
        protected readonly NettyEventExecutor nettyEventExecutor;

        /**
         * The default request processor to use in case there is no exact match in {@link #processorTable} per request code.
         */
        protected Pair<NettyRequestProcessor, ExecutorService> defaultRequestProcessor;

        /**
         * SSL context via which to create {@link SslHandler}.
         */
        protected volatile SslContext sslContext;

        /**
         * custom rpc hooks
         */
        protected List<RPCHook> rpcHooks = new List<RPCHook>();


        static NettyRemotingAbstract()
        {
            //NettyLogger.initNettyLogger();
        }

        public NettyRemotingAbstract()
        {
            nettyEventExecutor = new NettyEventExecutor(this);
        }

        /**
         * Constructor, specifying capacity of one-way and asynchronous semaphores.
         *
         * @param permitsOneway Number of permits for one-way requests.
         * @param permitsAsync Number of permits for asynchronous requests.
         */
        public NettyRemotingAbstract(int permitsOneway, int permitsAsync)
        {
            //this.semaphoreOneway = new Semaphore(permitsOneway, true);
            //this.semaphoreAsync = new Semaphore(permitsAsync, true);
            this.semaphoreOneway = new SemaphoreSlim(permitsOneway, permitsOneway);
            this.semaphoreAsync = new SemaphoreSlim(permitsAsync, permitsAsync);
        }

        /**
         * Custom channel event listener.
         *
         * @return custom channel event listener if defined; null otherwise.
         */
        public abstract ChannelEventListener getChannelEventListener();

        /**
         * Put a netty event to the executor.
         *
         * @param event Netty event instance.
         */
        public void putNettyEvent(NettyEvent evt)
        {
            this.nettyEventExecutor.putNettyEvent(evt);
        }

        /**
         * Entry of incoming command processing.
         *
         * <p>
         * <strong>Note:</strong>
         * The incoming remoting command may be
         * <ul>
         * <li>An inquiry request from a remote peer component;</li>
         * <li>A response to a previous request issued by this very participant.</li>
         * </ul>
         * </p>
         *
         * @param ctx Channel handler context.
         * @param msg incoming remoting command.
         * @throws Exception if there were any error while processing the incoming command.
         */
        ///<exception cref="Exception"/>
        public void processMessageReceived(IChannelHandlerContext ctx, RemotingCommand msg)
        {
            RemotingCommand cmd = msg;
            if (cmd != null)
            {
                switch (cmd.getType())
                {
                    case RemotingCommandType.REQUEST_COMMAND:
                        processRequestCommand(ctx, cmd);
                        break;
                    case RemotingCommandType.RESPONSE_COMMAND:
                        processResponseCommand(ctx, cmd);
                        break;
                    default:
                        break;
                }
            }
        }

        protected void doBeforeRpcHooks(String addr, RemotingCommand request)
        {
            if (rpcHooks.Count > 0)
            {
                foreach (RPCHook rpcHook in rpcHooks)
                {
                    rpcHook.doBeforeRequest(addr, request);
                }
            }
        }

        protected void doAfterRpcHooks(String addr, RemotingCommand request, RemotingCommand response)
        {
            if (rpcHooks.Count > 0)
            {
                foreach (RPCHook rpcHook in rpcHooks)
                {
                    rpcHook.doAfterResponse(addr, request, response);
                }
            }
        }


        /**
         * Process incoming request command issued by remote peer.
         *
         * @param ctx channel handler context.
         * @param cmd request command.
         */
        public void processRequestCommand(IChannelHandlerContext ctx, RemotingCommand cmd)
        {
            Pair<NettyRequestProcessor, ExecutorService> matched = this.processorTable.get(cmd.getCode());
            Pair<NettyRequestProcessor, ExecutorService> pair = null == matched ? this.defaultRequestProcessor : matched;
            int opaque = cmd.getOpaque();

            if (pair != null)
            {
                Runnable run = new Runnable()
                {
                    //public void run()
                    Run = () =>
                {
                    try
                    {
                        String remoteAddr = RemotingHelper.parseChannelRemoteAddr(ctx.Channel);
                        doBeforeRpcHooks(remoteAddr, cmd);
                        RemotingResponseCallback callback = new RemotingResponseCallback()
                        {
                        //@Override
                        //public void callback(RemotingCommand response)
                        Callback = (response) =>
                {
                            doAfterRpcHooks(remoteAddr, cmd, response);
                            if (!cmd.isOnewayRPC())
                            {
                                if (response != null)
                                {
                                    response.setOpaque(opaque);
                                    response.markResponseType();
                                    try
                                    {
                                    //ctx.writeAndFlush(response);
                                    ctx.WriteAndFlushAsync(response);
                                    }
                                    catch (Exception e)
                                    {
                                        log.Error("process request over, but response failed", e.ToString());
                                        log.Error(cmd.ToString());
                                        log.Error(response.ToString());
                                    }
                                }
                                else
                                {
                                }
                            }
                        }
                        };
                        if (pair.getObject1() is AsyncNettyRequestProcessor)
                        {
                            AsyncNettyRequestProcessor processor = (AsyncNettyRequestProcessor)pair.getObject1();
                            processor.asyncProcessRequest(ctx, cmd, callback);
                        }
                        else
                        {
                            NettyRequestProcessor processor = pair.getObject1();
                            RemotingCommand response = processor.processRequest(ctx, cmd);
                            callback.Callback(response);
                        }
                    }
                    catch (Exception e)
                    {
                        log.Error("process request exception", e.ToString());
                        log.Error(cmd.ToString());

                        if (!cmd.isOnewayRPC())
                        {
                            RemotingCommand response = RemotingCommand.createResponseCommand(RemotingSysResponseCode.SYSTEM_ERROR,
                            RemotingHelper.exceptionSimpleDesc(e));
                            response.setOpaque(opaque);
                        //ctx.writeAndFlush(response);
                        ctx.WriteAndFlushAsync(response);
                        }
                    }
                }
                };

                if (pair.getObject1().rejectRequest())
                {
                    RemotingCommand response = RemotingCommand.createResponseCommand(RemotingSysResponseCode.SYSTEM_BUSY,
                        "[REJECTREQUEST]system busy, start flow control for a while");
                    response.setOpaque(opaque);
                    //ctx.writeAndFlush(response);
                    ctx.WriteAndFlushAsync(response);
                    return;
                }

                try
                {
                    RequestTask requestTask = new RequestTask(run, ctx.Channel, cmd);
                    pair.getObject2().Submit(requestTask);
                }
                catch (Exception e)
                {
                    if ((Sys.currentTimeMillis() % 10000) == 0)
                    {
                        log.Warn(RemotingHelper.parseChannelRemoteAddr(ctx.Channel)
                            + ", too many requests and system thread pool busy, RejectedExecutionException "
                            + pair.getObject2().ToString()
                            + " request code: " + cmd.getCode());
                    }

                    if (!cmd.isOnewayRPC())
                    {
                        RemotingCommand response = RemotingCommand.createResponseCommand(RemotingSysResponseCode.SYSTEM_BUSY,
                            "[OVERLOAD]system busy, start flow control for a while");
                        response.setOpaque(opaque);
                        //ctx.writeAndFlush(response);
                        ctx.WriteAndFlushAsync(response);
                    }
                }
            }
            else
            {
                String error = " request type " + cmd.getCode() + " not supported";
                RemotingCommand response =
                    RemotingCommand.createResponseCommand(RemotingSysResponseCode.REQUEST_CODE_NOT_SUPPORTED, error);
                response.setOpaque(opaque);
                //ctx.writeAndFlush(response);
                ctx.WriteAndFlushAsync(response);
                log.Error(RemotingHelper.parseChannelRemoteAddr(ctx.Channel) + error);
            }
        }

        /**
         * Process response from remote peer to the previous issued requests.
         *
         * @param ctx channel handler context.
         * @param cmd response command instance.
         */
        public void processResponseCommand(IChannelHandlerContext ctx, RemotingCommand cmd)
        {
            int opaque = cmd.getOpaque();
            ResponseFuture responseFuture = responseTable.get(opaque);
            if (responseFuture != null)
            {
                responseFuture.setResponseCommand(cmd);

                responseTable.remove(opaque);

                if (responseFuture.getInvokeCallback() != null)
                {
                    executeInvokeCallback(responseFuture);
                }
                else
                {
                    responseFuture.putResponse(cmd);
                    responseFuture.release();
                }
            }
            else
            {
                log.Warn("receive response, but not matched any request, " + RemotingHelper.parseChannelRemoteAddr(ctx.Channel));
                log.Warn(cmd.ToString());
            }
        }

        /**
         * Execute callback in callback executor. If callback executor is null, run directly in current thread
         */
        private void executeInvokeCallback(ResponseFuture responseFuture)
        {
            bool runInThisThread = false;
            ExecutorService executor = this.getCallbackExecutor();
            if (executor != null)
            {
                try
                {
                    executor.Submit(new Runnable()
                    {
                        //public void run()
                        Run = () =>
                {
                    try
                    {
                        responseFuture.executeInvokeCallback();
                    }
                    catch (Exception e)
                    {
                        log.Warn("execute callback in executor exception, and callback throw", e.ToString());
                    }
                    finally
                    {
                        responseFuture.release();
                    }
                }
                    });
                }
                catch (Exception e)
                {
                    runInThisThread = true;
                    log.Warn("execute callback in executor exception, maybe executor busy", e.ToString());
                }
            }
            else
            {
                runInThisThread = true;
            }

            if (runInThisThread)
            {
                try
                {
                    responseFuture.executeInvokeCallback();
                }
                catch (Exception e)
                {
                    log.Warn("executeInvokeCallback Exception", e.ToString());
                }
                finally
                {
                    responseFuture.release();
                }
            }
        }



        /**
         * Custom RPC hook.
         * Just be compatible with the previous version, use getRPCHooks instead.
         */
        [Obsolete]//@Deprecated
        protected RPCHook getRPCHook()
        {
            if (rpcHooks.Count > 0)
            {
                return rpcHooks.get(0);
            }
            return null;
        }

        /**
         * Custom RPC hooks.
         *
         * @return RPC hooks if specified; null otherwise.
         */
        public List<RPCHook> getRPCHooks()
        {
            return rpcHooks;
        }


        /**
         * This method specifies thread pool to use while invoking callback methods.
         *
         * @return Dedicated thread pool instance if specified; or null if the callback is supposed to be executed in the
         * netty event-loop thread.
         */
        public abstract ExecutorService getCallbackExecutor();

        /**
         * <p>
         * This method is periodically invoked to scan and expire deprecated request.
         * </p>
         */
        public void scanResponseTable()
        {
            LinkedList<ResponseFuture> rfList = new LinkedList<ResponseFuture>();
            //Iterator<Entry<Integer, ResponseFuture>> it = this.responseTable.entrySet().iterator();
            //while (it.hasNext())
            foreach (var entry in this.responseTable)
            {
                //Entry<Integer, ResponseFuture> next = it.next();
                ResponseFuture rep = entry.Value;
                if ((rep.getBeginTimestamp() + rep.getTimeoutMillis() + 1000) <= Sys.currentTimeMillis())
                {
                    rep.release();
                    //it.remove();
                    responseTable.TryRemove(entry.Key, out _);
                    rfList.AddLast(rep);
                    log.Warn("remove timeout request, " + rep);
                }
            }

            foreach (ResponseFuture rf in rfList)
            {
                try
                {
                    executeInvokeCallback(rf);
                }
                catch (Exception e)
                {
                    log.Warn("scanResponseTable, operationComplete Exception", e.ToString());
                }
            }
        }

        ///<exception cref="InterruptedException"/>
        ///<exception cref="RemotingSendRequestException"/>
        ///<exception cref="RemotingTimeoutException"/>
        public RemotingCommand invokeSyncImpl(IChannel channel, RemotingCommand request, long timeoutMillis)
        {
            int opaque = request.getOpaque();

            try
            {
                ResponseFuture responseFuture = new ResponseFuture(channel, opaque, timeoutMillis, null);
                this.responseTable.put(opaque, responseFuture);
                EndPoint addr = channel.RemoteAddress;


                var task = channel.WriteAndFlushAsync(request);
                task.Wait();//await task;
                if (task.IsCompletedSuccessfully)
                {
                    responseFuture.setSendRequestOK(true);
                }
                else
                {
                    responseFuture.setSendRequestOK(false);
                    responseTable.remove(opaque);
                    //responseFuture.setCause(f.cause());
                    responseFuture.setCause(task.Exception);
                    responseFuture.putResponse(null);
                    log.Warn("send a request command to channel <" + addr + "> failed.");
                }
                //ok
                //channel.writeAndFlush(request).addListener(new ChannelFutureListener() {
                //            //@Override
                //            public void operationComplete(ChannelFuture f) {
                //    if (f.isSuccess())
                //    {
                //        responseFuture.setSendRequestOK(true);
                //        return;
                //    }
                //    else
                //    {
                //        responseFuture.setSendRequestOK(false);
                //    }
                //}
                //});

                RemotingCommand responseCommand = responseFuture.waitResponse(timeoutMillis).Result;
                if (null == responseCommand)
                {
                    if (responseFuture.isSendRequestOK())
                    {
                        throw new RemotingTimeoutException(RemotingHelper.parseSocketAddressAddr(addr), timeoutMillis, responseFuture.getCause());
                    }
                    else
                    {
                        throw new RemotingSendRequestException(RemotingHelper.parseSocketAddressAddr(addr), responseFuture.getCause());
                    }
                }
                return responseCommand;
            }
            finally
            {
                this.responseTable.remove(opaque);
            }
        }

        ///<exception cref="InterruptedException"/>
        ///<exception cref="RemotingSendRequestException"/>
        ///<exception cref="RemotingTimeoutException"/>
        ///<exception cref="RemotingTooMuchRequestException"/>

        public async void invokeAsyncImpl(IChannel channel, RemotingCommand request, long timeoutMillis, InvokeCallback invokeCallback)
        {
            long beginStartTime = Sys.currentTimeMillis();
            int opaque = request.getOpaque();

            bool acquired = await semaphoreAsync.WaitAsync(TimeSpan.FromMilliseconds(timeoutMillis));
            //bool acquired = semaphoreAsync.WaitOne(TimeSpan.FromMilliseconds(timeoutMillis));
            //bool acquired = this.semaphoreAsync.tryAcquire(timeoutMillis, TimeUnit.MILLISECONDS);
            if (acquired)
            {
                //SemaphoreReleaseOnlyOnce once = new SemaphoreReleaseOnlyOnce(this.semaphoreAsync);
                long costTime = Sys.currentTimeMillis() - beginStartTime;
                if (timeoutMillis < costTime)
                {
                    //once.release();
                    semaphoreAsync.Release();
                    throw new RemotingTimeoutException("invokeAsyncImpl call timeout");
                }

                ResponseFuture responseFuture = new ResponseFuture(channel, opaque, timeoutMillis - costTime, invokeCallback);
                this.responseTable.put(opaque, responseFuture);
                try
                {
                    var task = channel.WriteAndFlushAsync(request);
                    await task;
                    if (task.IsCompletedSuccessfully)
                    {
                        responseFuture.setSendRequestOK(true);
                    }
                    else
                    {
                        requestFail(opaque);
                        log.Warn("send a request command to channel <{}> failed.", RemotingHelper.parseChannelRemoteAddr(channel));
                    }
                    //    channel.writeAndFlush(request).addListener(new ChannelFutureListener() {
                    //                @Override
                    //                public void operationComplete(ChannelFuture f) throws Exception {
                    //        if (f.isSuccess())
                    //        {
                    //            responseFuture.setSendRequestOK(true);
                    //            return;
                    //        }
                    //        requestFail(opaque);
                    //        log.warn("send a request command to channel <{}> failed.", RemotingHelper.parseChannelRemoteAddr(channel));
                    //    }
                    //});
                }
                catch (Exception e)
                {
                    responseFuture.release();
                    log.Warn("send a request command to channel <" + RemotingHelper.parseChannelRemoteAddr(channel) + "> Exception", e.ToString());
                    throw new RemotingSendRequestException(RemotingHelper.parseChannelRemoteAddr(channel), e);
                }
            }
            else
            {
                if (timeoutMillis <= 0)
                {
                    throw new RemotingTooMuchRequestException("invokeAsyncImpl invoke too fast");
                }
                else
                {
                    //semaphoreAsync.CurrentCount
                    //String info =
                    //    String.Format($"invokeAsyncImpl tryAcquire semaphore timeout, %dms, waiting thread nums: %d semaphoreAsyncValue: %d",
                    //        timeoutMillis,
                    //        this.semaphoreAsync.getQueueLength(),
                    //        this.semaphoreAsync.availablePermits()
                    //    );
                    string info = $"invokeAsyncImpl tryAcquire semaphore timeout, {timeoutMillis}ms, CurrentCount: {semaphoreAsync.CurrentCount}";
                    log.Warn(info);
                    throw new RemotingTimeoutException(info);
                }
            }
        }

        private void requestFail(int opaque)
        {
            ResponseFuture responseFuture = responseTable.remove(opaque);
            if (responseFuture != null)
            {
                responseFuture.setSendRequestOK(false);
                responseFuture.putResponse(null);
                try
                {
                    executeInvokeCallback(responseFuture);
                }
                catch (Exception e)
                {
                    log.Warn("execute callback in requestFail, and callback throw", e.ToString());
                }
                finally
                {
                    responseFuture.release();
                }
            }
        }

        /**
         * mark the request of the specified channel as fail and to invoke fail callback immediately
         * @param channel the channel which is close already
         */
        protected void failFast(IChannel channel)
        {
            //Iterator<Entry<Integer, ResponseFuture>> it = responseTable.entrySet().iterator();
            //while (it.hasNext())
            foreach (var entry in responseTable)
            {
                //Entry<Integer, ResponseFuture> entry = it.next();
                if (entry.Value.getProcessChannel() == channel)
                {
                    int opaque = entry.Key;
                    if (opaque != 0) //??? if (opaque != null)
                    {
                        requestFail(opaque);
                    }
                }
            }
        }

        ///<exception cref="InterruptedException"/>
        ///<exception cref="RemotingTooMuchRequestException"/>
        ///<exception cref="RemotingTimeoutException"/>
        ///<exception cref="RemotingSendRequestException"/>
        public async Task invokeOnewayImpl(IChannel channel, RemotingCommand request, long timeoutMillis)
        {
            request.markOnewayRPC();
            //bool acquired = this.semaphoreOneway.tryAcquire(timeoutMillis, TimeUnit.MILLISECONDS);
            bool acquired = await semaphoreOneway.WaitAsync(TimeSpan.FromMilliseconds(timeoutMillis));
            if (acquired)
            {
                //SemaphoreReleaseOnlyOnce once = new SemaphoreReleaseOnlyOnce(this.semaphoreOneway);
                try
                {
                    var task = channel.WriteAndFlushAsync(request);
                    await task;
                    semaphoreOneway.Release();
                    if (!task.IsCompletedSuccessfully)
                    {
                        log.Warn("send a request command to channel <" + channel.RemoteAddress + "> failed.");
                    }
                    //    channel.writeAndFlush(request).addListener(new ChannelFutureListener() {
                    //                    @Override
                    //                    public void operationComplete(ChannelFuture f) throws Exception {
                    //        once.release();
                    //        if (!f.isSuccess())
                    //        {
                    //            log.warn("send a request command to channel <" + channel.remoteAddress() + "> failed.");
                    //        }
                    //    }
                    //});
                }
                catch (Exception e)
                {
                    semaphoreOneway.Release();
                    log.Warn("write send a request command to channel <" + channel.RemoteAddress + "> failed.");
                    throw new RemotingSendRequestException(RemotingHelper.parseChannelRemoteAddr(channel), e);
                }
            }
            else
            {
                if (timeoutMillis <= 0)
                {
                    throw new RemotingTooMuchRequestException("invokeOnewayImpl invoke too fast");
                }
                else
                {
                    //String info = String.format(
                    //    "invokeOnewayImpl tryAcquire semaphore timeout, %dms, waiting thread nums: %d semaphoreOnewayValue: %d",
                    //    timeoutMillis,
                    //    this.semaphoreOneway.getQueueLength(),
                    //    this.semaphoreOneway.availablePermits()
                    //);
                    string info = $"invokeOnewayImpl tryAcquire semaphore timeout, {timeoutMillis}ms, CurrentCount: {semaphoreAsync.CurrentCount}";
                    log.Warn(info);
                    throw new RemotingTimeoutException(info);
                }
            }
        }

        protected class NettyEventExecutor : ServiceThread
        {

            private NettyRemotingAbstract owner;
            //private LinkedBlockingQueue<NettyEvent> eventQueue = new LinkedBlockingQueue<NettyEvent>();
            private const int maxSize = 10000;

            protected override Model ExecuteModel => Model.OnDemand;

            public NettyEventExecutor(NettyRemotingAbstract owner)
                : base(maxSize)
            {
                this.owner = owner;
            }

            public void putNettyEvent(NettyEvent evt)
            {
                int currentSize = GetWaitingCount();
                if (currentSize <= maxSize)
                {
                    //this.eventQueue.add(evt);
                    Executor.SendAsync(() => { Work(evt); });
                }
                else
                {
                    log.Warn("event queue size [{}] over the limit [{}], so drop this event {}", currentSize, maxSize, evt.ToString());
                }
            }

            public Task Work(NettyEvent evt)
            {
                ChannelEventListener listener = owner.getChannelEventListener();
                try
                {
                    if (evt != null && listener != null)
                    {
                        switch (evt.getType())
                        {
                            case NettyEventType.IDLE:
                                listener.onChannelIdle(evt.getRemoteAddr(), evt.getChannel());
                                break;
                            case NettyEventType.CLOSE:
                                listener.onChannelClose(evt.getRemoteAddr(), evt.getChannel());
                                break;
                            case NettyEventType.CONNECT:
                                listener.onChannelConnect(evt.getRemoteAddr(), evt.getChannel());
                                break;
                            case NettyEventType.EXCEPTION:
                                listener.onChannelException(evt.getRemoteAddr(), evt.getChannel());
                                break;
                            default:
                                break;

                        }
                    }
                }
                catch (Exception e)
                {
                    log.Warn(this.getServiceName() + " service has exception. ", e.ToString());
                }
                return Task.CompletedTask;
            }

            public override string getServiceName()
            {
                return typeof(NettyEventExecutor).Name;
                //return NettyEventExecutor.class.getSimpleName();
            }

            public override void run()
            {
                throw new NotImplementedException();
            }
        }
    }
}
