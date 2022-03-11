using DotNetty.Common.Concurrency;
using DotNetty.Common.Utilities;
using DotNetty.Handlers.Timeout;
using DotNetty.Transport.Bootstrapping;
using DotNetty.Transport.Channels;
using DotNetty.Transport.Channels.Sockets;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace RocketMQ.Client
{
    public class NettyRemotingClient : NettyRemotingAbstract, RemotingClient
    {
        //private static readonly InternalLogger log = InternalLoggerFactory.getLogger(RemotingHelper.ROCKETMQ_REMOTING);
        static NLog.Logger log = NLog.LogManager.GetCurrentClassLogger();

        private static readonly int LOCK_TIMEOUT_MILLIS = 3000;

        private readonly NettyClientConfig nettyClientConfig;
        private readonly Bootstrap bootstrap = new Bootstrap();
        private readonly IEventLoopGroup eventLoopGroupWorker;
        //private readonly Lock lockChannelTables = new ReentrantLock();
        private readonly object lockChannelTables = new object();
        private readonly ConcurrentDictionary<string /* addr */, ChannelWrapper> channelTables = new ConcurrentDictionary<string, ChannelWrapper>();

        //private readonly Timer timer = new Timer("ClientHouseKeepingService", true);
        private readonly ScheduledExecutorService timer = new ScheduledExecutorService(1);

        private readonly AtomicReference<List<string>> namesrvAddrList = new AtomicReference<List<string>>();
        private readonly AtomicReference<string> namesrvAddrChoosed = new AtomicReference<string>();
        private readonly AtomicInteger namesrvIndex = new AtomicInteger(initValueIndex());
        //private readonly Lock namesrvChannelLock = new ReentrantLock();
        private readonly object namesrvChannelLock = new object();

        private readonly ExecutorService publicExecutor;

        /**
         * Invoke the callback methods in this executor when process response.
         */
        private ExecutorService callbackExecutor;
        private readonly ChannelEventListener channelEventListener;
        //private DefaultEventExecutorGroup defaultEventExecutorGroup;
        private IEventExecutorGroup defaultEventExecutorGroup;

        public NettyRemotingClient(NettyClientConfig nettyClientConfig) 
            : this(nettyClientConfig, null)
        {
            
        }

        private AtomicInteger threadIndex = new AtomicInteger(0);
        public NettyRemotingClient(NettyClientConfig nettyClientConfig, ChannelEventListener channelEventListener)
             : base(nettyClientConfig.getClientOnewaySemaphoreValue(), nettyClientConfig.getClientAsyncSemaphoreValue())
        {
            this.nettyClientConfig = nettyClientConfig;
            this.channelEventListener = channelEventListener;

            int publicThreadNums = nettyClientConfig.getClientCallbackExecutorThreads();
            if (publicThreadNums <= 0)
            {
                publicThreadNums = 4;
            }

            //this.publicExecutor = Executors.newFixedThreadPool(publicThreadNums, new ThreadFactory()
            //{
            //    private AtomicInteger threadIndex = new AtomicInteger(0);
            //    //@Override
            //    public Thread newThread(Runnable r)
            //    {
            //        return new Thread(r, "NettyClientPublicExecutor_" + this.threadIndex.incrementAndGet());
            //    }
            //});
            publicExecutor = new ExecutorService(publicThreadNums);

            //this.eventLoopGroupWorker = new NioEventLoopGroup(1, new ThreadFactory()
            //{
            //    private AtomicInteger threadIndex = new AtomicInteger(0);
            //    //@Override
            //    public Thread newThread(Runnable r)
            //    {
            //        return new Thread(r, String.format("NettyClientSelector_%d", this.threadIndex.incrementAndGet()));
            //    }
            //});
            eventLoopGroupWorker = new SingleThreadEventLoop(String.Format("NettyClientSelector_%d", this.threadIndex.incrementAndGet()));

            if (nettyClientConfig.isUseTLS())
            {
                try
                {
                    //sslContext = TlsHelper.buildSslContext(true);
                    sslContext = new SslContext();
                    log.Info("SSL enabled for client");
                }
                catch (IOException e)
                {
                    log.Error("Failed to create SSLContext", e);
                }
                catch (Exception e)
                {
                    log.Error("Failed to create SSLContext", e.ToString());
                    throw new RuntimeException("Failed to create SSLContext", e);
                }
            }
        }

        private static int initValueIndex()
        {
            Random r = new Random();

            return Math.Abs(r.nextInt() % 999) % 999;
        }

        //@Override
        public void start()
        {
            //    this.defaultEventExecutorGroup = new DefaultEventExecutorGroup(
            //        nettyClientConfig.getClientWorkerThreads(),
            //        new ThreadFactory()
            //        {

            //                private AtomicInteger threadIndex = new AtomicInteger(0);

            //@Override
            //                public Thread newThread(Runnable r)
            //{
            //    return new Thread(r, "NettyClientWorkerThread_" + this.threadIndex.incrementAndGet());
            //}
            //            });
            this.defaultEventExecutorGroup = new MultithreadEventLoopGroup(nettyClientConfig.getClientWorkerThreads());

            Bootstrap handler = this.bootstrap.Group(this.eventLoopGroupWorker).Channel<TcpSocketChannel>()
            .Option(ChannelOption.TcpNodelay, true)
            .Option(ChannelOption.SoKeepalive, false)
            .Option(ChannelOption.ConnectTimeout, TimeSpan.FromMilliseconds(nettyClientConfig.getConnectTimeoutMillis()))
            .Handler(new ActionChannelInitializer<ISocketChannel>((channel) =>
            {
                IChannelPipeline pipeline = channel.Pipeline;
                if (nettyClientConfig.isUseTLS())
                {
                    if (null != sslContext)
                    {
                        //channel.Allocator
                        pipeline.AddFirst(defaultEventExecutorGroup, "sslHandler", sslContext.newHandler());
                        log.Info("Prepend SSL handler");
                    }
                    else
                    {
                        log.Warn("Connections are insecure as SSLContext is null!");
                    }
                }
                pipeline.AddLast(
                                    defaultEventExecutorGroup,
                                    new NettyEncoder(),
                                    new NettyDecoder(),
                                    new IdleStateHandler(0, 0, nettyClientConfig.getClientChannelMaxIdleTimeSeconds()),
                                    new NettyConnectManageHandler(this),
                                    new NettyClientHandler(this));
            }));
            if (nettyClientConfig.getClientSocketSndBufSize() > 0)
            {
                log.Info("client set SO_SNDBUF to {}", nettyClientConfig.getClientSocketSndBufSize());
                handler.Option(ChannelOption.SoSndbuf, nettyClientConfig.getClientSocketSndBufSize());
            }
            if (nettyClientConfig.getClientSocketRcvBufSize() > 0)
            {
                log.Info("client set SO_RCVBUF to {}", nettyClientConfig.getClientSocketRcvBufSize());
                handler.Option(ChannelOption.SoRcvbuf, nettyClientConfig.getClientSocketRcvBufSize());
            }
            if (nettyClientConfig.getWriteBufferLowWaterMark() > 0 && nettyClientConfig.getWriteBufferHighWaterMark() > 0)
            {
                log.Info("client set netty WRITE_BUFFER_WATER_MARK to {},{}",
                        nettyClientConfig.getWriteBufferLowWaterMark(), nettyClientConfig.getWriteBufferHighWaterMark());
                //handler.Option(ChannelOption.WRITE_BUFFER_WATER_MARK, new WriteBufferWaterMark(
                //        nettyClientConfig.getWriteBufferLowWaterMark(), nettyClientConfig.getWriteBufferHighWaterMark()));
                handler.Option(ChannelOption.WriteBufferHighWaterMark, nettyClientConfig.getWriteBufferHighWaterMark());
                handler.Option(ChannelOption.WriteBufferLowWaterMark, nettyClientConfig.getWriteBufferLowWaterMark());
            }

            this.timer.ScheduleAtFixedRate(new Runnable()
            {
                //public void run()
                Run = () =>
                {
                    try
                    {
                        /*NettyRemotingClient.this.*/
                        scanResponseTable();
                    }
                    catch (Exception e)
                    {
                        log.Error("scanResponseTable exception", e.ToString());
                    }
                }
            }, 1000 * 3, 1000);

            if (this.channelEventListener != null)
            {
                this.nettyEventExecutor.start();
            }
        }

        //@Override
        public void shutdown()
        {
            try
            {
                //this.timer.cancel();
                this.timer.Shutdown();

                foreach (ChannelWrapper cw in this.channelTables.Values)
                {
                    this.closeChannel(null, cw.getChannel());
                }

                this.channelTables.Clear();
                this.eventLoopGroupWorker.ShutdownGracefullyAsync();

                if (this.nettyEventExecutor != null)
                {
                    this.nettyEventExecutor.shutdown();
                }

                if (this.defaultEventExecutorGroup != null)
                {
                    this.defaultEventExecutorGroup.ShutdownGracefullyAsync();
                }
            }
            catch (Exception e)
            {
                log.Error("NettyRemotingClient shutdown exception, ", e.ToString());
            }

            if (this.publicExecutor != null)
            {
                try
                {
                    this.publicExecutor.Shutdown();
                }
                catch (Exception e)
                {
                    log.Error("NettyRemotingServer shutdown exception, ", e.ToString());
                }
            }
        }

        public void closeChannel(String addr, IChannel channel)
        {
            if (null == channel)
                return;

            string addrRemote = null == addr ? RemotingHelper.parseChannelRemoteAddr(channel) : addr;

            try
            {
                if(Monitor.TryEnter(lockChannelTables, LOCK_TIMEOUT_MILLIS))
                //if (this.lockChannelTables.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS))
                {
                    try
                    {
                        bool removeItemFromTable = true;
                        ChannelWrapper prevCW = this.channelTables.Get(addrRemote);

                        log.Info("closeChannel: begin close the channel[{}] Found: {}", addrRemote, prevCW != null);

                        if (null == prevCW)
                        {
                            log.Info("closeChannel: the channel[{}] has been removed from the channel table before", addrRemote);
                            removeItemFromTable = false;
                        }
                        else if (prevCW.getChannel() != channel)
                        {
                            log.Info("closeChannel: the channel[{}] has been closed before, and has been created again, nothing to do.",
                                addrRemote);
                            removeItemFromTable = false;
                        }

                        if (removeItemFromTable)
                        {
                            this.channelTables.JavaRemove(addrRemote);
                            log.Info("closeChannel: the channel[{}] was removed from channel table", addrRemote);
                        }

                        RemotingUtil.closeChannel(channel);
                    }
                    catch (Exception e)
                    {
                        log.Error("closeChannel: close the channel exception", e.ToString());
                    }
                    finally
                    {
                        //this.lockChannelTables.unlock();
                        Monitor.Exit(lockChannelTables);
                    }
                }
                else
                {
                    log.Warn("closeChannel: try to lock channel table, but timeout, {}ms", LOCK_TIMEOUT_MILLIS);
                }
            }
            catch (ThreadInterruptedException e)
            {
                log.Error("closeChannel exception", e);
            }
        }

        //@Override
        public void registerRPCHook(RPCHook rpcHook)
        {
            if (rpcHook != null && !rpcHooks.Contains(rpcHook))
            {
                rpcHooks.Add(rpcHook);
            }
        }

        public void closeChannel(IChannel channel)
        {
            if (null == channel)
                return;

            try
            {
                if(Monitor.TryEnter(lockChannelTables, LOCK_TIMEOUT_MILLIS))
                //if (this.lockChannelTables.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS))
                {
                    try
                    {
                        bool removeItemFromTable = true;
                        ChannelWrapper prevCW = null;
                        string addrRemote = null;
                        foreach (var entry in channelTables)
                        {
                            string key = entry.Key;
                            ChannelWrapper prev = entry.Value;
                            if (prev.getChannel() != null)
                            {
                                if (prev.getChannel() == channel)
                                {
                                    prevCW = prev;
                                    addrRemote = key;
                                    break;
                                }
                            }
                        }

                        if (null == prevCW)
                        {
                            log.Info("eventCloseChannel: the channel[{}] has been removed from the channel table before", addrRemote);
                            removeItemFromTable = false;
                        }

                        if (removeItemFromTable)
                        {
                            this.channelTables.JavaRemove(addrRemote);
                            log.Info("closeChannel: the channel[{}] was removed from channel table", addrRemote);
                            RemotingUtil.closeChannel(channel);
                        }
                    }
                    catch (Exception e)
                    {
                        log.Error("closeChannel: close the channel exception", e.ToString());
                    }
                    finally
                    {
                        //this.lockChannelTables.unlock();
                        Monitor.Exit(lockChannelTables);
                    }
                }
                else
                {
                    log.Warn("closeChannel: try to lock channel table, but timeout, {}ms", LOCK_TIMEOUT_MILLIS);
                }
            }
            catch (ThreadInterruptedException e)
            {
                log.Error("closeChannel exception", e);
            }
        }

        //@Override
        public void updateNameServerAddressList(List<String> addrs)
        {
            //List<String> old = this.namesrvAddrList.get();
            List<String> old = this.namesrvAddrList.Value;
            bool update = false;

            if (!addrs.IsEmpty())
            {
                if (null == old)
                {
                    update = true;
                }
                else if (addrs.Count != old.Count)
                {
                    update = true;
                }
                else
                {
                    for (int i = 0; i < addrs.Count && !update; i++)
                    {
                        if (!old.Contains(addrs.Get(i)))
                        {
                            update = true;
                        }
                    }
                }

                if (update)
                {
                    //Collections.shuffle(addrs);
                    addrs = addrs.Shuffle();  //???
                    log.Info("name server address updated. NEW : {} , OLD: {}", addrs, old);
                    //this.namesrvAddrList.set(addrs);
                    namesrvAddrList.Value = addrs;

                    if (!addrs.Contains(this.namesrvAddrChoosed.Value))
                    {
                        this.namesrvAddrChoosed.Value = null;
                    }
                }
            }
        }

        //@Override
        ///<exception cref="InterruptedException"/>
        ///<exception cref="RemotingConnectException"/>
        ///<exception cref="RemotingSendRequestException"/>
        ///<exception cref="RemotingTimeoutException"/>
        public RemotingCommand invokeSync(String addr, RemotingCommand request, long timeoutMillis)
        {
            long beginStartTime = Sys.currentTimeMillis();
            IChannel channel = getAndCreateChannel(addr).Result;
            if (channel != null && channel.Active)
            {
                try
                {
                    doBeforeRpcHooks(addr, request);
                    long costTime = Sys.currentTimeMillis() - beginStartTime;
                    if (timeoutMillis < costTime)
                    {
                        throw new RemotingTimeoutException("invokeSync call the addr[" + addr + "] timeout");
                    }
                    RemotingCommand response = invokeSyncImpl(channel, request, timeoutMillis - costTime);
                    doAfterRpcHooks(RemotingHelper.parseChannelRemoteAddr(channel), request, response);
                    return response;
                }
                catch (RemotingSendRequestException e)
                {
                    log.Warn("invokeSync: send request exception, so close the channel[{}]", addr);
                    this.closeChannel(addr, channel);
                    throw e;
                }
                catch (RemotingTimeoutException e)
                {
                    if (nettyClientConfig.isClientCloseSocketIfTimeout())
                    {
                        this.closeChannel(addr, channel);
                        log.Warn("invokeSync: close socket because of timeout, {}ms, {}", timeoutMillis, addr);
                    }
                    log.Warn("invokeSync: wait response timeout exception, the channel[{}]", addr);
                    throw e;
                }
            }
            else
            {
                this.closeChannel(addr, channel);
                throw new RemotingConnectException(addr);
            }
        }

        ///<exception cref="ThreadInterruptedException"/>
        ///<exception cref="RemotingConnectException"/>
        private async Task<IChannel> getAndCreateChannel(string addr)
        {
            if (null == addr)
            {
                return await getAndCreateNameserverChannel();
            }

            ChannelWrapper cw = this.channelTables.Get(addr);
            if (cw != null && cw.isOK())
            {
                return cw.getChannel();
            }

            return await createChannel(addr);
        }

        ///<exception cref="ThreadInterruptedException"/>
        ///<exception cref="RemotingConnectException"/>
        private async Task<IChannel> getAndCreateNameserverChannel()
        {
            string addr = this.namesrvAddrChoosed.Value;
            if (addr != null)
            {
                ChannelWrapper cw = this.channelTables.Get(addr);
                if (cw != null && cw.isOK())
                {
                    return cw.getChannel();
                }
            }

            List<string> addrList = this.namesrvAddrList.Value;
            if(Monitor.TryEnter(namesrvChannelLock, LOCK_TIMEOUT_MILLIS))
            //if (this.namesrvChannelLock.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS))
            {
                try
                {
                    //addr = this.namesrvAddrChoosed.get();
                    addr = this.namesrvAddrChoosed.Value;
                    if (addr != null)
                    {
                        ChannelWrapper cw = this.channelTables.Get(addr);
                        if (cw != null && cw.isOK())
                        {
                            return cw.getChannel();
                        }
                    }

                    if (addrList != null && !addrList.IsEmpty())
                    {
                        for (int i = 0; i < addrList.Count; i++)
                        {
                            int index = this.namesrvIndex.incrementAndGet();
                            index = Math.Abs(index);
                            index = index % addrList.Count;
                            string newAddr = addrList.Get(index);

                            this.namesrvAddrChoosed.Value = newAddr;
                            log.Info("new name server is chosen. OLD: {} , NEW: {}. namesrvIndex = {}", addr, newAddr, namesrvIndex);
                            IChannel channelNew = await createChannel(newAddr);
                            if (channelNew != null)
                            {
                                return channelNew;
                            }
                        }
                        throw new RemotingConnectException(addrList.ToString());
                    }
                }
                finally
                {
                    //this.namesrvChannelLock.unlock();
                    Monitor.Exit(namesrvChannelLock);
                }
            }
            else
            {
                log.Warn("getAndCreateNameserverChannel: try to lock name server, but timeout, {}ms", LOCK_TIMEOUT_MILLIS);
            }
            return null;
        }

        ///<exception cref="ThreadInterruptedException"/>
        private async Task<IChannel> createChannel(string addr)
        {
            ChannelWrapper cw = this.channelTables.Get(addr);
            if (cw != null && cw.isOK())
            {
                return cw.getChannel();
            }

            if(Monitor.TryEnter(lockChannelTables, LOCK_TIMEOUT_MILLIS))
            //if (this.lockChannelTables.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS))
            {
                try
                {
                    bool createNewConnection;
                    cw = this.channelTables.Get(addr);
                    if (cw != null)
                    {

                        if (cw.isOK())
                        {
                            return cw.getChannel();
                        }
                        //else if (!cw.getChannelFuture().isDone())
                        else if (!cw.getChannelFuture().IsCompleted)
                        {
                            createNewConnection = false;
                        }
                        else
                        {
                            this.channelTables.JavaRemove(addr);
                            createNewConnection = true;
                        }
                    }
                    else
                    {
                        createNewConnection = true;
                    }

                    if (createNewConnection)
                    {
                        Task<IChannel> channelFuture = bootstrap.ConnectAsync(RemotingHelper.string2SocketAddress(addr));
                        //ChannelFuture channelFuture = this.bootstrap.connect(RemotingHelper.string2SocketAddress(addr));
                        log.Info("createChannel: begin to connect remote host[{}] asynchronously", addr);
                        cw = new ChannelWrapper(channelFuture);
                        this.channelTables.Put(addr, cw);
                    }
                }
                catch (Exception e)
                {
                    log.Error("createChannel: create channel exception", e.ToString());
                }
                finally
                {
                    //this.lockChannelTables.unlock();
                    Monitor.Exit(lockChannelTables);
                }
            }
            else
            {
                log.Warn("createChannel: try to lock channel table, but timeout, {}ms", LOCK_TIMEOUT_MILLIS);
            }

            if (cw != null)
            {
                Task<IChannel> channelFuture = cw.getChannelFuture();
                var channel = await channelFuture.WaitAsync(TimeSpan.FromMilliseconds(nettyClientConfig.getConnectTimeoutMillis()));
                //if (channelFuture.awaitUninterruptibly(this.nettyClientConfig.getConnectTimeoutMillis()))
                if (channelFuture.IsCompletedSuccessfully)
                {
                    cw.SetChannel(channel);
                    if (cw.isOK())
                    {
                        log.Info("createChannel: connect remote host[{}] success, {}", addr, channelFuture.ToString());
                        return cw.getChannel();
                    }
                    else
                    {
                        log.Warn("createChannel: connect remote host[" + addr + "] failed, " + channelFuture.ToString(), channelFuture.Exception);
                    }
                }
                else
                {
                    log.Warn("createChannel: connect remote host[{}] timeout {}ms, {}", addr, this.nettyClientConfig.getConnectTimeoutMillis(),
                        channelFuture.ToString());
                }
            }
            return null;
        }

        //@Override
        ///<exception cref="ThreadInterruptedException"/>
        ///<exception cref="RemotingConnectException"/>
        ///<exception cref="RemotingTooMuchRequestException"/>
        ///<exception cref="RemotingTimeoutException"/>
        ///<exception cref="RemotingSendRequestException"/>
        public async Task invokeAsync(String addr, RemotingCommand request, long timeoutMillis, InvokeCallback invokeCallback)
        {
            long beginStartTime = Sys.currentTimeMillis();
            IChannel channel = await getAndCreateChannel(addr);
            if (channel != null && channel.Active)
            {
                try
                {
                    doBeforeRpcHooks(addr, request);
                    long costTime = Sys.currentTimeMillis() - beginStartTime;
                    if (timeoutMillis < costTime)
                    {
                        throw new RemotingTooMuchRequestException("invokeAsync call the addr[" + addr + "] timeout");
                    }
                    this.invokeAsyncImpl(channel, request, timeoutMillis - costTime, invokeCallback);
                }
                catch (RemotingSendRequestException e)
                {
                    log.Warn("invokeAsync: send request exception, so close the channel[{}]", addr);
                    this.closeChannel(addr, channel);
                    throw e;
                }
            }
            else
            {
                this.closeChannel(addr, channel);
                throw new RemotingConnectException(addr);
            }
        }

        ///<exception cref="ThreadInterruptedException"/>
        ///<exception cref="RemotingConnectException"/>
        ///<exception cref="RemotingTooMuchRequestException"/>
        ///<exception cref="RemotingTimeoutException"/>
        ///<exception cref="RemotingSendRequestException"/>
        public async Task invokeOneway(String addr, RemotingCommand request, long timeoutMillis)
        {
            IChannel channel = await getAndCreateChannel(addr);
            if (channel != null && channel.Active)
            {
                try
                {
                    doBeforeRpcHooks(addr, request);
                    this.invokeOnewayImpl(channel, request, timeoutMillis);
                }
                catch (RemotingSendRequestException e)
                {
                    log.Warn("invokeOneway: send request exception, so close the channel[{}]", addr);
                    this.closeChannel(addr, channel);
                    throw e;
                }
            }
            else
            {
                this.closeChannel(addr, channel);
                throw new RemotingConnectException(addr);
            }
        }

        //@Override
        public void registerProcessor(int requestCode, NettyRequestProcessor processor, ExecutorService executor)
        {
            ExecutorService executorThis = executor;
            if (null == executor)
            {
                executorThis = this.publicExecutor;
            }

            Pair<NettyRequestProcessor, ExecutorService> pair = new Pair<NettyRequestProcessor, ExecutorService>(processor, executorThis);
            this.processorTable.Put(requestCode, pair);
        }

        //@Override
        public bool isChannelWritable(String addr)
        {
            ChannelWrapper cw = this.channelTables.Get(addr);
            if (cw != null && cw.isOK())
            {
                return cw.isWritable();
            }
            return true;
        }

        //@Override
        public List<String> getNameServerAddressList()
        {
            return this.namesrvAddrList.Value;
        }

        //@Override
        public override ChannelEventListener getChannelEventListener()
        {
            return channelEventListener;
        }

        //@Override
        public override ExecutorService getCallbackExecutor()
        {
            return callbackExecutor != null ? callbackExecutor : publicExecutor;
        }

        //@Override
        public void setCallbackExecutor(ExecutorService callbackExecutor)
        {
            this.callbackExecutor = callbackExecutor;
        }

        class ChannelWrapper
        {
            private IChannel channel;

            private Task<IChannel> channelFuture;

            public ChannelWrapper(Task<IChannel> channelFuture)
            {
                this.channelFuture = channelFuture;
            }

            public bool isOK()
            {
                return channel != null && channel.Active;
            }

            public bool isWritable()
            {
                return channel.IsWritable;
            }

            public IChannel getChannel()
            {
                return channel;
            }

            public void SetChannel(IChannel channel)
            {
                this.channel = channel;
            }

            public Task<IChannel> getChannelFuture()
            {
                return channelFuture;
            }
        }

        class NettyClientHandler : SimpleChannelInboundHandler<RemotingCommand>
        {

            private NettyRemotingClient owner;
            public NettyClientHandler(NettyRemotingClient owner)
            {
                this.owner = owner;
            }

            protected override void ChannelRead0(IChannelHandlerContext ctx, RemotingCommand msg)
            {
                owner.processMessageReceived(ctx, msg);
            }
        }

        class NettyConnectManageHandler : ChannelDuplexHandler
        {
            private NettyRemotingClient owner;
            public NettyConnectManageHandler(NettyRemotingClient owner)
            {
                this.owner = owner;
            }

            static NLog.Logger log = NLog.LogManager.GetCurrentClassLogger();
            public override Task ConnectAsync(IChannelHandlerContext ctx, EndPoint remoteAddress, EndPoint localAddress)
            {
                //return base.ConnectAsync(context, remoteAddress, localAddress);
                string local = localAddress == null ? "UNKNOWN" : RemotingHelper.parseSocketAddressAddr(localAddress);
                string remote = remoteAddress == null ? "UNKNOWN" : RemotingHelper.parseSocketAddressAddr(remoteAddress);
                log.Info("NETTY CLIENT PIPELINE: CONNECT  {} => {}", local, remote);

                base.ConnectAsync(ctx, remoteAddress, localAddress);

                if (owner.channelEventListener != null)
                {
                    owner.putNettyEvent(new NettyEvent(NettyEventType.CONNECT, remote, ctx.Channel));
                }
                return Task.CompletedTask;
            }

            public override Task DisconnectAsync(IChannelHandlerContext ctx)
            {
                //return base.DisconnectAsync(context);
                string remoteAddress = RemotingHelper.parseChannelRemoteAddr(ctx.Channel);
                log.Info("NETTY CLIENT PIPELINE: DISCONNECT {}", remoteAddress);
                owner.closeChannel(ctx.Channel);

                base.DisconnectAsync(ctx);

                if (owner.channelEventListener != null)
                {
                    owner.putNettyEvent(new NettyEvent(NettyEventType.CLOSE, remoteAddress, ctx.Channel));
                }
                return Task.CompletedTask;
            }

            public override Task CloseAsync(IChannelHandlerContext ctx)
            {
                //return base.CloseAsync(context);
                string remoteAddress = RemotingHelper.parseChannelRemoteAddr(ctx.Channel);
                log.Info("NETTY CLIENT PIPELINE: CLOSE {}", remoteAddress);
                owner.closeChannel(ctx.Channel);

                base.CloseAsync(ctx);
                owner.failFast(ctx.Channel);

                if (owner.channelEventListener != null)
                {
                    owner.putNettyEvent(new NettyEvent(NettyEventType.CLOSE, remoteAddress, ctx.Channel));
                }
                return Task.CompletedTask;
            }

            public override void UserEventTriggered(IChannelHandlerContext ctx, object evt)
            {
                if (evt is IdleStateEvent)
                {
                    IdleStateEvent ievt = (IdleStateEvent)evt;
                    if (ievt.State.Equals(IdleState.AllIdle))
                    {
                        string remoteAddress = RemotingHelper.parseChannelRemoteAddr(ctx.Channel);
                        log.Warn("NETTY CLIENT PIPELINE: IDLE exception [{}]", remoteAddress);

                        owner.closeChannel(ctx.Channel);

                        if (owner.channelEventListener != null)
                        {
                            owner.putNettyEvent(new NettyEvent(NettyEventType.IDLE, remoteAddress, ctx.Channel));
                        }
                    }
                }
                ctx.FireUserEventTriggered(evt);
            }

            public override void ExceptionCaught(IChannelHandlerContext ctx, Exception cause)
            {
                string remoteAddress = RemotingHelper.parseChannelRemoteAddr(ctx.Channel);
                log.Warn("NETTY CLIENT PIPELINE: exceptionCaught {}", remoteAddress);
                log.Warn("NETTY CLIENT PIPELINE: exceptionCaught exception.", cause.ToString());
                owner.closeChannel(ctx.Channel);
                if (owner.channelEventListener != null)
                {
                    owner.putNettyEvent(new NettyEvent(NettyEventType.EXCEPTION, remoteAddress, ctx.Channel));
                }
            }
        }
    }
}
