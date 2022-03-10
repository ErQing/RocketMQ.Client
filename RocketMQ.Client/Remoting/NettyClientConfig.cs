using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RocketMQ.Client
{
    public class NettyClientConfig
    {
        /**
     * Worker thread number
     */
        private int clientWorkerThreads = NettySystemConfig.clientWorkerSize;
        private int clientCallbackExecutorThreads = Runtime.getRuntime().availableProcessors();
        private int clientOnewaySemaphoreValue = NettySystemConfig.CLIENT_ONEWAY_SEMAPHORE_VALUE;
        private int clientAsyncSemaphoreValue = NettySystemConfig.CLIENT_ASYNC_SEMAPHORE_VALUE;
        private int connectTimeoutMillis = NettySystemConfig.connectTimeoutMillis;
        private long channelNotActiveInterval = 1000 * 60;

        /**
         * IdleStateEvent will be triggered when neither read nor write was performed for
         * the specified period of this time. Specify {@code 0} to disable
         */
        private int clientChannelMaxIdleTimeSeconds = NettySystemConfig.clientChannelMaxIdleTimeSeconds;

        private int clientSocketSndBufSize = NettySystemConfig.socketSndbufSize;
        private int clientSocketRcvBufSize = NettySystemConfig.socketRcvbufSize;
        private bool clientPooledByteBufAllocatorEnable = false;
        private bool clientCloseSocketIfTimeout = NettySystemConfig.clientCloseSocketIfTimeout;

        private bool useTLS;

        private int writeBufferHighWaterMark = NettySystemConfig.writeBufferHighWaterMark;
        private int writeBufferLowWaterMark = NettySystemConfig.writeBufferLowWaterMark;

        public bool isClientCloseSocketIfTimeout()
        {
            return clientCloseSocketIfTimeout;
        }

        public void setClientCloseSocketIfTimeout(bool clientCloseSocketIfTimeout)
        {
            this.clientCloseSocketIfTimeout = clientCloseSocketIfTimeout;
        }

        public int getClientWorkerThreads()
        {
            return clientWorkerThreads;
        }

        public void setClientWorkerThreads(int clientWorkerThreads)
        {
            this.clientWorkerThreads = clientWorkerThreads;
        }

        public int getClientOnewaySemaphoreValue()
        {
            return clientOnewaySemaphoreValue;
        }

        public void setClientOnewaySemaphoreValue(int clientOnewaySemaphoreValue)
        {
            this.clientOnewaySemaphoreValue = clientOnewaySemaphoreValue;
        }

        public int getConnectTimeoutMillis()
        {
            return connectTimeoutMillis;
        }

        public void setConnectTimeoutMillis(int connectTimeoutMillis)
        {
            this.connectTimeoutMillis = connectTimeoutMillis;
        }

        public int getClientCallbackExecutorThreads()
        {
            return clientCallbackExecutorThreads;
        }

        public void setClientCallbackExecutorThreads(int clientCallbackExecutorThreads)
        {
            this.clientCallbackExecutorThreads = clientCallbackExecutorThreads;
        }

        public long getChannelNotActiveInterval()
        {
            return channelNotActiveInterval;
        }

        public void setChannelNotActiveInterval(long channelNotActiveInterval)
        {
            this.channelNotActiveInterval = channelNotActiveInterval;
        }

        public int getClientAsyncSemaphoreValue()
        {
            return clientAsyncSemaphoreValue;
        }

        public void setClientAsyncSemaphoreValue(int clientAsyncSemaphoreValue)
        {
            this.clientAsyncSemaphoreValue = clientAsyncSemaphoreValue;
        }

        public int getClientChannelMaxIdleTimeSeconds()
        {
            return clientChannelMaxIdleTimeSeconds;
        }

        public void setClientChannelMaxIdleTimeSeconds(int clientChannelMaxIdleTimeSeconds)
        {
            this.clientChannelMaxIdleTimeSeconds = clientChannelMaxIdleTimeSeconds;
        }

        public int getClientSocketSndBufSize()
        {
            return clientSocketSndBufSize;
        }

        public void setClientSocketSndBufSize(int clientSocketSndBufSize)
        {
            this.clientSocketSndBufSize = clientSocketSndBufSize;
        }

        public int getClientSocketRcvBufSize()
        {
            return clientSocketRcvBufSize;
        }

        public void setClientSocketRcvBufSize(int clientSocketRcvBufSize)
        {
            this.clientSocketRcvBufSize = clientSocketRcvBufSize;
        }

        public bool isClientPooledByteBufAllocatorEnable()
        {
            return clientPooledByteBufAllocatorEnable;
        }

        public void setClientPooledByteBufAllocatorEnable(bool clientPooledByteBufAllocatorEnable)
        {
            this.clientPooledByteBufAllocatorEnable = clientPooledByteBufAllocatorEnable;
        }

        public bool isUseTLS()
        {
            return useTLS;
        }

        public void setUseTLS(bool useTLS)
        {
            this.useTLS = useTLS;
        }

        public int getWriteBufferLowWaterMark()
        {
            return writeBufferLowWaterMark;
        }

        public void setWriteBufferLowWaterMark(int writeBufferLowWaterMark)
        {
            this.writeBufferLowWaterMark = writeBufferLowWaterMark;
        }

        public int getWriteBufferHighWaterMark()
        {
            return writeBufferHighWaterMark;
        }

        public void setWriteBufferHighWaterMark(int writeBufferHighWaterMark)
        {
            this.writeBufferHighWaterMark = writeBufferHighWaterMark;
        }
    }
}
