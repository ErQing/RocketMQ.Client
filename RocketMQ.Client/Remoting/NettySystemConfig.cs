using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RocketMQ.Client
{
    public class NettySystemConfig
    {
        public static readonly string COM_ROCKETMQ_REMOTING_NETTY_POOLED_BYTE_BUF_ALLOCATOR_ENABLE =
        "com.rocketmq.remoting.nettyPooledByteBufAllocatorEnable";
    public static readonly string COM_ROCKETMQ_REMOTING_SOCKET_SNDBUF_SIZE =
        "com.rocketmq.remoting.socket.sndbuf.size";
    public static readonly string COM_ROCKETMQ_REMOTING_SOCKET_RCVBUF_SIZE =
        "com.rocketmq.remoting.socket.rcvbuf.size";
    public static readonly string COM_ROCKETMQ_REMOTING_SOCKET_BACKLOG =
        "com.rocketmq.remoting.socket.backlog";
    public static readonly string COM_ROCKETMQ_REMOTING_CLIENT_ASYNC_SEMAPHORE_VALUE =
        "com.rocketmq.remoting.clientAsyncSemaphoreValue";
    public static readonly string COM_ROCKETMQ_REMOTING_CLIENT_ONEWAY_SEMAPHORE_VALUE =
        "com.rocketmq.remoting.clientOnewaySemaphoreValue";
    public static readonly string COM_ROCKETMQ_REMOTING_CLIENT_WORKER_SIZE =
        "com.rocketmq.remoting.client.worker.size";
    public static readonly string COM_ROCKETMQ_REMOTING_CLIENT_CONNECT_TIMEOUT =
        "com.rocketmq.remoting.client.connect.timeout";
    public static readonly string COM_ROCKETMQ_REMOTING_CLIENT_CHANNEL_MAX_IDLE_SECONDS =
        "com.rocketmq.remoting.client.channel.maxIdleTimeSeconds";
    public static readonly string COM_ROCKETMQ_REMOTING_CLIENT_CLOSE_SOCKET_IF_TIMEOUT =
        "com.rocketmq.remoting.client.closeSocketIfTimeout";
    public static readonly string COM_ROCKETMQ_REMOTING_WRITE_BUFFER_HIGH_WATER_MARK_VALUE =
        "com.rocketmq.remoting.write.buffer.high.water.mark";
    public static readonly string COM_ROCKETMQ_REMOTING_WRITE_BUFFER_LOW_WATER_MARK =
        "com.rocketmq.remoting.write.buffer.low.water.mark";

    public static readonly bool NETTY_POOLED_BYTE_BUF_ALLOCATOR_ENABLE = //
        bool.Parse(Sys.getProperty(COM_ROCKETMQ_REMOTING_NETTY_POOLED_BYTE_BUF_ALLOCATOR_ENABLE, "false"));
    public static readonly int CLIENT_ASYNC_SEMAPHORE_VALUE = //
        int.Parse(Sys.getProperty(COM_ROCKETMQ_REMOTING_CLIENT_ASYNC_SEMAPHORE_VALUE, "65535"));
        public static readonly int CLIENT_ONEWAY_SEMAPHORE_VALUE =
            int.Parse(Sys.getProperty(COM_ROCKETMQ_REMOTING_CLIENT_ONEWAY_SEMAPHORE_VALUE, "65535"));
        public static int socketSndbufSize =
            int.Parse(Sys.getProperty(COM_ROCKETMQ_REMOTING_SOCKET_SNDBUF_SIZE, "0"));
        public static int socketRcvbufSize =
            int.Parse(Sys.getProperty(COM_ROCKETMQ_REMOTING_SOCKET_RCVBUF_SIZE, "0"));
        public static int socketBacklog =
            int.Parse(Sys.getProperty(COM_ROCKETMQ_REMOTING_SOCKET_BACKLOG, "1024"));
        public static int clientWorkerSize =
            int.Parse(Sys.getProperty(COM_ROCKETMQ_REMOTING_CLIENT_WORKER_SIZE, "4"));
        public static int connectTimeoutMillis =
            int.Parse(Sys.getProperty(COM_ROCKETMQ_REMOTING_CLIENT_CONNECT_TIMEOUT, "3000"));
        public static int clientChannelMaxIdleTimeSeconds =
            int.Parse(Sys.getProperty(COM_ROCKETMQ_REMOTING_CLIENT_CHANNEL_MAX_IDLE_SECONDS, "120"));
        public static bool clientCloseSocketIfTimeout =
            bool.Parse(Sys.getProperty(COM_ROCKETMQ_REMOTING_CLIENT_CLOSE_SOCKET_IF_TIMEOUT, "true"));
        public static int writeBufferHighWaterMark =
            int.Parse(Sys.getProperty(COM_ROCKETMQ_REMOTING_WRITE_BUFFER_HIGH_WATER_MARK_VALUE, "0"));
        public static int writeBufferLowWaterMark =
            int.Parse(Sys.getProperty(COM_ROCKETMQ_REMOTING_WRITE_BUFFER_LOW_WATER_MARK, "0"));
    }
}
