using DotNetty.Common.Utilities;
using DotNetty.Transport.Channels;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace RocketMQ.Client
{
    public class RemotingHelper
    {
        public static readonly String ROCKETMQ_REMOTING = "RocketmqRemoting";
        public static readonly String DEFAULT_CHARSET = "UTF-8";

        //private static readonly InternalLogger log = InternalLoggerFactory.getLogger(ROCKETMQ_REMOTING);
        static NLog.Logger log = NLog.LogManager.GetCurrentClassLogger();
        //private static readonly AttributeKey<String> REMOTE_ADDR_KEY = AttributeKey.valueOf("RemoteAddr");
        public static readonly AttributeKey<string> REMOTE_ADDR_KEY = AttributeKey<string>.ValueOf("RemoteAddr");

        public static String exceptionSimpleDesc(Exception e)
        {
            StringBuilder sb = new StringBuilder();
            if (e != null)
            {
                sb.Append(e.ToString());
                sb.Append(e.StackTrace);
                //StackTraceElement[] stackTrace = e.getStackTrace();
                //if (stackTrace != null && stackTrace.Length > 0)
                //{
                //    StackTraceElement element = stackTrace[0];
                //    sb.Append(", ");
                //    sb.Append(element.toString());
                //}
            }

            return sb.ToString();
        }

        public static IPEndPoint string2SocketAddress(string addr)
        {
            int split = addr.LastIndexOf(":");
            string host = addr.JavaSubstring(0, split);
            string port = addr.Substring(split + 1);
            IPEndPoint isa = new IPEndPoint(IPAddress.Parse(host), int.Parse(port));
            return isa;
        }

        ///<exception cref="RemotingCommandException"/>
        ///<exception cref="RemotingTimeoutException"/>
        ///<exception cref="ThreadInterruptedException"/>
        ///<exception cref="RemotingConnectException"/>
        ///<exception cref="RemotingSendRequestException"/>
        //public static RemotingCommand invokeSync(String addr, RemotingCommand request, long timeoutMillis)
        //{
        //    long beginTime = Sys.currentTimeMillis();
        //    SocketAddress socketAddress = RemotingUtil.string2SocketAddress(addr);
        //    SocketChannel socketChannel = RemotingUtil.connect(socketAddress);
        //    if (socketChannel != null)
        //    {
        //        bool sendRequestOK = false;
        //        try
        //        {

        //            socketChannel.configureBlocking(true);

        //            //bugfix  http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=4614802
        //            socketChannel.socket().setSoTimeout((int)timeoutMillis);

        //            ByteBuffer byteBufferRequest = request.encode();
        //            while (byteBufferRequest.hasRemaining())
        //            {
        //                int length = socketChannel.write(byteBufferRequest);
        //                if (length > 0)
        //                {
        //                    if (byteBufferRequest.hasRemaining())
        //                    {
        //                        if ((Sys.currentTimeMillis() - beginTime) > timeoutMillis)
        //                        {

        //                            throw new RemotingSendRequestException(addr);
        //                        }
        //                    }
        //                }
        //                else
        //                {
        //                    throw new RemotingSendRequestException(addr);
        //                }

        //                Thread.Sleep(1);
        //            }

        //            sendRequestOK = true;

        //            ByteBuffer byteBufferSize = ByteBuffer.allocate(4);
        //            while (byteBufferSize.hasRemaining())
        //            {
        //                int length = socketChannel.read(byteBufferSize);
        //                if (length > 0)
        //                {
        //                    if (byteBufferSize.hasRemaining())
        //                    {
        //                        if ((Sys.currentTimeMillis() - beginTime) > timeoutMillis)
        //                        {

        //                            throw new RemotingTimeoutException(addr, timeoutMillis);
        //                        }
        //                    }
        //                }
        //                else
        //                {
        //                    throw new RemotingTimeoutException(addr, timeoutMillis);
        //                }

        //                Thread.Sleep(1);
        //            }

        //            int size = byteBufferSize.getInt(0);
        //            ByteBuffer byteBufferBody = ByteBuffer.allocate(size);
        //            while (byteBufferBody.hasRemaining())
        //            {
        //                int length = socketChannel.read(byteBufferBody);
        //                if (length > 0)
        //                {
        //                    if (byteBufferBody.hasRemaining())
        //                    {
        //                        if ((Sys.currentTimeMillis() - beginTime) > timeoutMillis)
        //                        {

        //                            throw new RemotingTimeoutException(addr, timeoutMillis);
        //                        }
        //                    }
        //                }
        //                else
        //                {
        //                    throw new RemotingTimeoutException(addr, timeoutMillis);
        //                }

        //                Thread.Sleep(1);
        //            }

        //            byteBufferBody.flip();
        //            return RemotingCommand.decode(byteBufferBody);
        //        }
        //        catch (IOException e)
        //        {
        //            log.Error("invokeSync failure", e);

        //            if (sendRequestOK)
        //            {
        //                throw new RemotingTimeoutException(addr, timeoutMillis);
        //            }
        //            else
        //            {
        //                throw new RemotingSendRequestException(addr);
        //            }
        //        }
        //        finally
        //        {
        //            try
        //            {
        //                socketChannel.close();
        //            }
        //            catch (IOException e)
        //            {
        //                //e.printStackTrace();
        //                log.Error(e);
        //            }
        //        }
        //    }
        //    else
        //    {
        //        throw new RemotingConnectException(addr);
        //    }
        //}

        public static String parseChannelRemoteAddr(IChannel channel)
        {
            if (null == channel)
            {
                return "";
            }
            //Attribute<String> att = channel.attr(REMOTE_ADDR_KEY);
            var att = channel.GetAttribute<String>(REMOTE_ADDR_KEY);
            if (att == null)
            {
                // mocked in unit test
                return parseChannelRemoteAddr0(channel);
            }
            string addr = att.Get();
            if (addr == null)
            {
                addr = parseChannelRemoteAddr0(channel);
                att.Set(addr);
            }
            return addr;
        }

        private static String parseChannelRemoteAddr0(IChannel channel)
        {
            EndPoint remote = channel.RemoteAddress;
            string addr = remote != null ? remote.ToString() : "";

            if (addr.length() > 0)
            {
                int index = addr.LastIndexOf("/");
                if (index >= 0)
                {
                    return addr.Substring(index + 1);
                }

                return addr;
            }

            return "";
        }

        public static string parseSocketAddressAddr(EndPoint socketAddress)
        {
            if (socketAddress != null)
            {
                string addr = socketAddress.ToString();
                int index = addr.LastIndexOf("/");
                return (index != -1) ? addr.Substring(index + 1) : addr;
            }
            return "";
        }
    }
}
