using DotNetty.Transport.Channels;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.NetworkInformation;
using System.Text;
using System.Threading.Tasks;

namespace RocketMQ.Client
{
    public class RemotingUtil
    {
        static NLog.Logger log = NLog.LogManager.GetCurrentClassLogger();
        //private static readonly InternalLogger log = InternalLoggerFactory.getLogger(RemotingHelper.ROCKETMQ_REMOTING);

        public static readonly string OS_NAME = Sys.getProperty("os.name");
        private static bool isLinuxPlatform = false;
        private static bool isWindowsPlatform = false;

        static RemotingUtil()
        {
            if (OS_NAME != null && OS_NAME.ToLower().Contains("linux"))
            {
                isLinuxPlatform = true;
            }

            if (OS_NAME != null && OS_NAME.ToLower().Contains("windows"))
            {
                isWindowsPlatform = true;
            }
        }

        public static bool GetIsWindowsPlatform()
        {
            return isWindowsPlatform;
        }

        ///<exception cref="IOException"/>
        //public static Selector openSelector()
        //{
        //    Selector result = null;

        //    if (isLinuxPlatform())
        //    {
        //        try
        //        {
        //            Class <?> providerClazz = Class.forName("sun.nio.ch.EPollSelectorProvider");
        //            if (providerClazz != null)
        //            {
        //                try
        //                {
        //                    Method method = providerClazz.getMethod("provider");
        //                    if (method != null)
        //                    {
        //                        SelectorProvider selectorProvider = (SelectorProvider)method.invoke(null);
        //                        if (selectorProvider != null)
        //                        {
        //                            result = selectorProvider.openSelector();
        //                        }
        //                    }
        //                }
        //                catch (Exception e)
        //                {
        //                    log.Warn("Open ePoll Selector for linux platform exception", e);
        //                }
        //            }
        //        }
        //        catch (Exception e)
        //        {
        //            // ignore
        //        }
        //    }

        //    if (result == null)
        //    {
        //        result = Selector.open();
        //    }

        //    return result;
        //}

        public static bool GetIsLinuxPlatform()
        {
            return isLinuxPlatform;
        }

        public static string getLocalAddress()
        {
            try
            {
                // Traversal Network interface to get the first non-loopback and non-private address
                var interfaces = System.Net.NetworkInformation.NetworkInterface.GetAllNetworkInterfaces();
                ArrayList<string> ipv4Result = new ArrayList<string>();
                ArrayList<string> ipv6Result = new ArrayList<string>();
                //while (enumeration.hasMoreElements())
                foreach(var networkInterface in interfaces)
                {
                    //NetworkInterface networkInterface = enumeration.nextElement();
                    if (isBridge(networkInterface))
                    {
                        continue;
                    }

                    //networkInterface.GetIPProperties().UnicastAddresses;
                    //Enumeration<InetAddress> en = networkInterface.getInetAddresses();
                    //while (en.hasMoreElements())
                    foreach (UnicastIPAddressInformation ip in networkInterface.GetIPProperties().UnicastAddresses)
                    {
                        IPAddress address = ip.Address;
                        
                        if (!address.Equals(IPAddress.Loopback))
                        {
                            if (address.AddressFamily == System.Net.Sockets.AddressFamily.InterNetworkV6)
                            {
                                ipv6Result.Add(normalizeHostAddress(address));
                            }
                            else
                            {
                                ipv4Result.Add(normalizeHostAddress(address));
                            }
                        }
                    }
                }

                // prefer ipv4
                if (!ipv4Result.IsEmpty())
                {
                    foreach (String ip in ipv4Result)
                    {
                        if (ip.StartsWith("127.0") || ip.StartsWith("192.168"))
                        {
                            continue;
                        }

                        return ip;
                    }

                    return ipv4Result.Get(ipv4Result.Count - 1);
                }
                else if (!ipv6Result.IsEmpty())
                {
                    return ipv6Result.Get(0);
                }
                //If failed to find,fall back to localhost
                return normalizeHostAddress(IPAddress.Loopback);
            }
            catch (Exception e)
            {
                log.Error("Failed to obtain local address", e.ToString());
            }
            return null;
        }

        public static string normalizeHostAddress(IPAddress localHost)
        {
            if (localHost.AddressFamily == System.Net.Sockets.AddressFamily.InterNetworkV6)
            {
                return "[" + localHost.ToString() + "]";
            }
            else
            {
                return localHost.ToString();
            }
        }


        /// <summary>
        /// 127.0.0.1:8888
        /// </summary>
        /// <param name="addr"></param>
        /// <returns></returns>
        public static IPEndPoint string2SocketAddress(string addr)
        {
            int split = addr.LastIndexOf(":");
            string host = addr.JavaSubstring(0, split);
            string port = addr.Substring(split + 1);
            //InetSocketAddress isa = new InetSocketAddress(host, int.Parse(port));
            //return isa;
            return new IPEndPoint(IPAddress.Parse(host), int.Parse(port));
        }

        public static string socketAddress2String(IPEndPoint addr)
        {
            StringBuilder sb = new StringBuilder();
            //InetSocketAddress inetSocketAddress = (InetSocketAddress)addr;
            //sb.Append(inetSocketAddress.getAddress().getHostAddress());
            sb.Append(addr.Address.ToString());
            sb.Append(":");
            sb.Append(addr.Port);
            return sb.ToString();
        }

        public static string convert2IpString(String addr)
        {
            return socketAddress2String(string2SocketAddress(addr));
        }

        private static bool isBridge(NetworkInterface networkInterface)
        {
            try
            {
                if (isLinuxPlatform)
                {
                    string interfaceName = networkInterface.Name;
                    string path = "/sys/class/net/" + interfaceName + "/bridge";
                    return File.Exists(path);
                }
            }
            catch (Exception e)
            {
                //Ignore
            }
            return false;
        }

        //public static SocketChannel connect(SocketAddress remote)
        //{
        //    return connect(remote, 1000 * 5);
        //}

        //public static SocketChannel connect(SocketAddress remote, int timeoutMillis)
        //{
        //    SocketChannel sc = null;
        //    try
        //    {
        //        sc = SocketChannel.open();
        //        sc.configureBlocking(true);
        //        sc.socket().setSoLinger(false, -1);
        //        sc.socket().setTcpNoDelay(true);
        //        if (NettySystemConfig.socketSndbufSize > 0)
        //        {
        //            sc.socket().setReceiveBufferSize(NettySystemConfig.socketSndbufSize);
        //        }
        //        if (NettySystemConfig.socketRcvbufSize > 0)
        //        {
        //            sc.socket().setSendBufferSize(NettySystemConfig.socketRcvbufSize);
        //        }
        //        sc.socket().connect(remote, timeoutMillis);
        //        sc.configureBlocking(false);
        //        return sc;
        //    }
        //    catch (Exception e)
        //    {
        //        if (sc != null)
        //        {
        //            try
        //            {
        //                sc.close();
        //            }
        //            catch (IOException e1)
        //            {
        //                //e1.printStackTrace();
        //                log.Error(e1);
        //            }
        //        }
        //    }

        //    return null;
        //}

        public static void closeChannel(IChannel channel)
        {
            string addrRemote = RemotingHelper.parseChannelRemoteAddr(channel);
            //    channel.close().addListener(new ChannelFutureListener() {
            //    @Override
            //    public void operationComplete(ChannelFuture future) throws Exception {
            //        log.Info("closeChannel: close the connection to remote address[{}] result: {}", addrRemote,
            //            future.isSuccess());
            //    }
            //});
            var task = channel.CloseAsync();
            //await task;
            log.Info("closeChannel: close the connection to remote address[{}] result: {}", addrRemote, task.IsCompletedSuccessfully);
        }
    }
}
