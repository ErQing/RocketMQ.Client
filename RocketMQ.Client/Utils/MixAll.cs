using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Net;
using System.Net.NetworkInformation;
using System.Reflection;
using System.Text;

namespace RocketMQ.Client
{
    public class MixAll
    {
        public static readonly string ROCKETMQ_HOME_ENV = "ROCKETMQ_HOME";
        public static readonly string ROCKETMQ_HOME_PROPERTY = "rocketmq.home.dir";
        public static readonly string NAMESRV_ADDR_ENV = "NAMESRV_ADDR";
        public static readonly string NAMESRV_ADDR_PROPERTY = "rocketmq.namesrv.addr";
        public static readonly string MESSAGE_COMPRESS_LEVEL = "rocketmq.message.compressLevel";
        public static readonly string DEFAULT_NAMESRV_ADDR_LOOKUP = "jmenv.tbsite.net";
        public static readonly string WS_DOMAIN_NAME = Sys.getProperty("rocketmq.namesrv.domain", DEFAULT_NAMESRV_ADDR_LOOKUP);
        public static readonly string WS_DOMAIN_SUBGROUP = Sys.getProperty("rocketmq.namesrv.domain.subgroup", "nsaddr");
        //http://jmenv.tbsite.net:8080/rocketmq/nsaddr
        //public static readonly string WS_ADDR = "http://" + WS_DOMAIN_NAME + ":8080/rocketmq/" + WS_DOMAIN_SUBGROUP;
        public static readonly string DEFAULT_PRODUCER_GROUP = "DEFAULT_PRODUCER";
        public static readonly string DEFAULT_CONSUMER_GROUP = "DEFAULT_CONSUMER";
        public static readonly string TOOLS_CONSUMER_GROUP = "TOOLS_CONSUMER";
        public static readonly string SCHEDULE_CONSUMER_GROUP = "SCHEDULE_CONSUMER";
        public static readonly string FILTERSRV_CONSUMER_GROUP = "FILTERSRV_CONSUMER";
        public static readonly string MONITOR_CONSUMER_GROUP = "__MONITOR_CONSUMER";
        public static readonly string CLIENT_INNER_PRODUCER_GROUP = "CLIENT_INNER_PRODUCER";
        public static readonly string SELF_TEST_PRODUCER_GROUP = "SELF_TEST_P_GROUP";
        public static readonly string SELF_TEST_CONSUMER_GROUP = "SELF_TEST_C_GROUP";
        public static readonly string ONS_HTTP_PROXY_GROUP = "CID_ONS-HTTP-PROXY";
        public static readonly string CID_ONSAPI_PERMISSION_GROUP = "CID_ONSAPI_PERMISSION";
        public static readonly string CID_ONSAPI_OWNER_GROUP = "CID_ONSAPI_OWNER";
        public static readonly string CID_ONSAPI_PULL_GROUP = "CID_ONSAPI_PULL";
        public static readonly string CID_RMQ_SYS_PREFIX = "CID_RMQ_SYS_";
        public static readonly List<String> LOCAL_INET_ADDRESS = getLocalInetAddress();
        public static readonly string LOCALHOST = localhost();
        public static readonly long MASTER_ID = 0L;//???
        public static readonly long CURRENT_JVM_PID = getPID();
        public static readonly string RETRY_GROUP_TOPIC_PREFIX = "%RETRY%";
        public static readonly string DLQ_GROUP_TOPIC_PREFIX = "%DLQ%";
        public static readonly string REPLY_TOPIC_POSTFIX = "REPLY_TOPIC";
        public static readonly string UNIQUE_MSG_QUERY_FLAG = "_UNIQUE_KEY_QUERY";
        public static readonly string DEFAULT_TRACE_REGION_ID = "DefaultRegion";
        public static readonly string CONSUME_CONTEXT_TYPE = "ConsumeContextType";
        public static readonly string CID_SYS_RMQ_TRANS = "CID_RMQ_SYS_TRANS";
        public static readonly string ACL_CONF_TOOLS_FILE = "/conf/tools.yml";
        public static readonly string REPLY_MESSAGE_FLAG = "reply";
        public static readonly string LMQ_PREFIX = "%LMQ%";
        public static readonly string MULTI_DISPATCH_QUEUE_SPLITTER = ",";
        //private static readonly InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.COMMON_LOGGER_NAME);
        static NLog.Logger log = NLog.LogManager.GetCurrentClassLogger();
        //public static readonly string DEFAULT_CHARSET = "UTF-8";
        public static readonly Encoding DEFAULT_CHARSET = Encoding.UTF8;

        public static string getWSAddr()
        {
            string wsDomainName = Sys.getProperty("rocketmq.namesrv.domain", DEFAULT_NAMESRV_ADDR_LOOKUP);
            string wsDomainSubgroup = Sys.getProperty("rocketmq.namesrv.domain.subgroup", "nsaddr");
            string wsAddr = "http://" + wsDomainName + ":8080/rocketmq/" + wsDomainSubgroup;
            if (wsDomainName.IndexOf(":") > 0)
            {
                wsAddr = "http://" + wsDomainName + "/rocketmq/" + wsDomainSubgroup;
            }
            return wsAddr;
        }

        public static string getRetryTopic(String consumerGroup)
        {
            return RETRY_GROUP_TOPIC_PREFIX + consumerGroup;
        }

        public static string getReplyTopic(String clusterName)
        {
            return clusterName + "_" + REPLY_TOPIC_POSTFIX;
        }

        public static bool isSysConsumerGroup(String consumerGroup)
        {
            return consumerGroup.StartsWith(CID_RMQ_SYS_PREFIX);
        }

        public static string getDLQTopic(String consumerGroup)
        {
            return DLQ_GROUP_TOPIC_PREFIX + consumerGroup;
        }

        public static string brokerVIPChannel(bool isChange, string brokerAddr)
        {
            if (isChange)
            {
                int split = brokerAddr.LastIndexOf(":");
                string ip = brokerAddr.Substring(0, split);
                string port = brokerAddr.Substring(split + 1);
                string brokerAddrNew = ip + ":" + (int.Parse(port) - 2);
                return brokerAddrNew;
            }
            else
            {
                return brokerAddr;
            }
        }

        public static long getPID()
        {
            return Process.GetCurrentProcess().Id;
            //String processName = java.lang.management.ManagementFactory.getRuntimeMXBean().getName();
            //if (processName != null && processName.Length > 0)
            //{
            //    try
            //    {
            //        return long.Parse(processName.Split("@")[0]);
            //    }
            //    catch (Exception e)
            //    {
            //        return 0;
            //    }
            //}
            //return 0;
        }

        ///<exception cref="IOException"/>
        public static void string2File(String str, string fileName)
        {
            string tmpFile = fileName + ".tmp";
            string2FileNotSafe(str, tmpFile);

            string bakFile = fileName + ".bak";
            string prevContent = file2String(fileName);
            if (prevContent != null)
            {
                string2FileNotSafe(prevContent, bakFile);
            }

            //File file = new File(fileName);
            //file.delete();
            if (File.Exists(fileName))
                File.Delete(fileName);

            //file = new File(tmpFile);
            //file.renameTo(new File(fileName));
            File.Move(tmpFile, fileName);
        }

        ///<exception cref="IOException"/>
        public static void string2FileNotSafe(String str, string fileName)
        {
            //File file = new File(fileName);
            //File fileParent = file.getParentFile();
            //if (fileParent != null)
            //{
            //    fileParent.mkdirs();
            //}
            //FileWriter fileWriter = null;

            //try
            //{
            //    fileWriter = new FileWriter(file);
            //    fileWriter.write(str);
            //}
            //catch (IOException e)
            //{
            //    throw e;
            //}
            //finally
            //{
            //    if (fileWriter != null)
            //    {
            //        fileWriter.close();
            //    }
            //}
            try
            {
                var fileInfo = new FileInfo(fileName);
                var dirInfo = fileInfo.Directory;
                if (!dirInfo.Exists)
                    dirInfo.Create();
                if (!File.Exists(fileName))
                    File.Create(fileName);
                File.WriteAllText(fileName, str);
            }
            catch (IOException e)
            {
                throw e;
            }
        }

        ///<exception cref="IOException"/>
        public static string file2String(String fileName)
        {
            if (File.Exists(fileName))
                return File.ReadAllText(fileName);
            else
                return null;
            //File file = new File(fileName);
            //return file2String(file);
        }

        ///<exception cref="IOException"/>
        //public static string file2String(File file)
        //{
        //    if (file.exists())
        //    {
        //        byte[] data = new byte[(int)file.length()];
        //        bool result;

        //        FileInputStream inputStream = null;
        //        try
        //        {
        //            inputStream = new FileInputStream(file);
        //            int len = inputStream.read(data);
        //            result = len == data.length;
        //        }
        //        finally
        //        {
        //            if (inputStream != null)
        //            {
        //                inputStream.close();
        //            }
        //        }

        //        if (result)
        //        {
        //            return new String(data);
        //        }
        //    }
        //    return null;
        //}

        //public static string file2String(URL url)
        //{
        //    InputStream input = null;
        //    try
        //    {
        //        URLConnection urlConnection = url.openConnection();
        //        urlConnection.setUseCaches(false);
        //        input = urlConnection.getInputStream();
        //        int len = input.available();
        //        byte[] data = new byte[len];
        //        input.read(data, 0, len);
        //        return new String(data, "UTF-8");
        //    }
        //    catch (Exception ignored)
        //    {
        //    }
        //    finally
        //    {
        //        if (null != input) {
        //            try
        //            {
        //                input.close();
        //            }
        //            catch (IOException ignored)
        //            {
        //            }
        //        }
        //    }

        //    return null;
        //}

        public static void printObjectProperties(NLog.Logger logger, object obj)
        {
            printObjectProperties(logger, obj, false);
        }

        public static void printObjectProperties(NLog.Logger logger, object obj,
            bool onlyImportantField)
        {
            //Field[] fields = obj.getClass().getDeclaredFields();
            var fields = obj.GetType().GetProperties();
            foreach (var field in fields)
            {
                //if (!Modifier.isStatic(field.getModifiers()))
                {
                    string name = field.Name;
                    //if (!name.StartsWith("this"))
                    {
                        object value = null;
                        try
                        {
                            //field.setAccessible(true);
                            //value = field.get(obj);
                            value = field.GetValue(obj);
                            if (null == value)
                            {
                                value = "";
                            }
                        }
                        catch (Exception e)
                        {
                            log.Error("Failed to obtain object properties", e.ToString());
                        }

                        if (onlyImportantField)
                        {
                            //Annotation annotation = field.getAnnotation(ImportantField/*.class*/);
                            var annotation = field.GetCustomAttribute<ImportantField>();
                            if (null == annotation)
                            {
                                continue;
                            }
                        }

                        if (logger != null)
                        {
                            logger.Info(name + "=" + value);
                        }
                        else
                        {
                        }
                    }
                }
            }
        }

        public static string properties2String(Properties properties)
        {
            StringBuilder sb = new StringBuilder();
            foreach (var entry in properties)
            {
                if (entry.Value != null)
                {
                    sb.Append(entry.Key.ToString() + "=" + entry.Value.ToString() + "\n");
                }
            }
            return sb.ToString();
        }

        public static Properties string2Properties(String str)
        {
            Properties properties = new Properties();
            try
            {
                //InputStream input = new ByteArrayInputStream(str.getBytes(DEFAULT_CHARSET));
                //properties.load(input); //TODO
            }
            catch (Exception e)
            {
                log.Error("Failed to handle properties", e.ToString());
                return null;
            }

            return properties;
        }

        public static Properties object2Properties(object obj)
        {
            Properties properties = new Properties();

            var fields = obj.GetType().GetProperties();
            foreach (var field in fields)
            {
                //if (!Modifier.isStatic(field.getModifiers()))
                {
                    string name = field.Name;
                    //if (!name.StartsWith("this")) //???
                    {
                        object value = null;
                        try
                        {
                            //field.setAccessible(true);
                            //value = field.get(obj);
                            value = field.GetValue(obj);
                        }
                        catch (Exception e)
                        {
                            log.Error("Failed to handle properties", e.ToString());
                        }

                        if (value != null)
                        {
                            properties.setProperty(name, value.ToString());
                        }
                    }
                }
            }

            return properties;
        }

        //public static void properties2Object(Properties p, object obj)
        //{
        //    Method[] methods = obj.getClass().getMethods();
        //    foreach (Method method in methods)
        //    {
        //        string mn = method.getName();
        //        if (mn.startsWith("set"))
        //        {
        //            try
        //            {
        //                string tmp = mn.substring(4);
        //                string first = mn.substring(3, 4);

        //                string key = first.toLowerCase() + tmp;
        //                string property = p.getProperty(key);
        //                if (property != null)
        //                {
        //                    Class <?>[] pt = method.getParameterTypes();
        //                    if (pt != null && pt.length > 0)
        //                    {
        //                        string cn = pt[0].getSimpleName();
        //                        Object arg = null;
        //                        if (cn.Equals("int") || cn.Equals("Integer"))
        //                        {
        //                            arg = Integer.parseInt(property);
        //                        }
        //                        else if (cn.Equals("long") || cn.Equals("Long"))
        //                        {
        //                            arg = Long.parseLong(property);
        //                        }
        //                        else if (cn.Equals("double") || cn.Equals("Double"))
        //                        {
        //                            arg = Double.parseDouble(property);
        //                        }
        //                        else if (cn.Equals("boolean") || cn.Equals("Boolean"))
        //                        {
        //                            arg = Boolean.parseBoolean(property);
        //                        }
        //                        else if (cn.Equals("float") || cn.Equals("Float"))
        //                        {
        //                            arg = Float.parseFloat(property);
        //                        }
        //                        else if (cn.Equals("String"))
        //                        {
        //                            arg = property;
        //                        }
        //                        else
        //                        {
        //                            continue;
        //                        }
        //                        method.invoke(obj, arg);
        //                    }
        //                }
        //            }
        //            catch (Exception ignored)
        //            {
        //            }
        //        }
        //    }
        //}

        public static bool isPropertiesEqual(Properties p1, Properties p2)
        {
            return p1.Equals(p2);
        }

        public static List<String> getLocalInetAddress()
        {
            List<String> inetAddressList = new List<String>();
            try
            {
                var interfaces = System.Net.NetworkInformation.NetworkInterface.GetAllNetworkInterfaces();
                //Enumeration<NetworkInterface> enumeration = NetworkInterface.GetAllNetworkInterfaces();
                //while (enumeration.hasMoreElements())
                foreach (var networkInterface in interfaces)
                {
                    //NetworkInterface networkInterface = enumeration.nextElement();
                    //Enumeration<InetAddress> addrs = networkInterface.getInetAddresses();
                    //while (addrs.hasMoreElements())
                    foreach (UnicastIPAddressInformation ip in networkInterface.GetIPProperties().UnicastAddresses)
                    {
                        inetAddressList.Add(ip.Address.ToString());
                    }
                }
            }
            catch (Exception e)
            {
                throw new RuntimeException("get local inet address fail", e);
            }

            return inetAddressList;
        }

        private static string localhost()
        {
            try
            {
                return IPAddress.Loopback.ToString();
                //return InetAddress.getLocalHost().getHostAddress();
            }
            catch (Exception e)
            {
                try
                {
                    string candidatesHost = getLocalhostByNetworkInterface();
                    if (candidatesHost != null)
                        return candidatesHost;

                }
                catch (Exception ignored)
                {
                }

                throw new RuntimeException("InetAddress java.net.InetAddress.getLocalHost() throws UnknownHostException" + FAQUrl.suggestTodo(FAQUrl.UNKNOWN_HOST_EXCEPTION), e);
            }
        }

        //Reverse logic comparing to RemotingUtil method, consider refactor in RocketMQ 5.0
        ///<exception cref="SocketException"/>
        public static string getLocalhostByNetworkInterface()
        {
            List<String> candidatesHost = new List<String>();

            var interfaces = System.Net.NetworkInformation.NetworkInterface.GetAllNetworkInterfaces();
            //Enumeration<NetworkInterface> enumeration = NetworkInterface.getNetworkInterfaces();
            //while (enumeration.hasMoreElements())
            foreach (var networkInterface in interfaces)
            {
                //NetworkInterface networkInterface = enumeration.nextElement();
                // Workaround for docker0 bridge
                //if ("docker0".Equals(networkInterface.Name) || !networkInterface.isUp())
                if ("docker0".Equals(networkInterface.Name)) //??? TODO
                {
                    continue;
                }
                //Enumeration<InetAddress> addrs = networkInterface.getInetAddresses();
                //while (addrs.hasMoreElements())
                foreach (UnicastIPAddressInformation ip in networkInterface.GetIPProperties().UnicastAddresses)
                {
                    IPAddress address = ip.Address;
                    if (!address.Equals(IPAddress.Loopback))
                    {
                        continue;
                    }
                    //ip4 higher priority
                    //if (address.AddressFamily is Inet6Address)
                    if (address.AddressFamily == System.Net.Sockets.AddressFamily.InterNetworkV6)
                    {
                        candidatesHost.Add(address.ToString());
                        continue;
                    }
                    return address.ToString();
                }
            }

            if (!candidatesHost.IsEmpty())
            {
                return candidatesHost.Get(0);
            }
            return null;
        }

        public static bool compareAndIncreaseOnly(AtomicLong target, long value)
        {
            long prev = target.Get();
            while (value > prev)
            {
                bool updated = target.CompareAndSet(prev, value);
                if (updated)
                    return true;

                prev = target.Get();
            }

            return false;
        }

        public static string humanReadableByteCount(long bytes, bool si)
        {
            int unit = si ? 1000 : 1024;
            if (bytes < unit)
                return bytes + " B";
            int exp = (int)(Math.Log(bytes) / Math.Log(unit));
            //String pre = (si ? "kMGTPE" : "KMGTPE").charAt(exp - 1) + (si ? "" : "i");
            //return String.format("%.1f %sB", bytes / Math.pow(unit, exp), pre);
            string pre = (si ? "kMGTPE" : "KMGTPE")[exp - 1] + (si ? "" : "i");
            return string.Format("%.1f %sB", bytes / Math.Pow(unit, exp), pre);
        }

        public static bool isLmq(String lmqMetaData)
        {
            return lmqMetaData != null && lmqMetaData.StartsWith(LMQ_PREFIX);
        }

    }
}
