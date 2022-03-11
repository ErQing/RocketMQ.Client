using ICSharpCode.SharpZipLib.Checksum;
using ICSharpCode.SharpZipLib.Zip;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.NetworkInformation;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;

namespace RocketMQ.Client
{
    public static class UtilAll
    {
        static NLog.Logger log = NLog.LogManager.GetCurrentClassLogger();
        public static readonly string YYYY_MM_DD_HH_MM_SS = "yyyy-MM-dd HH:mm:ss";
        public static readonly string YYYY_MM_DD_HH_MM_SS_SSS = "yyyy-MM-dd#HH:mm:ss:SSS";
        public static readonly string YYYYMMDDHHMMSS = "yyyyMMddHHmmss";

        internal static string formatDate(DateTime dateTime, string yYYY_MM_DD_HH_MM_SS_SSS)
        {
            //throw new NotImplementedException();
            return dateTime.ToString(yYYY_MM_DD_HH_MM_SS_SSS);
        }

        readonly static char[] HEX_ARRAY = "0123456789ABCDEF".ToCharArray();
        //readonly static string HOST_NAME = ManagementFactory.getRuntimeMXBean().getName(); // format: "pid@hostname"

        public static string Array2String<T>(IEnumerable<T> list)
        {
            return "[" + string.Join(",", list) + "]";
        }

        public static string responseCode2String(int code)
        {
            return code.ToString();
        }


        /// <summary>
        /// TODO
        /// </summary>
        /// <returns></returns>
        public static int getPid()
        {
            return -1;
        }

        public static long nanoTime()
        {
            long nano = 10000L * Stopwatch.GetTimestamp();
            nano /= TimeSpan.TicksPerMillisecond;
            nano *= 100L;
            return nano;
        }

        public static string JavaSubstring(this string s, int beginIndex, int endIndex)
        {
            // simulates Java substring function
            int len = endIndex - beginIndex;
            return s.Substring(beginIndex, len);
        }

        /// <summary>
        /// TODO
        /// </summary>
        /// <param name="t"></param>
        /// <returns></returns>
        public static string timeMillisToHumanString3(long tick)
        {
            DateTime dt = new DateTime(tick);
            return dt.ToString();
            //Calendar cal = Calendar.getInstance();
            //cal.setTimeInMillis(t);
            //return String.format("%04d%02d%02d%02d%02d%02d",
            //    cal.get(Calendar.YEAR),
            //    cal.get(Calendar.MONTH) + 1,
            //    cal.get(Calendar.DAY_OF_MONTH),
            //    cal.get(Calendar.HOUR_OF_DAY),
            //    cal.get(Calendar.MINUTE),
            //    cal.get(Calendar.SECOND));
        }

        internal static string bytes2string(byte[] src)
        {
            char[] hexChars = new char[src.Length * 2];
            for (int j = 0; j < src.Length; j++)
            {
                uint v = (uint)src[j] & 0xFF;
                hexChars[j * 2] = HEX_ARRAY[v >> 4]; //uint
                hexChars[j * 2 + 1] = HEX_ARRAY[v & 0x0F];
            }
            return new String(hexChars);
        }

        public static string timeMillisToHumanString(long tick)
        {
            //Calendar cal = Calendar.getInstance();
            //cal.setTimeInMillis(t);
            //return String.format("%04d%02d%02d%02d%02d%02d%03d", cal.get(Calendar.YEAR), cal.get(Calendar.MONTH) + 1,
            //    cal.get(Calendar.DAY_OF_MONTH), cal.get(Calendar.HOUR_OF_DAY), cal.get(Calendar.MINUTE), cal.get(Calendar.SECOND),
            //    cal.get(Calendar.MILLISECOND));
            DateTime dt = new DateTime(tick);
            return dt.ToString();
        }

        public static DateTime? parseDate(string date, string pattern)
        {
            try
            {
                return DateTime.ParseExact(date, pattern, CultureInfo.InvariantCulture);
            }
            catch (Exception)
            {
                return null;
            }
        }


        internal static byte[] string2bytes(string hexString)
        {
            throw new NotImplementedException();
            if (hexString == null || hexString.Equals(""))
            {
                return null;
            }
            //hexString = hexString.toUpperCase();
            hexString = hexString.ToUpper();
            int length = hexString.length() / 2;
            char[] hexChars = hexString.ToCharArray();
            byte[] d = new byte[length];
            for (int i = 0; i < length; i++)
            {
                int pos = i * 2;
                d[i] = (byte)(charToByte(hexChars[pos]) << 4 | charToByte(hexChars[pos + 1]));
            }
            return d;
        }

        private static byte charToByte(char c)
        {
            return (byte)"0123456789ABCDEF".IndexOf(c);
        }

        public static string ipToIPv6Str(byte[] ipBytes)
        {
            if (ipBytes.Length != 16)
                return null;
            try
            {
                IPAddress add = new IPAddress(ipBytes);
                return IsValideIPv6(add.ToString()) ? add.ToString() : null;
            }
            catch (Exception)
            {
                return null;
            }
        }

        public static bool IsValideIPv4(byte[] ipBytes)
        {
            if (ipBytes.Length != 4)
                throw new RuntimeException("illegal ipv4 bytes");
            IPAddress add = new IPAddress(ipBytes);
            return IsValideIPv4(add.ToString());
        }

        public static bool IsValideIPv4(string ipString)
        {
            if (string.IsNullOrEmpty(ipString))
                return false;
            if (ipString.Count(c => c == '.') != 3) return false;
            IPAddress.TryParse(ipString, out IPAddress address);
            return address.AddressFamily == System.Net.Sockets.AddressFamily.InterNetwork;
        }

        private static bool IsValideIPv6(byte[] ip)
        {
            if (ip.Length != 16)
                throw new RuntimeException("illegal ipv6 bytes");
            IPAddress add = new IPAddress(ip);
            return IsValideIPv6(add.ToString());
        }

        public static bool IsValideIPv6(string ipString)
        {
            if (string.IsNullOrEmpty(ipString))
                return false;
            IPAddress.TryParse(ipString, out IPAddress address);
            return address.AddressFamily == System.Net.Sockets.AddressFamily.InterNetworkV6;
        }

        public static int availableProcessors()
        {
            return System.Environment.ProcessorCount;
        }

        public static bool isNotEmpty(string str)
        {
            return !string.IsNullOrEmpty(str);
        }

        public static bool isBlank(string str)
        {
            return string.IsNullOrEmpty(str);
        }

        public static int crc32(byte[] array)
        {
            if (array != null)
            {
                Crc32 crc32 = new Crc32();
                crc32.Update(array);
                return (int)(crc32.Value & 0x7FFFFFFF);
            }
            return 0;
        }

        internal static string join(List<string> list, string v)
        {
            throw new NotImplementedException();
        }

        internal static long computeNextMinutesTimeMillis()
        {
            //Calendar cal = Calendar.getInstance();
            //cal.setTimeInMillis(System.currentTimeMillis());
            //cal.add(Calendar.DAY_OF_MONTH, 0);
            //cal.add(Calendar.HOUR_OF_DAY, 0);
            //cal.add(Calendar.MINUTE, 1);
            //cal.set(Calendar.SECOND, 0);
            //cal.set(Calendar.MILLISECOND, 0);
            DateTime now = DateTime.Now;
            now.AddMinutes(1);
            return now.Ticks;
        }

        public static string name(this Enum col)
        {
            return col.ToString();
        }

        public static long computeNextHourTimeMillis()
        {
            //Calendar cal = Calendar.getInstance();
            //cal.setTimeInMillis(System.currentTimeMillis());
            //cal.add(Calendar.DAY_OF_MONTH, 0);
            //cal.add(Calendar.HOUR_OF_DAY, 1);
            //cal.set(Calendar.MINUTE, 0);
            //cal.set(Calendar.SECOND, 0);
            //cal.set(Calendar.MILLISECOND, 0);
            //return cal.getTimeInMillis();
            DateTime now = DateTime.Now;
            now.AddHours(1);
            return now.Ticks;


        }

        public static long computeNextMorningTimeMillis()
        {
            //throw new NotImplementedException();
            //Calendar cal = Calendar.getInstance();
            //cal.setTimeInMillis(System.currentTimeMillis());
            //cal.add(Calendar.DAY_OF_MONTH, 1);
            //cal.set(Calendar.HOUR_OF_DAY, 0);
            //cal.set(Calendar.MINUTE, 0);
            //cal.set(Calendar.SECOND, 0);
            //cal.set(Calendar.MILLISECOND, 0);
            //return cal.getTimeInMillis();
            DateTime now = DateTime.Now;
            now.AddMonths(1);
            return now.Ticks;
        }

        public static void writeInt(char[] buffer, int pos, int value)
        {
            //throw new NotImplementedException();
            char[] hexArray = HEX_ARRAY;
            for (int moveBits = 28; moveBits >= 0; moveBits -= 4)
            {
                //buffer[pos++] = hexArray[(value >>> moveBits) & 0x0F];
                buffer[pos++] = hexArray[(value.UnsignedRightShift(moveBits)) & 0x0F];
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int UnsignedRightShift(this int val, int moveBits)
        {
            return (int)((uint)val >> moveBits);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static long UnsignedRightShift(this long val, int moveBits)
        {
            return (long)((ulong)val >> moveBits);
        }

        public static void writeShort(char[] buffer, int pos, int value)
        {
            char[] hexArray = HEX_ARRAY;
            for (int moveBits = 12; moveBits >= 0; moveBits -= 4)
            {
                //buffer[pos++] = hexArray[(value >>> moveBits) & 0x0F];
                buffer[pos++] = hexArray[(value.UnsignedRightShift(moveBits)) & 0x0F];
            }
        }

        //public static string ArrayToString(this string[] items)
        //{
        //    return string.Join(",", items.Select(x => x.ToString()).ToArray());
        //}


        /// <summary>
        ///  java : return ((long)(next(32)) << 32) + next(32);
        /// </summary>
        /// <param name="ran"></param>
        /// <returns></returns>
        public static long nextLong(this Random ran)
        {
            return ((long)(ran.Next(32)) << 32) + ran.Next(32);
        }

        public static int nextInt(this Random ran)
        {
            return ran.Next(32);
        }

        internal static string ipToIPv4Str(byte[] ip)
        {
            if (ip.Length != 4)
                return null;
            return new StringBuilder().Append(ip[0] & 0xFF).Append(".").Append(
                ip[1] & 0xFF).Append(".").Append(ip[2] & 0xFF)
                .Append(".").Append(ip[3] & 0xFF).ToString();
        }

        public static bool isInternalIP(byte[] ip)
        {
            if (ip.Length != 4)
                throw new RuntimeException("illegal ipv4 bytes");

            //10.0.0.0~10.255.255.255
            //172.16.0.0~172.31.255.255
            //192.168.0.0~192.168.255.255
            if (ip[0] == (byte)10)
            {

                return true;
            }
            else if (ip[0] == (byte)172)
            {
                if (ip[1] >= (byte)16 && ip[1] <= (byte)31)
                {
                    return true;
                }
            }
            else if (ip[0] == (byte)192)
            {
                if (ip[1] == (byte)168)
                {
                    return true;
                }
            }
            return false;
        }

        public static bool isInternalV6IP(IPAddress inetAddr)
        {
            //if (inetAddr.isAnyLocalAddress() // Wild card ipv6
            //    || inetAddr.isLinkLocalAddress() // Single broadcast ipv6 address: fe80:xx:xx...
            //    || inetAddr.isLoopbackAddress() //Loopback ipv6 address
            //    || inetAddr.isSiteLocalAddress())
            //{ // Site local ipv6 address: fec0:xx:xx...
            //    return true;
            //}
            //return false;
            if (IPAddress.IsLoopback(inetAddr) || inetAddr.IsIPv6LinkLocal || inetAddr.IsIPv6LinkLocal)
            {
                return true;
            }
            return false;
        }

        public static byte[] getIP()
        {
            try
            {
                var allNetInterfaces = NetworkInterface.GetAllNetworkInterfaces();
                IPAddress ip = null;
                byte[] internalIP = null;
                //while (allNetInterfaces.hasMoreElements())
                foreach(var networkInterface in allNetInterfaces)
                {
                    foreach (UnicastIPAddressInformation uniIp in networkInterface.GetIPProperties().UnicastAddresses)
                    {
                        ip = uniIp.Address;
                        if (ip != null && ip.AddressFamily == System.Net.Sockets.AddressFamily.InterNetwork) {
                            byte[] ipByte = ip.GetAddressBytes();
                            if (ipByte.Length == 4)
                            {
                                if (IsValideIPv4(ipByte))
                                {
                                    if (!isInternalIP(ipByte))
                                    {
                                        return ipByte;
                                    }
                                    else if (internalIP == null)
                                    {
                                        internalIP = ipByte;
                                    }
                                }
                            }
                        } 
                        else if (ip != null && ip.AddressFamily == System.Net.Sockets.AddressFamily.InterNetworkV6) {
                            byte[] ipByte = ip.GetAddressBytes();
                            if (ipByte.Length == 16)
                            {
                                if (IsValideIPv6(ipByte))
                                {
                                    if (!isInternalV6IP(ip))
                                    {
                                        return ipByte;
                                    }
                                }
                            }
                        }
                    }
                }
                if (internalIP != null)
                {
                    return internalIP;
                }
                else
                {
                    throw new RuntimeException("Can not get local ip");
                }
            }
            catch (Exception e)
            {
                throw new RuntimeException("Can not get local ip", e);
            }
        }

        public static byte[] uncompress(byte[] before)
        {
            try
            {
                if (before == null)
                    return null;
                using MemoryStream ms = new MemoryStream(before);
                using ZipInputStream zipStream = new ZipInputStream(ms);
                zipStream.IsStreamOwner = true;
                var file = zipStream.GetNextEntry();
                var after = new byte[file.Size];
                zipStream.Read(after, 0, after.Length);
                return after;
            }
            catch (Exception e)
            {
                log.Error($"消息解压失败>{e.ToString()}");
            }
            return null;
        }

        public static byte[] compress(byte[] rawData, int zipCompressLevel)
        {
            try
            {
                using (MemoryStream ms = new MemoryStream())
                {
                    using (ZipOutputStream compressedzipStream = new ZipOutputStream(ms))
                    {
                        compressedzipStream.SetLevel(zipCompressLevel);
                        var entry = new ZipEntry("m");
                        entry.Size = rawData.Length;
                        compressedzipStream.PutNextEntry(entry);
                        compressedzipStream.Write(rawData, 0, rawData.Length);
                        compressedzipStream.CloseEntry();
                        compressedzipStream.Close();
                        return ms.ToArray();
                    }
                }
            }
            catch (Exception ex)
            {
                log.Error("数据压缩失败{}", ex.Message);
                return rawData;
            }
        }
    }
}
