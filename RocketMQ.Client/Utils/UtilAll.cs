using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Runtime.InteropServices;

namespace RocketMQ.Client
{
    public static class UtilAll
    {

        public static readonly String YYYY_MM_DD_HH_MM_SS = "yyyy-MM-dd HH:mm:ss";
        public static readonly String YYYY_MM_DD_HH_MM_SS_SSS = "yyyy-MM-dd#HH:mm:ss:SSS";
        public static readonly String YYYYMMDDHHMMSS = "yyyyMMddHHmmss";

        internal static string formatDate(DateTime dateTime, string yYYY_MM_DD_HH_MM_SS_SSS)
        {
            throw new NotImplementedException();
        }

        readonly static char[] HEX_ARRAY = "0123456789ABCDEF".ToCharArray();
        //readonly static String HOST_NAME = ManagementFactory.getRuntimeMXBean().getName(); // format: "pid@hostname"

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
        public static string timeMillisToHumanString3(long t)
        {
            return null;
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

        public static String timeMillisToHumanString(long t)
        {
            return null;
            //Calendar cal = Calendar.getInstance();
            //cal.setTimeInMillis(t);
            //return String.format("%04d%02d%02d%02d%02d%02d%03d", cal.get(Calendar.YEAR), cal.get(Calendar.MONTH) + 1,
            //    cal.get(Calendar.DAY_OF_MONTH), cal.get(Calendar.HOUR_OF_DAY), cal.get(Calendar.MINUTE), cal.get(Calendar.SECOND),
            //    cal.get(Calendar.MILLISECOND));
        }

        public static DateTime parseDate(String date, String pattern)
        {
            throw new NotImplementedException();
            //SimpleDateFormat df = new SimpleDateFormat(pattern);
            //try
            //{
            //    return df.parse(date);
            //}
            //catch (ParseException e)
            //{
            //    return null;
            //}
        }


        internal static byte[] string2bytes(string msgID)
        {
            throw new NotImplementedException();
        }

        internal static string ipToIPv6Str(byte[] ipBytes)
        {
            throw new NotImplementedException();
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

        internal static int crc32(byte[] classBody)
        {
            throw new NotImplementedException();
        }

        internal static string join(List<string> list, string v)
        {
            throw new NotImplementedException();
        }

        internal static long computeNextMinutesTimeMillis()
        {
            throw new NotImplementedException();
        }

        public static string name(this Enum col)
        {
            return col.ToString();
        }

        internal static byte[] compress(byte[] body, int zipCompressLevel)
        {
            throw new NotImplementedException();
        }

        internal static long computeNextHourTimeMillis()
        {
            throw new NotImplementedException();
        }

        internal static void writeInt(char[] sb, int pos, int diff)
        {
            throw new NotImplementedException();
        }

        internal static void writeShort(char[] sb, int pos, int v)
        {
            throw new NotImplementedException();
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

        internal static long computeNextMorningTimeMillis()
        {
            throw new NotImplementedException();
        }

        public static int nextInt(this Random ran)
        {
            return ran.Next(32);
        }

        internal static string ipToIPv4Str(object p)
        {
            throw new NotImplementedException();
        }

        internal static byte[] getIP()
        {
            throw new NotImplementedException();
        }

        internal static byte[] uncompress(byte[] body)
        {
            throw new NotImplementedException();
        }
    }
}
