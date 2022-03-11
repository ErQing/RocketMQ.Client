using System;
using System.Runtime.CompilerServices;

namespace RocketMQ.Client
{
    public class MessageClientIDSetter
    {
        private static readonly string TOPIC_KEY_SPLITTER = "#";
        private static readonly int LEN;
        private static readonly char[] FIX_STRING;
        private static readonly AtomicInteger COUNTER;
        private static long startTime;
        private static long nextStartTime;

        static MessageClientIDSetter()
        {
            byte[] ip;
            try
            {
                ip = UtilAll.getIP();
            }
            catch (Exception e)
            {
                ip = createFakeIP();
            }
            LEN = ip.Length + 2 + 4 + 4 + 2;
            ByteBuffer tempBuffer = ByteBuffer.allocate(ip.Length + 2 + 4);
            tempBuffer.put(ip);
            tempBuffer.putShort((short)UtilAll.getPid());
            //tempBuffer.putInt(MessageClientIDSetter./*class.*/getClassLoader().hashCode());
            tempBuffer.putInt(typeof(MessageClientIDSetter).GetHashCode());
            FIX_STRING = UtilAll.bytes2string(tempBuffer.array()).ToCharArray();
            setStartTime(Sys.currentTimeMillis());
            COUNTER = new AtomicInteger(0);
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        private static void setStartTime(long millis)
        {
            //Calendar cal = Calendar.getInstance();
            //cal.setTimeInMillis(millis);
            //cal.set(Calendar.DAY_OF_MONTH, 1);
            //cal.set(Calendar.HOUR_OF_DAY, 0);
            //cal.set(Calendar.MINUTE, 0);
            //cal.set(Calendar.SECOND, 0);
            //cal.set(Calendar.MILLISECOND, 0);
            //startTime = cal.getTimeInMillis();
            //cal.Add(Calendar.MONTH, 1);
            //nextStartTime = cal.getTimeInMillis();
            DateTime dt = new DateTime(millis);
            DateTime dt2 = new DateTime(dt.Year, dt.Month, 1, 0, 0, 0, 0);
            startTime = dt2.Ticks;
            dt2.AddMonths(1);
            nextStartTime = dt2.Ticks;
        }

        public static long getNearlyTimeFromID(string msgID)
        {
            ByteBuffer buf = ByteBuffer.allocate(8);
            byte[] bytes = UtilAll.string2bytes(msgID);
            int ipLength = bytes.Length == 28 ? 16 : 4;
            buf.put((byte)0);
            buf.put((byte)0);
            buf.put((byte)0);
            buf.put((byte)0);
            buf.put(bytes, ipLength + 2 + 4, 4);
            buf.ResetPosition(0);
            long spanMS = buf.getLong();

            //Calendar cal = Calendar.getInstance();
            //long now = cal.getTimeInMillis();
            //cal.set(Calendar.DAY_OF_MONTH, 1);
            //cal.set(Calendar.HOUR_OF_DAY, 0);
            //cal.set(Calendar.MINUTE, 0);
            //cal.set(Calendar.SECOND, 0);
            //cal.set(Calendar.MILLISECOND, 0);
            //long monStartTime = cal.getTimeInMillis();
            //if (monStartTime + spanMS >= now)
            //{
            //    cal.Add(Calendar.MONTH, -1);
            //    monStartTime = cal.getTimeInMillis();
            //}
            //cal.setTimeInMillis(monStartTime + spanMS);

            DateTime now = DateTime.Now;
            DateTime dt = new DateTime(now.Year, now.Month, 1, 0, 0, 0, 0);
            long monStartTime = dt.Ticks;
            var ts = TimeSpan.FromMilliseconds(spanMS);
            if (dt + ts >= now)
                dt.AddMonths(-1);
            dt += TimeSpan.FromMilliseconds(spanMS);
            return dt.Ticks;
        }

        public static string getIPStrFromID(string msgID)
        {
            byte[] ipBytes = getIPFromID(msgID);
            if (ipBytes.Length == 16)
            {
                return UtilAll.ipToIPv6Str(ipBytes);
            }
            else
            {
                return UtilAll.ipToIPv4Str(ipBytes);
            }
        }

        public static byte[] getIPFromID(String msgID)
        {
            byte[] bytes = UtilAll.string2bytes(msgID);
            int ipLength = bytes.Length == 28 ? 16 : 4;
            byte[] result = new byte[ipLength];
            Array.Copy(bytes, 0, result, 0, ipLength);
            return result;
        }

        public static int getPidFromID(String msgID)
        {
            byte[] bytes = UtilAll.string2bytes(msgID);
            ByteBuffer wrap = ByteBuffer.wrap(bytes);
            int value = wrap.getShort(bytes.Length - 2 - 4 - 4 - 2);
            return value & 0x0000FFFF;
        }

        public static string createUniqID()
        {
            char[] sb = new char[LEN * 2];
            Array.Copy(FIX_STRING, 0, sb, 0, FIX_STRING.Length);  //???
            long current = Sys.currentTimeMillis();
            if (current >= nextStartTime)
            {
                setStartTime(current);
            }
            int diff = (int)(current - startTime);
            if (diff < 0 && diff > -1000_000)
            {
                // may cause by NTP
                diff = 0;
            }
            int pos = FIX_STRING.Length;
            UtilAll.writeInt(sb, pos, diff);
            pos += 8;
            UtilAll.writeShort(sb, pos, COUNTER.getAndIncrement());
            return new String(sb);
        }

        public static void setUniqID(Message msg)
        {
            if (msg.getProperty(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX) == null)
            {
                msg.putProperty(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX, createUniqID());
            }
        }

        public static string getUniqID(Message msg)
        {
            return msg.getProperty(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX);
        }

        public static byte[] createFakeIP()
        {
            ByteBuffer bb = ByteBuffer.allocate(8);
            bb.putLong(Sys.currentTimeMillis());
            bb.ResetPosition(4);
            byte[] fakeIP = new byte[4];
            bb.get(fakeIP);
            return fakeIP;
        }

    }
}
