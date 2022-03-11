using System;

namespace RocketMQ.Client
{
    public class Sys
    {
        public static string getProperty(string key, string defaultVal)
        {
            throw new NotImplementedException();
        }

        public static string getProperty(string key)
        {
            return getProperty(key, null);
        }

        public static void setProperty(string key, string val)
        {
            throw new NotImplementedException();
        }

        public static long currentTimeMillis()
        {
            return TimeUtils.CurrentTimeMillisUTC();
        }

        public static string getenv(string sERIALIZE_TYPE_ENV)
        {
            throw new NotImplementedException();
        }

    }
}
