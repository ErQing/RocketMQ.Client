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

        internal static long currentTimeMillis()
        {
            throw new NotImplementedException();
        }
        internal static ulong currentTimeMillisUnsigned()
        {
            throw new NotImplementedException();
        }

        internal static string getenv(string sERIALIZE_TYPE_ENV)
        {
            throw new NotImplementedException();
        }
    }
}
