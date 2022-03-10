using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RocketMQ.Client
{
    public abstract class RemotingSerializable
    {
        //private readonly static Charset CHARSET_UTF8 = Charset.forName("UTF-8");
        private static readonly System.Text.Encoding CHARSET_UTF8 = System.Text.Encoding.UTF8;

        public static byte[] encode(object obj)
        {
            string json = toJson(obj, false);
            if (json != null)
            {
                return System.Text.Encoding.UTF8.GetBytes(json);
                //return json.getBytes(CHARSET_UTF8);
            }
            return null;
        }

        public byte[] encode()
        {
            string json = this.toJson();
            if (json != null)
            {
                //return json.getBytes(CHARSET_UTF8);
                return System.Text.Encoding.UTF8.GetBytes(json);
            }
            return null;
        }

        public static string toJson(object obj, bool prettyFormat)
        {
            return JSON.toJSONString(obj, prettyFormat);
        }

        //public static T decode<T>(byte[] data, Class<T> classOfT)
        //{
        //    String json = new String(data, CHARSET_UTF8);
        //    return fromJson(json, classOfT);
        //}
        public static T decode<T>(byte[] data)
        {
            //String json = new String(data, CHARSET_UTF8);
            string json = System.Text.Encoding.UTF8.GetString(data);
            return fromJson<T>(json);
        }

        //public static T fromJson<T>(String json, Class<T> classOfT)
        //{
        //    return JSON.parseObject(json, classOfT);
        //}

        public static T fromJson<T>(String json)
        {
            return JSON.parseObject<T>(json);
        }

        public String toJson()
        {
            return toJson(false);
        }

        public String toJson(bool prettyFormat)
        {
            return toJson(this, prettyFormat);
        }
    }
}
