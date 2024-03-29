﻿using System;
using System.Text;

namespace RocketMQ.Client
{
    public static class Str
    {

        public static string valueOf(long val)
        {
            return val.ToString();
        }

        public static string valueOf(int val)
        {
            return val.ToString();
        }

        public static int length(this string str)
        {
            return str.Length;
        }

        public static T ToEnum<T>(this string value, T defaultValue) where T : struct
        {
            if (string.IsNullOrEmpty(value))
                return defaultValue;
            return Enum.TryParse(value, true, out T result) ? result : defaultValue;
        }

        /// <summary>
        /// str==null or str.length()==0
        /// </summary>
        /// <param name="str"></param>
        /// <returns></returns>
        public static bool isEmpty(this string str)
        {
            return string.IsNullOrEmpty(str);
        }

        /// <summary>
        /// str==null or str.length()==0 or str = "   "
        /// </summary>
        /// <param name="str"></param>
        /// <returns></returns>
        public static bool isBlank(string str)
        {
            return string.IsNullOrWhiteSpace(str);
        }

        public static bool isNotEmpty(string str)
        {
            return !isEmpty(str);
        }

        public static byte[] getBytes(this string str, Encoding charSet)
        {
            return charSet.GetBytes(str);
        }

        public static byte[] getBytes(this string str)
        {
            return System.Text.Encoding.UTF8.GetBytes(str);
        }

        public static StringBuilder deleteCharAt(this StringBuilder sb, int index)
        {
            sb.Remove(index, 1);
            return sb;
        }

    }
}
