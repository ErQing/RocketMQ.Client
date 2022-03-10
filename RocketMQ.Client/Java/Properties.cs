using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;

namespace RocketMQ.Client
{
    public class Properties : Dictionary<object, object>
    {

        [MethodImpl(MethodImplOptions.Synchronized)]
        public object setProperty(string key, string value)
        {
            TryGetValue(key, out object old);
            this[key] = value;
            return old;
        }

        public string getProperty(string key)
        {
            throw new NotImplementedException();
        }

        internal void load(object input)
        {
            throw new NotImplementedException();
        }
    }
}
