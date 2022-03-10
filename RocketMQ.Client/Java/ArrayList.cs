using System.Collections.Generic;

namespace RocketMQ.Client
{
    public class ArrayList<T> : List<T>
    {
        public ArrayList()
        { 
        }

        public ArrayList(int capacity) : base(capacity)
        {
        }
    }
}
