using System.Collections.Concurrent;

namespace RocketMQ.Client
{
    public class MessageQueueLock
    {
        private ConcurrentDictionary<MessageQueue, object> mqLockTable = new ConcurrentDictionary<MessageQueue, object>();

        public object fetchLockObject(MessageQueue mq)
        {
            mqLockTable.TryGetValue(mq, out object objLock);
            if (null == objLock)
            {
                objLock = new object();
                object prevLock = this.mqLockTable.TryAdd(mq, objLock);
                if (prevLock != null)
                {
                    objLock = prevLock;
                }
            }
            return objLock;
        }
    }
}
