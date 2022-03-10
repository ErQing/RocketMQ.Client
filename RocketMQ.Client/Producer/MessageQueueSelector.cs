using System;
using System.Collections.Generic;

namespace RocketMQ.Client
{
    //public interface MessageQueueSelector
    //{
    //    MessageQueue select(List<MessageQueue> mqs, Message msg, object arg);
    //}


    public class MessageQueueSelector
    {
        public Func<List<MessageQueue>, Message, object, MessageQueue> Select { get; set; }
    }

}
