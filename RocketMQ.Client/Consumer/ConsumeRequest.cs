//using System;
//using System.Collections.Generic;
//using System.Linq;
//using System.Text;
//using System.Threading.Tasks;

//namespace RocketMQ.Client
//{
//    public class ConsumeRequest
//    {
//        private readonly List<MessageExt> messageExts;
//        private readonly MessageQueue messageQueue;
//        private readonly ProcessQueue processQueue;

//        public ConsumeRequest(List<MessageExt> messageExts, MessageQueue messageQueue, ProcessQueue processQueue)
//        {
//            this.messageExts = messageExts;
//            this.messageQueue = messageQueue;
//            this.processQueue = processQueue;
//        }

//        public List<MessageExt> getMessageExts()
//        {
//            return messageExts;
//        }

//        public MessageQueue getMessageQueue()
//        {
//            return messageQueue;
//        }

//        public ProcessQueue getProcessQueue()
//        {
//            return processQueue;
//        }
//    }
//}
