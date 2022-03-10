using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;

namespace RocketMQ.Client
{
    public interface MQProducer
    {

        /// <summary>
        /// 
        /// </summary>
        /// <exception cref="MQClientException"/>
        void start();

        void shutdown();

        List<MessageQueue> fetchPublishMessageQueues(String topic);

        SendResult send(Message msg);

        SendResult send(Message msg, long timeout);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="msg"></param>
        /// <param name="sendCallback"></param>
        ///  <exception cref="ThreadInterruptedException"/>
        void send(Message msg, SendCallback sendCallback);

        void send(Message msg, SendCallback sendCallback, long timeout);

        void sendOneway(Message msg);

        SendResult send(Message msg, MessageQueue mq);

        SendResult send(Message msg, MessageQueue mq, long timeout);

        void send(Message msg, MessageQueue mq, SendCallback sendCallback);

        void send(Message msg, MessageQueue mq, SendCallback sendCallback, long timeout);

        void sendOneway(Message msg, MessageQueue mq);

        SendResult send(Message msg, MessageQueueSelector selector, Object arg);

        SendResult send(Message msg, MessageQueueSelector selector, Object arg, long timeout);

        void send(Message msg, MessageQueueSelector selector, Object arg, SendCallback sendCallback) ;

        void send(Message msg, MessageQueueSelector selector, Object arg, SendCallback sendCallback, long timeout);

        void sendOneway(Message msg, MessageQueueSelector selector, Object arg);

        TransactionSendResult sendMessageInTransaction(Message msg, LocalTransactionExecuter tranExecuter, object arg) ;

        TransactionSendResult sendMessageInTransaction(Message msg, Object arg);

        //for batch
        SendResult send(ICollection<Message> msgs);

        SendResult send(ICollection<Message> msgs, long timeout);

        SendResult send(ICollection<Message> msgs, MessageQueue mq);

        SendResult send(ICollection<Message> msgs, MessageQueue mq, long timeout);

        void send(ICollection<Message> msgs, SendCallback sendCallback);

        void send(ICollection<Message> msgs, SendCallback sendCallback, long timeout);

        void send(ICollection<Message> msgs, MessageQueue mq, SendCallback sendCallback);

        void send(ICollection<Message> msgs, MessageQueue mq, SendCallback sendCallback, long timeout);

        //for rpc
        Message request(Message msg, long timeout);

        void request(Message msg, RequestCallback requestCallback, long timeout);

        Message request(Message msg, MessageQueueSelector selector, Object arg, long timeout);

        void request(Message msg, MessageQueueSelector selector, Object arg, RequestCallback requestCallback, long timeout);

        Message request(Message msg, MessageQueue mq, long timeout);

        void request(Message msg, MessageQueue mq, RequestCallback requestCallback, long timeout);
    }
}
