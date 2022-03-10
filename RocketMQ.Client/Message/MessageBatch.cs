using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;

namespace RocketMQ.Client
{
    public class MessageBatch : Message, IEnumerable<Message>
    {
        //private static readonly long serialVersionUID = 621335151046335557L;
        private readonly List<Message> messages;

        private MessageBatch(List<Message> messages)
        {
            this.messages = messages;
        }

        public byte[] encode()
        {
            return MessageDecoder.encodeMessages(messages);
        }

        public static MessageBatch generateFromList(ICollection<Message> messages)
        {
            //assert messages != null;
            //assert messages.size() > 0;
            Debug.Assert(messages != null);
            Debug.Assert(messages.Count > 0);
            List<Message> messageList = new List<Message>(messages.Count);
            Message first = null;
            foreach (Message message in messages)
            {
                if (message.getDelayTimeLevel() > 0)
                {
                    
                    //throw new UnsupportedOperationException("TimeDelayLevel is not supported for batching");
                    throw new InvalidOperationException("TimeDelayLevel is not supported for batching");
                }
                if (message.getTopic().StartsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX))
                {
                    throw new InvalidOperationException("Retry Group is not supported for batching");
                }
                if (first == null)
                {
                    first = message;
                }
                else
                {
                    if (!first.getTopic().Equals(message.getTopic()))
                    {
                        throw new InvalidOperationException("The topic of the messages in one batch should be the same");
                    }
                    if (first.isWaitStoreMsgOK() != message.isWaitStoreMsgOK())
                    {
                        throw new InvalidOperationException("The waitStoreMsgOK of the messages in one batch should the same");
                    }
                }
                messageList.Add(message);
            }
            MessageBatch messageBatch = new MessageBatch(messageList);

            messageBatch.setTopic(first.getTopic());
            messageBatch.setWaitStoreMsgOK(first.isWaitStoreMsgOK());
            return messageBatch;
        }

        public IEnumerator<Message> GetEnumerator()
        {
            return messages.GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return messages.GetEnumerator();
        }
    }
}
