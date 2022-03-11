using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;

namespace RocketMQ.Client
{
    public class AssignedMessageQueue
    {
        private readonly ConcurrentDictionary<MessageQueue, MessageQueueState> assignedMessageQueueState;

        private RebalanceImpl rebalanceImpl;

        public AssignedMessageQueue()
        {
            assignedMessageQueueState = new ConcurrentDictionary<MessageQueue, MessageQueueState>();
        }

        public void setRebalanceImpl(RebalanceImpl rebalanceImpl)
        {
            this.rebalanceImpl = rebalanceImpl;
        }

        public HashSet<MessageQueue> messageQueues()
        {
            return assignedMessageQueueState.Keys.ToHashSet();
        }

        public bool isPaused(MessageQueue messageQueue)
        {
            //MessageQueueState messageQueueState = assignedMessageQueueState.get(messageQueue);
            assignedMessageQueueState.TryGetValue(messageQueue, out MessageQueueState messageQueueState);
            if (messageQueueState != null)
            {
                return messageQueueState.isPaused();
            }
            return true;
        }

        public void pause(ICollection<MessageQueue> messageQueues)
        {
            foreach (MessageQueue messageQueue in messageQueues)
            {
                MessageQueueState messageQueueState = assignedMessageQueueState.Get(messageQueue);
                if (assignedMessageQueueState.Get(messageQueue) != null)  //??? 为什么不直接使用messageQueueState变量？
                {
                    messageQueueState.setPaused(true);
                }
            }
        }

        public void resume(ICollection<MessageQueue> messageQueueCollection)
        {
            foreach (MessageQueue messageQueue in messageQueueCollection)
            {
                MessageQueueState messageQueueState = assignedMessageQueueState.Get(messageQueue);
                if (assignedMessageQueueState.Get(messageQueue) != null) //??? 为什么不直接使用messageQueueState变量？
                {
                    messageQueueState.setPaused(false);
                }
            }
        }

        public ProcessQueue getProcessQueue(MessageQueue messageQueue)
        {
            MessageQueueState messageQueueState = assignedMessageQueueState.Get(messageQueue);
            if (messageQueueState != null)
            {
                return messageQueueState.getProcessQueue();
            }
            return null;
        }

        public long getPullOffset(MessageQueue messageQueue)
        {
            MessageQueueState messageQueueState = assignedMessageQueueState.Get(messageQueue);
            if (messageQueueState != null)
            {
                return messageQueueState.getPullOffset();
            }
            return -1;
        }

        public void updatePullOffset(MessageQueue messageQueue, long offset, ProcessQueue processQueue)
        {
            //MessageQueueState messageQueueState = assignedMessageQueueState.get(messageQueue);
            assignedMessageQueueState.TryGetValue(messageQueue, out MessageQueueState messageQueueState);
            if (messageQueueState != null)
            {
                if (messageQueueState.getProcessQueue() != processQueue)
                {
                    return;
                }
                messageQueueState.setPullOffset(offset);
            }
        }

        public long getConsumerOffset(MessageQueue messageQueue)
        {
            //MessageQueueState messageQueueState = assignedMessageQueueState.get(messageQueue);
            assignedMessageQueueState.TryGetValue(messageQueue, out MessageQueueState messageQueueState);
            if (messageQueueState != null)
            {
                return messageQueueState.getConsumeOffset();
            }
            return -1;
        }

        public void updateConsumeOffset(MessageQueue messageQueue, long offset)
        {
            //MessageQueueState messageQueueState = assignedMessageQueueState.get(messageQueue);
            assignedMessageQueueState.TryGetValue(messageQueue, out MessageQueueState messageQueueState);
            if (messageQueueState != null)
            {
                messageQueueState.setConsumeOffset(offset);
            }
        }

        public void setSeekOffset(MessageQueue messageQueue, long offset)
        {
            //MessageQueueState messageQueueState = assignedMessageQueueState.get(messageQueue);
            assignedMessageQueueState.TryGetValue(messageQueue, out MessageQueueState messageQueueState);
            if (messageQueueState != null)
            {
                messageQueueState.setSeekOffset(offset);
            }
        }

        public long getSeekOffset(MessageQueue messageQueue)
        {
            //MessageQueueState messageQueueState = assignedMessageQueueState.get(messageQueue);
            assignedMessageQueueState.TryGetValue(messageQueue, out MessageQueueState messageQueueState);
            if (messageQueueState != null)
            {
                return messageQueueState.getSeekOffset();
            }
            return -1;
        }

        public void updateAssignedMessageQueue(String topic, ICollection<MessageQueue> assigned)
        {
            lock (this.assignedMessageQueueState)
            {
                //Iterator<Map.Entry<MessageQueue, MessageQueueState>> it = this.assignedMessageQueueState.entrySet().iterator();
                //while (it.hasNext())
                //{
                //    Map.Entry<MessageQueue, MessageQueueState> next = it.next();
                //    if (next.getKey().getTopic().equals(topic))
                //    {
                //        if (!assigned.contains(next.getKey()))
                //        {
                //            next.getValue().getProcessQueue().setDropped(true);
                //            it.remove();
                //        }
                //    }
                //}
                //addAssignedMessageQueue(assigned);

                //这种遍历方式是否降低了效率？
                var keys = this.assignedMessageQueueState.Keys.ToList();
                foreach (var key in keys)
                {
                    if (key.getTopic().Equals(topic))
                    {
                        if (!assigned.Contains(key))
                        {
                            assignedMessageQueueState[key].getProcessQueue().setDropped(true);
                            assignedMessageQueueState.Remove(key, out _);
                        }
                    }
                }
                addAssignedMessageQueue(assigned);
            }
        }

        public void updateAssignedMessageQueue(ICollection<MessageQueue> assigned)
        {
            lock (this.assignedMessageQueueState)
            {
                //Iterator<Map.Entry<MessageQueue, MessageQueueState>> it = this.assignedMessageQueueState.entrySet().iterator();
                //while (it.hasNext())
                //{
                //    Map.Entry<MessageQueue, MessageQueueState> next = it.next();
                //    if (!assigned.contains(next.getKey()))
                //    {
                //        next.getValue().getProcessQueue().setDropped(true);
                //        it.remove();
                //    }
                //}
                //addAssignedMessageQueue(assigned);

                //这种遍历方式是否降低了效率？
                var keys = this.assignedMessageQueueState.Keys.ToList();
                foreach (var key in keys)
                {
                    if (!assigned.Contains(key))
                    {
                        assignedMessageQueueState[key].getProcessQueue().setDropped(true);
                        assignedMessageQueueState.Remove(key, out _);
                    }
                }
                addAssignedMessageQueue(assigned);
            }
        }

        private void addAssignedMessageQueue(ICollection<MessageQueue> assigned)
        {
            foreach (MessageQueue messageQueue in assigned)
            {
                if (!this.assignedMessageQueueState.ContainsKey(messageQueue))
                {
                    MessageQueueState messageQueueState;
                    //if (rebalanceImpl != null && rebalanceImpl.getProcessQueueTable().get(messageQueue) != null)
                    if (rebalanceImpl != null && rebalanceImpl.getProcessQueueTable().TryGetValue(messageQueue, out ProcessQueue pQueue))
                    {
                        messageQueueState = new MessageQueueState(messageQueue, pQueue);
                    }
                    else
                    {
                        ProcessQueue processQueue = new ProcessQueue();
                        messageQueueState = new MessageQueueState(messageQueue, processQueue);
                    }
                    //this.assignedMessageQueueState.put(messageQueue, messageQueueState);
                    this.assignedMessageQueueState[messageQueue] = messageQueueState;
                }
            }
        }

        public void removeAssignedMessageQueue(String topic)
        {
            lock (this.assignedMessageQueueState)
            {
                //Iterator<Map.Entry<MessageQueue, MessageQueueState>> it = this.assignedMessageQueueState.entrySet().iterator();
                //while (it.hasNext())
                //{
                //    Map.Entry<MessageQueue, MessageQueueState> next = it.next();
                //    if (next.getKey().getTopic().equals(topic))
                //    {
                //        it.remove();
                //    }
                //}
                var keys = this.assignedMessageQueueState.Keys.ToList();
                foreach (var key in keys)
                {
                    if (key.getTopic().Equals(topic))
                    {
                        assignedMessageQueueState.Remove(key, out _);
                    }
                }
            }
        }

        public HashSet<MessageQueue> getAssignedMessageQueues()
        {
            return this.assignedMessageQueueState.Keys.ToHashSet();
        }

        class MessageQueueState
        {
            private MessageQueue messageQueue;
            private ProcessQueue processQueue;
            private volatile bool paused = false;
            private /*volatile*/ long pullOffset = -1;
            private /*volatile*/ long consumeOffset = -1;
            private /*volatile*/ long seekOffset = -1;

            public MessageQueueState(MessageQueue messageQueue, ProcessQueue processQueue)
            {
                this.messageQueue = messageQueue;
                this.processQueue = processQueue;
            }

            public MessageQueue getMessageQueue()
            {
                return messageQueue;
            }

            public void setMessageQueue(MessageQueue messageQueue)
            {
                this.messageQueue = messageQueue;
            }

            public bool isPaused()
            {
                return paused;
            }

            public void setPaused(bool paused)
            {
                this.paused = paused;
            }

            public long getPullOffset()
            {
                return Volatile.Read(ref pullOffset);
                //return pullOffset;
            }

            public void setPullOffset(long offset)
            {
                //this.pullOffset = pullOffset;
                Volatile.Write(ref pullOffset, offset);
            }

            public ProcessQueue getProcessQueue()
            {
                return processQueue;
            }

            public void setProcessQueue(ProcessQueue processQueue)
            {
                this.processQueue = processQueue;
            }

            public long getConsumeOffset()
            {
                //return consumeOffset;
                return Volatile.Read(ref consumeOffset);
            }

            public void setConsumeOffset(long offset)
            {
                //this.consumeOffset = consumeOffset;
                Volatile.Write(ref consumeOffset, offset);
            }

            public long getSeekOffset()
            {
                //return seekOffset;
                return Volatile.Read(ref seekOffset);
            }

            public void setSeekOffset(long offset)
            {
                //this.seekOffset = seekOffset;
                Volatile.Write(ref seekOffset, offset);
            }
        }
    }
}
