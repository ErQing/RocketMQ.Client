using System;

namespace RocketMQ.Client
{
    public class PullRequest
    {
        private string consumerGroup;
        private MessageQueue messageQueue;
        private ProcessQueue processQueue;
        private long nextOffset;
        private bool previouslyLocked = false;

        public bool isPreviouslyLocked()
        {
            return previouslyLocked;
        }

        public void setPreviouslyLocked(bool previouslyLocked)
        {
            this.previouslyLocked = previouslyLocked;
        }

        public string getConsumerGroup()
        {
            return consumerGroup;
        }

        public void setConsumerGroup(String consumerGroup)
        {
            this.consumerGroup = consumerGroup;
        }

        public MessageQueue getMessageQueue()
        {
            return messageQueue;
        }

        public void setMessageQueue(MessageQueue messageQueue)
        {
            this.messageQueue = messageQueue;
        }

        public long getNextOffset()
        {
            return nextOffset;
        }

        public void setNextOffset(long nextOffset)
        {
            this.nextOffset = nextOffset;
        }

        public override int GetHashCode()
        {
            int prime = 31;
            int result = 1;
            result = prime * result + ((consumerGroup == null) ? 0 : consumerGroup.GetHashCode());
            result = prime * result + ((messageQueue == null) ? 0 : messageQueue.GetHashCode());
            return result;
        }

        public override bool Equals(Object obj)
        {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (GetType() != obj.GetType())
                return false;
            PullRequest other = (PullRequest)obj;
            if (consumerGroup == null)
            {
                if (other.consumerGroup != null)
                    return false;
            }
            else if (!consumerGroup.Equals(other.consumerGroup))
                return false;
            if (messageQueue == null)
            {
                if (other.messageQueue != null)
                    return false;
            }
            else if (!messageQueue.Equals(other.messageQueue))
                return false;
            return true;
        }

        public override string ToString()
        {
            return "PullRequest [consumerGroup=" + consumerGroup + ", messageQueue=" + messageQueue
                + ", nextOffset=" + nextOffset + "]";
        }

        public ProcessQueue getProcessQueue()
        {
            return processQueue;
        }

        public void setProcessQueue(ProcessQueue processQueue)
        {
            this.processQueue = processQueue;
        }
    }

}
