using System;
using System.Collections.Generic;

namespace RocketMQ.Client
{
    public class TopicPublishInfo
    {
        private bool orderTopic = false;
        private bool haveTopicRouterInfo = false;
        private List<MessageQueue> messageQueueList = new List<MessageQueue>();
        private volatile ThreadLocalIndex sendWhichQueue = new ThreadLocalIndex();
        private TopicRouteData topicRouteData;

        public bool isOrderTopic()
        {
            return orderTopic;
        }

        public void setOrderTopic(bool orderTopic)
        {
            this.orderTopic = orderTopic;
        }

        public bool ok()
        {
            return null != this.messageQueueList && this.messageQueueList.Count > 0;
        }

        public List<MessageQueue> getMessageQueueList()
        {
            return messageQueueList;
        }

        public void setMessageQueueList(List<MessageQueue> messageQueueList)
        {
            this.messageQueueList = messageQueueList;
        }

        public ThreadLocalIndex getSendWhichQueue()
        {
            return sendWhichQueue;
        }

        public void setSendWhichQueue(ThreadLocalIndex sendWhichQueue)
        {
            this.sendWhichQueue = sendWhichQueue;
        }

        public bool isHaveTopicRouterInfo()
        {
            return haveTopicRouterInfo;
        }

        public void setHaveTopicRouterInfo(bool haveTopicRouterInfo)
        {
            this.haveTopicRouterInfo = haveTopicRouterInfo;
        }

        public MessageQueue selectOneMessageQueue(string lastBrokerName)
        {
            if (lastBrokerName == null)
            {
                return selectOneMessageQueue();
            }
            else
            {
                for (int i = 0; i < this.messageQueueList.Count; i++)
                {
                    int index = this.sendWhichQueue.incrementAndGet();
                    int pos = Math.Abs(index) % this.messageQueueList.Count;
                    if (pos < 0)
                        pos = 0;
                    MessageQueue mq = this.messageQueueList[pos];
                    if (!mq.getBrokerName().Equals(lastBrokerName))
                    {
                        return mq;
                    }
                }
                return selectOneMessageQueue();
            }
        }

        public MessageQueue selectOneMessageQueue()
        {
            int index = this.sendWhichQueue.incrementAndGet();
            int pos = Math.Abs(index) % this.messageQueueList.Count;
            if (pos < 0)
                pos = 0;
            return this.messageQueueList[pos];
        }

        public int getQueueIdByBroker(string brokerName)
        {
            for (int i = 0; i < topicRouteData.queueDatas.Count; i++)
            {
                QueueData queueData = this.topicRouteData.queueDatas[i];
                if (queueData.brokerName.Equals(brokerName))
                {
                    return queueData.writeQueueNums;
                }
            }

            return -1;
        }

        public override String ToString()
        {
            return "TopicPublishInfo [orderTopic=" + orderTopic + ", messageQueueList=" + messageQueueList
                + ", sendWhichQueue=" + sendWhichQueue + ", haveTopicRouterInfo=" + haveTopicRouterInfo + "]";
        }

        public TopicRouteData getTopicRouteData()
        {
            return topicRouteData;
        }

        public void setTopicRouteData(TopicRouteData topicRouteData)
        {
            this.topicRouteData = topicRouteData;
        }
    }
}
