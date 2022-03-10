using System;
using System.Collections.Generic;

namespace RocketMQ.Client
{
    public class AllocateMessageQueueAveragely : AllocateMessageQueueStrategy
    {
        //private final InternalLogger log = ClientLogger.getLog();
        static NLog.Logger log = NLog.LogManager.GetCurrentClassLogger();

        public List<MessageQueue> allocate(String consumerGroup, String currentCID, List<MessageQueue> mqAll, List<String> cidAll)
        {
            if (currentCID == null || currentCID.Length < 1)
            {
                throw new ArgumentException("currentCID is empty");
            }
            if (mqAll == null || mqAll.Count <= 0)
            {
                throw new ArgumentException("mqAll is null or mqAll empty");
            }
            if (cidAll == null || cidAll.Count <= 0)
            {
                throw new ArgumentException("cidAll is null or cidAll empty");
            }

            List<MessageQueue> result = new List<MessageQueue>();
            if (!cidAll.Contains(currentCID))
            {
                log.Info("[BUG] ConsumerGroup: {} The consumerId: {} not in cidAll: {}",
                    consumerGroup,
                    currentCID,
                    cidAll);
                return result;
            }

            int index = cidAll.IndexOf(currentCID);
            int mod = mqAll.Count % cidAll.Count;
            int averageSize =
                mqAll.Count <= cidAll.Count ? 1 : (mod > 0 && index < mod ? mqAll.Count / cidAll.Count
                    + 1 : mqAll.Count / cidAll.Count);
            int startIndex = (mod > 0 && index < mod) ? index * averageSize : index * averageSize + mod;
            int range = Math.Min(averageSize, mqAll.Count - startIndex);
            for (int i = 0; i < range; i++)
            {
                //result.Add(mqAll.get((startIndex + i) % mqAll.Count));
                result.Add(mqAll[(startIndex + i) % mqAll.Count]);
            }
            return result;
        }

        public string getName()
        {
            return "AVG";
        }
    }
}
