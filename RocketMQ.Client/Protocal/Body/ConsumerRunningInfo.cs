using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RocketMQ.Client
{
    public class ConsumerRunningInfo : RemotingSerializable
    {
        public static readonly String PROP_NAMESERVER_ADDR  = "PROP_NAMESERVER_ADDR";
        public static readonly String PROP_THREADPOOL_CORE_SIZE = "PROP_THREADPOOL_CORE_SIZE";
        public static readonly String PROP_CONSUME_ORDERLY = "PROP_CONSUMEORDERLY";
        public static readonly String PROP_CONSUME_TYPE = "PROP_CONSUME_TYPE";
        public static readonly String PROP_CLIENT_VERSION = "PROP_CLIENT_VERSION";
        public static readonly String PROP_CONSUMER_START_TIMESTAMP = "PROP_CONSUMER_START_TIMESTAMP";

        public Properties properties { get; set; } = new Properties();
        public TreeSet<SubscriptionData> subscriptionSet { get; set; }
        public TreeMap<MessageQueue, ProcessQueueInfo> mqTable { get; set; }
        public TreeMap<String/* Topic */, ConsumeStatus> statusTable { get; set; }
        public String jstack { get; set; }

        public static bool analyzeSubscription(TreeMap<String/* clientId */, ConsumerRunningInfo> criTable)
        {
            ConsumerRunningInfo prev = criTable.First().Value;
            bool push = false;
            {
                String property = prev.properties.getProperty(ConsumerRunningInfo.PROP_CONSUME_TYPE);

                if (property == null)
                {
                    property = ((ConsumeType)prev.properties.get(ConsumerRunningInfo.PROP_CONSUME_TYPE)).name();
                }
                //push = ConsumeType.valueOf(property) == ConsumeType.CONSUME_PASSIVELY;
                push = property.ToEnum<ConsumeType>(ConsumeType.UNKNOWN) == ConsumeType.CONSUME_PASSIVELY;
            }

            bool startForAWhile = false;
            {

                String property = prev.properties.getProperty(ConsumerRunningInfo.PROP_CONSUMER_START_TIMESTAMP);
                if (property == null)
                {
                    //property = String.valueOf(prev.properties.get(ConsumerRunningInfo.PROP_CONSUMER_START_TIMESTAMP));
                    property = prev.properties.get(ConsumerRunningInfo.PROP_CONSUMER_START_TIMESTAMP).ToString();
                }
                startForAWhile = (Sys.currentTimeMillis() - long.Parse(property)) > (1000 * 60 * 2);
            }

            if (push && startForAWhile)
            {

                {
                    //Iterator<Entry<String, ConsumerRunningInfo>> it = criTable.entrySet().iterator();
                    //while (it.hasNext())
                    foreach(var entry in criTable)
                    {
                        //Entry<String, ConsumerRunningInfo> next = it.next();
                        ConsumerRunningInfo current = entry.Value;
                        bool equals = current.subscriptionSet.Equals(prev.subscriptionSet);
                        if (!equals)
                        {
                            // Different subscription in the same group of consumer
                            return false;
                        }
                        prev = entry.Value;
                    }

                    if (prev != null)
                    {

                        if (prev.subscriptionSet.isEmpty())
                        {
                            // Subscription empty!
                            return false;
                        }
                    }
                }
            }

            return true;
        }

        public static bool analyzeRebalance(TreeMap<String/* clientId */, ConsumerRunningInfo> criTable)
        {
            return true;
        }

        public static String analyzeProcessQueue(String clientId, ConsumerRunningInfo info)
        {
            StringBuilder sb = new StringBuilder();
            bool push = false;
            {
                String property = info.properties.getProperty(ConsumerRunningInfo.PROP_CONSUME_TYPE);

                if (property == null)
                {
                    property = ((ConsumeType)info.properties.get(ConsumerRunningInfo.PROP_CONSUME_TYPE)).name();
                }
                //push = ConsumeType.valueOf(property) == ConsumeType.CONSUME_PASSIVELY;
                push = property.ToEnum<ConsumeType>(ConsumeType.UNKNOWN) == ConsumeType.CONSUME_PASSIVELY;
            }

            bool orderMsg = false;
            {
                String property = info.properties.getProperty(ConsumerRunningInfo.PROP_CONSUME_ORDERLY);
                orderMsg = bool.Parse(property);
            }

            if (push)
            {
                //Iterator<Entry<MessageQueue, ProcessQueueInfo>> it = info.getMqTable().entrySet().iterator();
                //while (it.hasNext())
                foreach(var entry in info.mqTable)
                {
                    //Entry<MessageQueue, ProcessQueueInfo> next = it.next();
                    MessageQueue mq = entry.Key;
                    ProcessQueueInfo pq = entry.Value;

                    if (orderMsg)
                    {

                        if (!pq.isLocked())
                        {
                            sb.Append(String.Format("%s %s can't lock for a while, %dms%n",
                                clientId,
                                mq,
                                Sys.currentTimeMillis() - pq.getLastLockTimestamp()));
                        }
                        else
                        {
                            if (pq.isDroped() && (pq.getTryUnlockTimes() > 0))
                            {
                                sb.Append(String.Format("%s %s unlock %d times, still failed%n",
                                    clientId,
                                    mq,
                                    pq.getTryUnlockTimes()));
                            }
                        }

                    }
                    else
                    {
                        long diff = Sys.currentTimeMillis() - pq.getLastConsumeTimestamp();

                        if (diff > (1000 * 60) && pq.getCachedMsgCount() > 0)
                        {
                            sb.Append(String.Format("%s %s can't consume for a while, maybe blocked, %dms%n",
                                clientId,
                                mq,
                                diff));
                        }
                    }
                }
            }

            return sb.ToString();
        }      

        public String formatString()
        {
            StringBuilder sb = new StringBuilder();
            {
                sb.Append("#Consumer Properties#\n");
                //Iterator<Entry<Object, Object>> it = this.properties.entrySet().iterator();
                //while (it.hasNext())
                foreach(var entry in this.properties)
                {
                    //Entry<Object, Object> next = it.next();
                    string item = string.Format("%-40s: %s%n", entry.Key.ToString(), entry.Value.ToString());
                    sb.Append(item);
                }
            }

            {
                sb.Append("\n\n#Consumer Subscription#\n");
                int i = 0;
                //Iterator<SubscriptionData> it = this.subscriptionSet.iterator();
                //while (it.hasNext())
                foreach(var entry in subscriptionSet)
                {
                    //SubscriptionData next = it.next();
                    String item = String.Format("%03d Topic: %-40s ClassFilter: %-8s SubExpression: %s%n",
                        ++i,
                        entry.topic,
                        entry.classFilterMode,
                        entry.subString);

                    sb.Append(item);
                }
            }

            {
                sb.Append("\n\n#Consumer Offset#\n");
                sb.Append(String.Format("%-64s  %-32s  %-4s  %-20s%n",
                    "#Topic",
                    "#Broker Name",
                    "#QID",
                    "#Consumer Offset"
                ));

                //Iterator<Entry<MessageQueue, ProcessQueueInfo>> it = this.mqTable.entrySet().iterator();
                //while (it.hasNext())
                foreach (var entry in mqTable)
                {
                    //Entry<MessageQueue, ProcessQueueInfo> next = it.next();
                    string item = string.Format("%-32s  %-32s  %-4d  %-20d%n",
                        entry.Key.getTopic(),
                        entry.Key.getBrokerName(),
                        entry.Key.getQueueId(),
                        entry.Value.getCommitOffset());

                    sb.Append(item);
                }
            }

            {
                sb.Append("\n\n#Consumer MQ Detail#\n");
                sb.Append(string.Format("%-64s  %-32s  %-4s  %-20s%n",
                    "#Topic",
                    "#Broker Name",
                    "#QID",
                    "#ProcessQueueInfo"
                ));

                //Iterator<Entry<MessageQueue, ProcessQueueInfo>> it = this.mqTable.entrySet().iterator();
                //while (it.hasNext())
                foreach (var entry in mqTable)
                {
                    //Entry<MessageQueue, ProcessQueueInfo> next = it.next();
                    string item = string.Format("%-64s  %-32s  %-4d  %s%n",
                        entry.Key.getTopic(),
                        entry.Key.getBrokerName(),
                        entry.Key.getQueueId(),
                        entry.Key.ToString());

                    sb.Append(item);
                }
            }

            {
                sb.Append("\n\n#Consumer RT&TPS#\n");
                sb.Append(string.Format("%-64s  %14s %14s %14s %14s %18s %25s%n",
                    "#Topic",
                    "#Pull RT",
                    "#Pull TPS",
                    "#Consume RT",
                    "#ConsumeOK TPS",
                    "#ConsumeFailed TPS",
                    "#ConsumeFailedMsgsInHour"
                ));

                //Iterator<Entry<String, ConsumeStatus>> it = this.statusTable.entrySet().iterator();
                //while (it.hasNext())
                foreach (var entry in statusTable)
                {
                    //Entry<String, ConsumeStatus> next = it.next();
                    String item = String.Format("%-32s  %14.2f %14.2f %14.2f %14.2f %18.2f %25d%n",
                        entry.Key,
                        entry.Value.pullRT,
                        entry.Value.pullTPS,
                        entry.Value.consumeRT,
                        entry.Value.consumeOKTPS,
                        entry.Value.consumeFailedTPS,
                        entry.Value.consumeFailedMsgs
                    );

                    sb.Append(item);
                }
            }

            if (this.jstack != null)
            {
                sb.Append("\n\n#Consumer jstack#\n");
                sb.Append(this.jstack);
            }

            return sb.ToString();
        }
    }
}
