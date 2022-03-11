using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RocketMQ.Client
{
    public class CreateTopicRequestHeader : CommandCustomHeader
    {
        [CFNotNull]
        public string topic { get; set; }
        [CFNotNull]
        public string defaultTopic { get; set; }
        [CFNotNull]
        public int readQueueNums { get; set; }
        [CFNotNull]
        public int writeQueueNums { get; set; }
        [CFNotNull]
        public int perm { get; set; }
        [CFNotNull]
        public string topicFilterType { get; set; }
        public int topicSysFlag { get; set; }
        [CFNotNull]
        public bool order { get; set; } = false;

        public void checkFields()
        {
            //try
            //{
            //    TopicFilterType.valueOf(this.topicFilterType);
            //}
            //catch (Exception e)
            //{
            //    throw new RemotingCommandException("topicFilterType = [" + topicFilterType + "] value invalid", e);
            //}
            var flag = Enum.IsDefined(typeof(TopicFilterType), topicFilterType);
            if(!flag)
                throw new RemotingCommandException("topicFilterType = [" + topicFilterType + "] value invalid");
        }

        public TopicFilterType getTopicFilterTypeEnum()
        {
            return topicFilterType.ToEnum(TopicFilterType.UNKNOWN);
            //return TopicFilterType.valueOf(this.topicFilterType);
        }

    }
}
