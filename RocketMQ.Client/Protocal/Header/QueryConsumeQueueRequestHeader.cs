using System;

namespace RocketMQ.Client
{
    public class QueryConsumeQueueRequestHeader : CommandCustomHeader
    {
        public String topic{ get; set; }
        public int queueId{ get; set; }
        public long index{ get; set; }
        public int count{ get; set; }
        public String consumerGroup{ get; set; }

   
        public void checkFields()
        {

        }
    }
}
