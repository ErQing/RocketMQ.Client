using System;
using System.Collections.Generic;
using System.Text;

namespace RocketMQ.Client
{
    public class QueryResult
    {
        private readonly long indexLastUpdateTimestamp;
        private readonly ICollection<MessageExt> messageList;

        public QueryResult(long indexLastUpdateTimestamp, ICollection<MessageExt> messageList)
        {
            this.indexLastUpdateTimestamp = indexLastUpdateTimestamp;
            this.messageList = messageList;
        }

        public long getIndexLastUpdateTimestamp()
        {
            return indexLastUpdateTimestamp;
        }

        public ICollection<MessageExt> getMessageList()
        {
            return messageList;
        }

        public override String ToString()
        {
            return "QueryResult [indexLastUpdateTimestamp=" + indexLastUpdateTimestamp + ", messageList="
                + messageList + "]";
        }
    }
}
