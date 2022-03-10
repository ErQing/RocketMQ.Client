using System;

namespace RocketMQ.Client
{
    public interface TraceDispatcher
    {
        enum Type
        {
            PRODUCE,
            CONSUME
        }
        /**
         * Initialize asynchronous transfer data module
         */
        ///<exception cref="MQClientException"/>
        void start(String nameSrvAddr, AccessChannel accessChannel);

        /**
         * Append the transfering data
         * @param ctx data infomation
         * @return
         */
        bool append(Object ctx);

        /**
         * Write flush action
         *
         * @throws IOException
         */
        ///<exception cref="System.IO.IOException"/>
        void flush();

        /**
         * Close the trace Hook
         */
        void shutdown();
    }
}
