using System;
using System.Collections.Generic;

namespace RocketMQ.Client
{
    public interface OffsetStore
    {
        /**
         * Load
         */
        ///<exception cref="MQClientException"/>
        void load();

        /**
         * Update the offset,store it in memory
         */
        void updateOffset(MessageQueue mq, long offset, bool increaseOnly);

        /**
         * Get offset from local storage
         *
         * @return The fetched offset
         */
        long readOffset(MessageQueue mq, ReadOffsetType type);

        /**
         * Persist all offsets,may be in local storage or remote name server
         */
        void persistAll(HashSet<MessageQueue> mqs);

        /**
         * Persist the offset,may be in local storage or remote name server
         */
        void persist(MessageQueue mq);

        /**
         * Remove offset
         */
        void removeOffset(MessageQueue mq);

        /**
         * @return The cloned offset table of given topic
         */
        Dictionary<MessageQueue, long> cloneOffsetTable(String topic);

        /**
         * @param mq
         * @param offset
         * @param isOneway
         */
        void updateConsumeOffsetToBroker(MessageQueue mq, long offset, bool isOneway);
    }
}
