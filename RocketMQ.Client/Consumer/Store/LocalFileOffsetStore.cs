using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;

namespace RocketMQ.Client//.Consumer.Store
{
    public class LocalFileOffsetStore : OffsetStore
    {
        public readonly static string LOCAL_OFFSET_STORE_DIR = Sys.getProperty(
        "rocketmq.client.localOffsetStoreDir",
        Sys.getProperty("user.home") + Path.DirectorySeparatorChar + ".rocketmq_offsets");

        static NLog.Logger log = NLog.LogManager.GetCurrentClassLogger();
        //private readonly static InternalLogger log = ClientLogger.getLog();
        private readonly MQClientInstance mQClientFactory;
        private readonly string groupName;
        private readonly string storePath;
        private ConcurrentDictionary<MessageQueue, AtomicLong> offsetTable =
            new ConcurrentDictionary<MessageQueue, AtomicLong>();

        public LocalFileOffsetStore(MQClientInstance mQClientFactory, string groupName)
        {
            this.mQClientFactory = mQClientFactory;
            this.groupName = groupName;
            //this.storePath = LOCAL_OFFSET_STORE_DIR + File.separator +
            //    this.mQClientFactory.getClientId() + File.separator +
            //    this.groupName + File.separator +
            //    "offsets.json";

            this.storePath = LOCAL_OFFSET_STORE_DIR + Path.DirectorySeparatorChar +
                this.mQClientFactory.getClientId() + Path.DirectorySeparatorChar +
                this.groupName + Path.DirectorySeparatorChar +
                "offsets.json";
        }

        //@Override
        public void load()
        {
            OffsetSerializeWrapper offsetSerializeWrapper = this.readLocalOffset();
            if (offsetSerializeWrapper != null && offsetSerializeWrapper.getOffsetTable() != null)
            {
                offsetTable.PutAll(offsetSerializeWrapper.getOffsetTable());

                foreach (var mqEntry in offsetSerializeWrapper.getOffsetTable())
                {
                    AtomicLong offset = mqEntry.Value;
                    log.Info("load consumer's offset, {} {} {}",
                                    this.groupName,
                                    mqEntry.Key,
                                    offset.Get());
                }
            }
        }

        //@Override
        public void updateOffset(MessageQueue mq, long offset, bool increaseOnly)
        {
            if (mq != null)
            {
                AtomicLong offsetOld = this.offsetTable.Get(mq);
                if (null == offsetOld)
                {
                    offsetOld = this.offsetTable.PutIfAbsent(mq, new AtomicLong(offset));
                }

                if (null != offsetOld)
                {
                    if (increaseOnly)
                    {
                        MixAll.compareAndIncreaseOnly(offsetOld, offset);
                    }
                    else
                    {
                        offsetOld.Set(offset);
                    }
                }
            }
        }

        //@Override
        public long readOffset(MessageQueue mq, ReadOffsetType type)
        {
            if (mq != null)
            {
                switch (type)
                {
                    case ReadOffsetType.MEMORY_FIRST_THEN_STORE:
                    case ReadOffsetType.READ_FROM_MEMORY:
                        {
                            AtomicLong offset = this.offsetTable.Get(mq);
                            if (offset != null)
                            {
                                return offset.Get();
                            }
                            else if (ReadOffsetType.READ_FROM_MEMORY == type)
                            {
                                return -1;
                            }
                            break;
                        }
                    case ReadOffsetType.READ_FROM_STORE:
                        {
                            OffsetSerializeWrapper offsetSerializeWrapper;
                            try
                            {
                                offsetSerializeWrapper = this.readLocalOffset();
                            }
                            catch (MQClientException e)
                            {
                                return -1;
                            }
                            if (offsetSerializeWrapper != null && offsetSerializeWrapper.getOffsetTable() != null)
                            {
                                AtomicLong offset = offsetSerializeWrapper.getOffsetTable().Get(mq);
                                if (offset != null)
                                {
                                    this.updateOffset(mq, offset.Get(), false);
                                    return offset.Get();
                                }
                            }
                            break;
                        }
                    default:
                        break;
                }
            }

            return -1;
        }

        //@Override
        public void persistAll(HashSet<MessageQueue> mqs)
        {
            if (null == mqs || mqs.IsEmpty())
                return;

            OffsetSerializeWrapper offsetSerializeWrapper = new OffsetSerializeWrapper();
            foreach (var entry in this.offsetTable)
            {
                if (mqs.Contains(entry.Key))
                {
                    AtomicLong offset = entry.Value;
                    offsetSerializeWrapper.getOffsetTable().Put(entry.Key, offset);
                }
            }

            string jsonString = offsetSerializeWrapper.toJson(true);
            if (jsonString != null)
            {
                try
                {
                    MixAll.string2File(jsonString, this.storePath);
                }
                catch (IOException e)
                {
                    log.Error("persistAll consumer offset Exception, " + this.storePath, e);
                }
            }
        }

        //@Override
        public void persist(MessageQueue mq)
        {
        }

        //@Override
        public void removeOffset(MessageQueue mq)
        {

        }

        //@Override
        public void updateConsumeOffsetToBroker(MessageQueue mq, long offset, bool isOneway)
        {

        }

        //@Override
        public Dictionary<MessageQueue, long> cloneOffsetTable(String topic)
        {
            Dictionary<MessageQueue, long> cloneOffsetTable = new Dictionary<MessageQueue, long>();
            foreach (var entry in this.offsetTable)
            {
                MessageQueue mq = entry.Key;
                if (!UtilAll.isBlank(topic) && !topic.Equals(mq.getTopic()))
                {
                    continue;
                }
                cloneOffsetTable.Put(mq, entry.Value.Get());

            }
            return cloneOffsetTable;
        }

        ///<exception cref="MQClientException"/>
        private OffsetSerializeWrapper readLocalOffset()
        {
            string content = null;
            try
            {
                content = MixAll.file2String(this.storePath);
            }
            catch (IOException e)
            {
                log.Warn("Load local offset store file exception", e);
            }
            if (null == content || content.Length == 0)
            {
                return this.readLocalOffsetBak();
            }
            else
            {
                OffsetSerializeWrapper offsetSerializeWrapper = null;
                try
                {
                    offsetSerializeWrapper =
                        OffsetSerializeWrapper.fromJson<OffsetSerializeWrapper>(content);
                }
                catch (Exception e)
                {
                    log.Warn("readLocalOffset Exception, and try to correct", e.ToString());
                    return this.readLocalOffsetBak();
                }

                return offsetSerializeWrapper;
            }
        }

        ///<exception cref="MQClientException"/>
        private OffsetSerializeWrapper readLocalOffsetBak()
        {
            string content = null;
            try
            {
                content = MixAll.file2String(this.storePath + ".bak");
            }
            catch (IOException e)
            {
                log.Warn("Load local offset store bak file exception", e);
            }
            if (content != null && content.Length > 0)
            {
                OffsetSerializeWrapper offsetSerializeWrapper = null;
                try
                {
                    offsetSerializeWrapper =
                        OffsetSerializeWrapper.fromJson<OffsetSerializeWrapper>(content);
                }
                catch (Exception e)
                {
                    log.Warn("readLocalOffset Exception", e.ToString());
                    throw new MQClientException("readLocalOffset Exception, maybe fastjson version too low"
                        + FAQUrl.suggestTodo(FAQUrl.LOAD_JSON_EXCEPTION),
                        e);
                }
                return offsetSerializeWrapper;
            }

            return null;
        }

    }
}
