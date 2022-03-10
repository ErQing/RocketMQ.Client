using System;
using System.Collections.Generic;
using System.Text;

namespace RocketMQ.Client
{
    public class ClientConfig
    {
        public static readonly string SEND_MESSAGE_WITH_VIP_CHANNEL_PROPERTY = "com.rocketmq.sendMessageWithVIPChannel";

        private string namesrvAddr = NameServerAddressUtils.getNameServerAddresses();
        private string clientIP = RemotingUtil.getLocalAddress();
        private string instanceName = Sys.getProperty("rocketmq.client.name", "DEFAULT");
        //private int clientCallbackExecutorThreads = Runtime.getRuntime().availableProcessors();
        private int clientCallbackExecutorThreads = Environment.ProcessorCount;
        protected string nameSpace;
        private bool namespaceInitialized = false;
        protected AccessChannel accessChannel = AccessChannel.LOCAL;

        /**
         * Pulling topic information interval from the named server
         */
        private int pollNameServerInterval = 1000 * 30;
        /**
         * Heartbeat interval in microseconds with message broker
         */
        private int heartbeatBrokerInterval = 1000 * 30;
        /**
         * Offset persistent interval for consumer
         */
        private int persistConsumerOffsetInterval = 1000 * 5;
        private long pullTimeDelayMillsWhenException = 1000;
        private bool unitMode = false;
        private string unitName;
        private bool vipChannelEnabled = bool.Parse(Sys.getProperty(SEND_MESSAGE_WITH_VIP_CHANNEL_PROPERTY, "false"));

        private bool useTLS = TlsSystemConfig.tlsEnable;

        private int mqClientApiTimeout = 3 * 1000;

        private LanguageCode language = LanguageCode.JAVA;

        public string buildMQClientId()
        {
            StringBuilder sb = new StringBuilder();
            sb.Append(this.getClientIP());

            sb.Append("@");
            sb.Append(this.getInstanceName());
            if (!string.IsNullOrEmpty(this.unitName))
            {
                sb.Append("@");
                sb.Append(this.unitName);
            }

            return sb.ToString();
        }

    public string getClientIP()
    {
        return clientIP;
    }

    public void setClientIP(String clientIP)
    {
        this.clientIP = clientIP;
    }

    public string getInstanceName()
    {
        return instanceName;
    }

    public void setInstanceName(String instanceName)
    {
        this.instanceName = instanceName;
    }

    public void changeInstanceNameToPID()
    {
        if (this.instanceName.Equals("DEFAULT"))
        {
            this.instanceName = UtilAll.getPid() + "#" + UtilAll.nanoTime();
        }
    }

    public string withNamespace(String resource)
    {
        return NamespaceUtil.wrapNamespace(this.getNamespace(), resource);
    }

    public HashSet<String> withNamespace(HashSet<String> resourceSet)
    {
            HashSet<String> resourceWithNamespace = new HashSet<String>();
        foreach (String resource in resourceSet)
        {
            resourceWithNamespace.Add(withNamespace(resource));
        }
        return resourceWithNamespace;
    }

    public string withoutNamespace(String resource)
    {
        return NamespaceUtil.withoutNamespace(resource, this.getNamespace());
    }

    public HashSet<String> withoutNamespace(HashSet<String> resourceSet)
    {
            HashSet<String> resourceWithoutNamespace = new HashSet<String>();
        foreach (String resource in resourceSet)
        {
            resourceWithoutNamespace.Add(withoutNamespace(resource));
        }
        return resourceWithoutNamespace;
    }

    public MessageQueue queueWithNamespace(MessageQueue queue)
    {
        if (string.IsNullOrEmpty(this.getNamespace()))
        {
            return queue;
        }
        return new MessageQueue(withNamespace(queue.getTopic()), queue.getBrokerName(), queue.getQueueId());
    }

        public ICollection<MessageQueue> queuesWithNamespace(ICollection<MessageQueue> queues)
        {
            if (string.IsNullOrEmpty(this.getNamespace()))
            {
                return queues;
            }
            //Iterator<MessageQueue> iter = queues.iterator();
            //while (iter.hasNext())
            //{
            //    MessageQueue queue = iter.next();
            //    queue.setTopic(withNamespace(queue.getTopic()));
            //}
            foreach (var queue in queues)
            {
                queue.setTopic(withNamespace(queue.getTopic()));
            }
            return queues;
        }

        public void resetClientConfig(ClientConfig cc)
    {
        this.namesrvAddr = cc.namesrvAddr;
        this.clientIP = cc.clientIP;
        this.instanceName = cc.instanceName;
        this.clientCallbackExecutorThreads = cc.clientCallbackExecutorThreads;
        this.pollNameServerInterval = cc.pollNameServerInterval;
        this.heartbeatBrokerInterval = cc.heartbeatBrokerInterval;
        this.persistConsumerOffsetInterval = cc.persistConsumerOffsetInterval;
        this.pullTimeDelayMillsWhenException = cc.pullTimeDelayMillsWhenException;
        this.unitMode = cc.unitMode;
        this.unitName = cc.unitName;
        this.vipChannelEnabled = cc.vipChannelEnabled;
        this.useTLS = cc.useTLS;
        this.nameSpace = cc.nameSpace;
        this.language = cc.language;
        this.mqClientApiTimeout = cc.mqClientApiTimeout;
    }

    public ClientConfig cloneClientConfig()
    {
        ClientConfig cc = new ClientConfig();
        cc.namesrvAddr = namesrvAddr;
        cc.clientIP = clientIP;
        cc.instanceName = instanceName;
        cc.clientCallbackExecutorThreads = clientCallbackExecutorThreads;
        cc.pollNameServerInterval = pollNameServerInterval;
        cc.heartbeatBrokerInterval = heartbeatBrokerInterval;
        cc.persistConsumerOffsetInterval = persistConsumerOffsetInterval;
        cc.pullTimeDelayMillsWhenException = pullTimeDelayMillsWhenException;
        cc.unitMode = unitMode;
        cc.unitName = unitName;
        cc.vipChannelEnabled = vipChannelEnabled;
        cc.useTLS = useTLS;
        cc.nameSpace = nameSpace;
        cc.language = language;
        cc.mqClientApiTimeout = mqClientApiTimeout;
        return cc;
    }

        public string getNamesrvAddr()
        {
            //if (!string.IsNullOrEmpty(namesrvAddr) && NameServerAddressUtils.NAMESRV_ENDPOINT_PATTERN.matcher(namesrvAddr.Trim()).matches())
            if (!string.IsNullOrEmpty(namesrvAddr) && NameServerAddressUtils.NAMESRV_ENDPOINT_PATTERN.Match(namesrvAddr.Trim()).Success) //???
            {
                return NameServerAddressUtils.getNameSrvAddrFromNamesrvEndpoint(namesrvAddr);
            }
            return namesrvAddr;
        }

        /**
         * Domain name mode access way does not support the delimiter(;), and only one domain name can be set.
         *
         * @param namesrvAddr name server address
         */
        public void setNamesrvAddr(String namesrvAddr)
    {
        this.namesrvAddr = namesrvAddr;
        this.namespaceInitialized = false;
    }

    public int getClientCallbackExecutorThreads()
    {
        return clientCallbackExecutorThreads;
    }

    public void setClientCallbackExecutorThreads(int clientCallbackExecutorThreads)
    {
        this.clientCallbackExecutorThreads = clientCallbackExecutorThreads;
    }

    public int getPollNameServerInterval()
    {
        return pollNameServerInterval;
    }

    public void setPollNameServerInterval(int pollNameServerInterval)
    {
        this.pollNameServerInterval = pollNameServerInterval;
    }

    public int getHeartbeatBrokerInterval()
    {
        return heartbeatBrokerInterval;
    }

    public void setHeartbeatBrokerInterval(int heartbeatBrokerInterval)
    {
        this.heartbeatBrokerInterval = heartbeatBrokerInterval;
    }

    public int getPersistConsumerOffsetInterval()
    {
        return persistConsumerOffsetInterval;
    }

    public void setPersistConsumerOffsetInterval(int persistConsumerOffsetInterval)
    {
        this.persistConsumerOffsetInterval = persistConsumerOffsetInterval;
    }

    public long getPullTimeDelayMillsWhenException()
    {
        return pullTimeDelayMillsWhenException;
    }

    public void setPullTimeDelayMillsWhenException(long pullTimeDelayMillsWhenException)
    {
        this.pullTimeDelayMillsWhenException = pullTimeDelayMillsWhenException;
    }

    public string getUnitName()
    {
        return unitName;
    }

    public void setUnitName(String unitName)
    {
        this.unitName = unitName;
    }

    public bool isUnitMode()
    {
        return unitMode;
    }

    public void setUnitMode(bool unitMode)
    {
        this.unitMode = unitMode;
    }

    public bool isVipChannelEnabled()
    {
        return vipChannelEnabled;
    }

    public void setVipChannelEnabled(bool vipChannelEnabled)
    {
        this.vipChannelEnabled = vipChannelEnabled;
    }

    public bool isUseTLS()
    {
        return useTLS;
    }

    public virtual void setUseTLS(bool useTLS)
    {
        this.useTLS = useTLS;
    }

    public LanguageCode getLanguage()
    {
        return language;
    }

    public void setLanguage(LanguageCode language)
    {
        this.language = language;
    }

    public string getNamespace()
    {
        if (namespaceInitialized)
        {
            return nameSpace;
        }

        if (!string.IsNullOrEmpty(nameSpace)) {
            return nameSpace;
        }

        if (!string.IsNullOrEmpty(this.namesrvAddr))
        {
            if (NameServerAddressUtils.validateInstanceEndpoint(namesrvAddr))
            {
                    nameSpace = NameServerAddressUtils.parseInstanceIdFromEndpoint(namesrvAddr);
            }
        }
        namespaceInitialized = true;
        return nameSpace;
    }

    public void setNamespace(String nameSpace) {
        this.nameSpace = nameSpace;
        this.namespaceInitialized = true;
    }

    public AccessChannel getAccessChannel()
    {
        return this.accessChannel;
    }

    public void setAccessChannel(AccessChannel accessChannel)
    {
        this.accessChannel = accessChannel;
    }

    public int getMqClientApiTimeout()
    {
        return mqClientApiTimeout;
    }

    public void setMqClientApiTimeout(int mqClientApiTimeout)
    {
        this.mqClientApiTimeout = mqClientApiTimeout;
    }

    public override string ToString()
    {
        return "ClientConfig [namesrvAddr=" + namesrvAddr + ", clientIP=" + clientIP + ", instanceName=" + instanceName
            + ", clientCallbackExecutorThreads=" + clientCallbackExecutorThreads + ", pollNameServerInterval=" + pollNameServerInterval
            + ", heartbeatBrokerInterval=" + heartbeatBrokerInterval + ", persistConsumerOffsetInterval=" + persistConsumerOffsetInterval
            + ", pullTimeDelayMillsWhenException=" + pullTimeDelayMillsWhenException + ", unitMode=" + unitMode + ", unitName=" + unitName + ", vipChannelEnabled="
            + vipChannelEnabled + ", useTLS=" + useTLS + ", language=" + language.ToString() + ", nameSpace=" + nameSpace + ", mqClientApiTimeout=" + mqClientApiTimeout + "]";
    }

    }
}
