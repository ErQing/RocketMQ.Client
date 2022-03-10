using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace RocketMQ.Client
{
    public class MQClientAPIImpl
    {
        //private final static InternalLogger log = ClientLogger.getLog();
        static NLog.Logger log = NLog.LogManager.GetCurrentClassLogger();
        private static bool sendSmartMsg = bool.Parse(Sys.getProperty("org.apache.rocketmq.client.sendSmartMsg", "true"));

        static MQClientAPIImpl()
        {
            Sys.setProperty(RemotingCommand.REMOTING_VERSION_KEY, MQVersion.CURRENT_VERSION.ToString());
        }

        private readonly RemotingClient remotingClient;
        private readonly TopAddressing topAddressing;
        private readonly ClientRemotingProcessor clientRemotingProcessor;
        private String nameSrvAddr = null;
        private ClientConfig clientConfig;

        public MQClientAPIImpl(NettyClientConfig nettyClientConfig,
            ClientRemotingProcessor clientRemotingProcessor,
            RPCHook rpcHook, ClientConfig clientConfig)
        {
            this.clientConfig = clientConfig;
            topAddressing = new TopAddressing(MixAll.getWSAddr(), clientConfig.getUnitName());
            this.remotingClient = new NettyRemotingClient(nettyClientConfig, null);
            this.clientRemotingProcessor = clientRemotingProcessor;

            this.remotingClient.registerRPCHook(rpcHook);
            this.remotingClient.registerProcessor(RequestCode.CHECK_TRANSACTION_STATE, this.clientRemotingProcessor, null);

            this.remotingClient.registerProcessor(RequestCode.NOTIFY_CONSUMER_IDS_CHANGED, this.clientRemotingProcessor, null);

            this.remotingClient.registerProcessor(RequestCode.RESET_CONSUMER_CLIENT_OFFSET, this.clientRemotingProcessor, null);

            this.remotingClient.registerProcessor(RequestCode.GET_CONSUMER_STATUS_FROM_CLIENT, this.clientRemotingProcessor, null);

            this.remotingClient.registerProcessor(RequestCode.GET_CONSUMER_RUNNING_INFO, this.clientRemotingProcessor, null);

            this.remotingClient.registerProcessor(RequestCode.CONSUME_MESSAGE_DIRECTLY, this.clientRemotingProcessor, null);

            this.remotingClient.registerProcessor(RequestCode.PUSH_REPLY_MESSAGE_TO_CLIENT, this.clientRemotingProcessor, null);
        }

        public List<String> getNameServerAddressList()
        {
            return this.remotingClient.getNameServerAddressList();
        }

        public RemotingClient getRemotingClient()
        {
            return remotingClient;
        }

        public String fetchNameServerAddr()
        {
            try
            {
                String addrs = this.topAddressing.fetchNSAddr();
                if (addrs != null)
                {
                    if (!addrs.Equals(this.nameSrvAddr))
                    {
                        log.Info("name server address changed, old=" + this.nameSrvAddr + ", new=" + addrs);
                        this.updateNameServerAddressList(addrs);
                        this.nameSrvAddr = addrs;
                        return nameSrvAddr;
                    }
                }
            }
            catch (Exception e)
            {
                log.Error("fetchNameServerAddr Exception", e.ToString());
            }
            return nameSrvAddr;
        }

        public void updateNameServerAddressList(String addrs)
        {
            String[] addrArray = addrs.Split(";");
            //List<String> list = Arrays.asList(addrArray);
            List<String> list = addrArray.ToList();
            this.remotingClient.updateNameServerAddressList(list);
        }

        public void start()
        {
            this.remotingClient.start();
        }

        public void shutdown()
        {
            this.remotingClient.shutdown();
        }


        ///<exception cref="ThreadInterruptedException"/>
        ///<exception cref="RemotingException"/>
        ///<exception cref="MQBrokerException"/>
        ///<exception cref="MQClientException"/>
        public void createSubscriptionGroup(String addr, SubscriptionGroupConfig config, long timeoutMillis)
        {
            RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.UPDATE_AND_CREATE_SUBSCRIPTIONGROUP, null);

            byte[] body = RemotingSerializable.encode(config);
            request.setBody(body);

            RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr),
                request, timeoutMillis);
            //assert response != null;
            Debug.Assert(response != null);
            switch (response.getCode())
            {
                case ResponseCode.SUCCESS:
                    {
                        return;
                    }
                default:
                    break;
            }

            throw new MQClientException(response.getCode(), response.getRemark());

        }

        ///<exception cref="ThreadInterruptedException"/>
        ///<exception cref="RemotingException"/>
        ///<exception cref="MQBrokerException"/>
        ///<exception cref="MQClientException"/>
        public void createTopic(String addr, String defaultTopic, TopicConfig topicConfig, long timeoutMillis)
        {
            CreateTopicRequestHeader requestHeader = new CreateTopicRequestHeader();
            requestHeader.topic = topicConfig.getTopicName();
            requestHeader.defaultTopic = defaultTopic;
            requestHeader.readQueueNums = topicConfig.getReadQueueNums();
            requestHeader.writeQueueNums = topicConfig.getWriteQueueNums();
            requestHeader.perm = topicConfig.getPerm();
            requestHeader.topicFilterType = topicConfig.getTopicFilterType().name();
            requestHeader.topicSysFlag = topicConfig.getTopicSysFlag();
            requestHeader.order = topicConfig.isOrder();

            RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.UPDATE_AND_CREATE_TOPIC, requestHeader);
            RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr),
                request, timeoutMillis);
            //assert response != null;
            Debug.Assert(response != null);
            switch (response.getCode())
            {
                case ResponseCode.SUCCESS:
                    {
                        return;
                    }
                default:
                    break;
            }

            throw new MQClientException(response.getCode(), response.getRemark());
        }

        ///<exception cref="ThreadInterruptedException"/>
        ///<exception cref="RemotingException"/>
        ///<exception cref="MQBrokerException"/>
        ///<exception cref="MQClientException"/>
        public void createPlainAccessConfig(String addr, PlainAccessConfig plainAccessConfig, long timeoutMillis)
        {
            CreateAccessConfigRequestHeader requestHeader = new CreateAccessConfigRequestHeader();
            requestHeader.accessKey = plainAccessConfig.getAccessKey();
            requestHeader.secretKey = plainAccessConfig.getSecretKey();
            requestHeader.admin = plainAccessConfig.isAdmin();
            requestHeader.defaultGroupPerm = plainAccessConfig.getDefaultGroupPerm();
            requestHeader.defaultTopicPerm = plainAccessConfig.getDefaultTopicPerm();
            requestHeader.whiteRemoteAddress = plainAccessConfig.getWhiteRemoteAddress();
            requestHeader.topicPerms = UtilAll.join(plainAccessConfig.getTopicPerms(), ",");
            requestHeader.groupPerms = UtilAll.join(plainAccessConfig.getGroupPerms(), ",");

            RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.UPDATE_AND_CREATE_ACL_CONFIG, requestHeader);
            RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr),
                request, timeoutMillis);
            //assert response != null;
            Debug.Assert(response != null);
            switch (response.getCode())
            {
                case ResponseCode.SUCCESS:
                    {
                        return;
                    }
                default:
                    break;
            }

            throw new MQClientException(response.getCode(), response.getRemark());
        }

        ///<exception cref="ThreadInterruptedException"/>
        ///<exception cref="RemotingException"/>
        ///<exception cref="MQBrokerException"/>
        ///<exception cref="MQClientException"/>
        public void deleteAccessConfig(String addr, String accessKey, long timeoutMillis)
        {
            DeleteAccessConfigRequestHeader requestHeader = new DeleteAccessConfigRequestHeader();
            requestHeader.accessKey = accessKey;

            RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.DELETE_ACL_CONFIG, requestHeader);
            RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr),
                request, timeoutMillis);
            //assert response != null;
            Debug.Assert(response != null);
            switch (response.getCode())
            {
                case ResponseCode.SUCCESS:
                    {
                        return;
                    }
                default:
                    break;
            }

            throw new MQClientException(response.getCode(), response.getRemark());
        }

        ///<exception cref="ThreadInterruptedException"/>
        ///<exception cref="RemotingException"/>
        ///<exception cref="MQBrokerException"/>
        ///<exception cref="MQClientException"/>
        public void updateGlobalWhiteAddrsConfig(String addr, String globalWhiteAddrs, long timeoutMillis)
        {

            UpdateGlobalWhiteAddrsConfigRequestHeader requestHeader = new UpdateGlobalWhiteAddrsConfigRequestHeader();
            requestHeader.globalWhiteAddrs = globalWhiteAddrs;

            RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.UPDATE_GLOBAL_WHITE_ADDRS_CONFIG, requestHeader);
            RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr),
                request, timeoutMillis);
            //assert response != null;
            Debug.Assert(response != null);
            switch (response.getCode())
            {
                case ResponseCode.SUCCESS:
                    {
                        return;
                    }
                default:
                    break;
            }

            throw new MQClientException(response.getCode(), response.getRemark());
        }

        ///<exception cref="ThreadInterruptedException"/>
        ///<exception cref="RemotingException"/>
        ///<exception cref="MQBrokerException"/>
        ///<exception cref="MQClientException"/>
        ///<exception cref="RemotingCommandException"/>
        ///<exception cref="RemotingTimeoutException"/>
        ///<exception cref="RemotingSendRequestException"/>
        ///<exception cref="RemotingConnectException"/>
        public ClusterAclVersionInfo getBrokerClusterAclInfo(String addr,
        long timeoutMillis)
        {
            RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_BROKER_CLUSTER_ACL_INFO, null);

            RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr), request, timeoutMillis);
            //assert response != null;
            Debug.Assert(response != null);
            switch (response.getCode())
            {
                case ResponseCode.SUCCESS:
                    {
                        //GetBrokerAclConfigResponseHeader responseHeader =
                        //(GetBrokerAclConfigResponseHeader)response.decodeCommandCustomHeader(GetBrokerAclConfigResponseHeader/*.class*/);
                        GetBrokerAclConfigResponseHeader responseHeader = response.decodeCommandCustomHeader<GetBrokerAclConfigResponseHeader>();
                        ClusterAclVersionInfo clusterAclVersionInfo = new ClusterAclVersionInfo();
                        clusterAclVersionInfo.clusterName = responseHeader.clusterName;
                        clusterAclVersionInfo.brokerName = responseHeader.brokerName;
                        clusterAclVersionInfo.brokerAddr = responseHeader.brokerAddr;
                        //clusterAclVersionInfo.setAclConfigDataVersion(DataVersion.fromJson(responseHeader.getVersion(), DataVersion/*.class*/));
                        clusterAclVersionInfo.aclConfigDataVersion = RemotingSerializable.fromJson<DataVersion>(responseHeader.version);
                        //Dictionary<String, Object> dataVersionMap = JSON.parseObject(responseHeader.getAllAclFileVersion(), HashMap/*.class*/);
                        Dictionary<string, object> dataVersionMap = JSON.parseObject<Dictionary<string, object>>(responseHeader.allAclFileVersion);
                        Dictionary<String, DataVersion> allAclConfigDataVersion = new Dictionary<String, DataVersion>();
                        foreach (var entry in dataVersionMap)
                        {
                            //allAclConfigDataVersion.put(entry.Key, DataVersion.fromJson(JSON.toJSONString(entry.Value), DataVersion/*.class*/));
                            allAclConfigDataVersion[entry.Key] = RemotingSerializable.fromJson<DataVersion>(JSON.toJSONString(entry.Value));
                        }
                        clusterAclVersionInfo.allAclConfigDataVersion = allAclConfigDataVersion;
                        return clusterAclVersionInfo;
                    }
                default:
                    break;
            }

            throw new MQBrokerException(response.getCode(), response.getRemark(), addr);

        }

        ///<exception cref="RemotingCommandException"/>
        ///<exception cref="InterruptedException"/>
        ///<exception cref="RemotingTimeoutException"/>
        ///<exception cref="RemotingSendRequestException"/>
        ///<exception cref="RemotingConnectException"/>
        ///<exception cref="MQBrokerException"/>
        public AclConfig getBrokerClusterConfig(String addr, long timeoutMillis)
        {
            RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_BROKER_CLUSTER_ACL_CONFIG, null);

            RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr), request, timeoutMillis);
            //assert response != null;
            Debug.Assert(response != null);
            switch (response.getCode())
            {
                case ResponseCode.SUCCESS:
                    {
                        if (response.getBody() != null)
                        {
                            //GetBrokerClusterAclConfigResponseBody body =
                            //GetBrokerClusterAclConfigResponseBody.decode(response.getBody(), GetBrokerClusterAclConfigResponseBody/*.class*/);
                            GetBrokerClusterAclConfigResponseBody body =
                               RemotingSerializable.decode<GetBrokerClusterAclConfigResponseBody>(response.getBody());
                            AclConfig aclConfig = new AclConfig();
                            aclConfig.setGlobalWhiteAddrs(body.globalWhiteAddrs);
                            aclConfig.setPlainAccessConfigs(body.plainAccessConfigs);
                            return aclConfig;
                        }
                        break; //TODO
                    }
                default:
                    break;
            }
            throw new MQBrokerException(response.getCode(), response.getRemark(), addr);

        }

        ///<exception cref="RemotingException"/>
        ///<exception cref="MQBrokerException"/>
        ///<exception cref="InterruptedException"/>
        public SendResult sendMessage(
        String addr,
        String brokerName,
        Message msg,
        SendMessageRequestHeader requestHeader,
        long timeoutMillis,
        CommunicationMode communicationMode,
        SendMessageContext context,
        DefaultMQProducerImpl producer
    )
        {
            return sendMessage(addr, brokerName, msg, requestHeader, timeoutMillis, communicationMode, null, null, null, 0, context, producer);
        }

        ///<exception cref="RemotingException"/>
        ///<exception cref="MQBrokerException"/>
        ///<exception cref="InterruptedException"/>
        public SendResult sendMessage(
        String addr,
        String brokerName,
        Message msg,
        SendMessageRequestHeader requestHeader,
        long timeoutMillis,
        CommunicationMode communicationMode,
        SendCallback sendCallback,
        TopicPublishInfo topicPublishInfo,
        MQClientInstance instance,
        int retryTimesWhenSendFailed,
        SendMessageContext context,
        DefaultMQProducerImpl producer
    )
        {
            long beginStartTime = Sys.currentTimeMillis();
            RemotingCommand request = null;
            String msgType = msg.getProperty(MessageConst.PROPERTY_MESSAGE_TYPE);
            bool isReply = msgType != null && msgType.Equals(MixAll.REPLY_MESSAGE_FLAG);
            if (isReply)
            {
                if (sendSmartMsg)
                {
                    SendMessageRequestHeaderV2 requestHeaderV2 = SendMessageRequestHeaderV2.createSendMessageRequestHeaderV2(requestHeader);
                    request = RemotingCommand.createRequestCommand(RequestCode.SEND_REPLY_MESSAGE_V2, requestHeaderV2);
                }
                else
                {
                    request = RemotingCommand.createRequestCommand(RequestCode.SEND_REPLY_MESSAGE, requestHeader);
                }
            }
            else
            {
                if (sendSmartMsg || msg is MessageBatch)
                {
                    SendMessageRequestHeaderV2 requestHeaderV2 = SendMessageRequestHeaderV2.createSendMessageRequestHeaderV2(requestHeader);
                    request = RemotingCommand.createRequestCommand(msg is MessageBatch ? RequestCode.SEND_BATCH_MESSAGE : RequestCode.SEND_MESSAGE_V2, requestHeaderV2);
                }
                else
                {
                    request = RemotingCommand.createRequestCommand(RequestCode.SEND_MESSAGE, requestHeader);
                }
            }
            request.setBody(msg.getBody());

            switch (communicationMode)
            {
                case CommunicationMode.ONEWAY:
                    this.remotingClient.invokeOneway(addr, request, timeoutMillis);
                    return null;
                case CommunicationMode.ASYNC:
                    AtomicInteger times = new AtomicInteger();
                    long costTimeAsync = Sys.currentTimeMillis() - beginStartTime;
                    if (timeoutMillis < costTimeAsync)
                    {
                        throw new RemotingTooMuchRequestException("sendMessage call timeout");
                    }
                    this.sendMessageAsync(addr, brokerName, msg, timeoutMillis - costTimeAsync, request, sendCallback, topicPublishInfo, instance,
                        retryTimesWhenSendFailed, times, context, producer);
                    return null;
                case CommunicationMode.SYNC:
                    long costTimeSync = Sys.currentTimeMillis() - beginStartTime;
                    if (timeoutMillis < costTimeSync)
                    {
                        throw new RemotingTooMuchRequestException("sendMessage call timeout");
                    }
                    return this.sendMessageSync(addr, brokerName, msg, timeoutMillis - costTimeSync, request);
                default:
                    //assert false;
                    Debug.Assert(false);
                    break;
            }

            return null;
        }

        ///<exception cref="RemotingException"/>
        ///<exception cref="MQBrokerException"/>
        ///<exception cref="InterruptedException"/>
        private SendResult sendMessageSync(
        String addr,
        String brokerName,
        Message msg,
        long timeoutMillis,
        RemotingCommand request
    )
        {
            RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);
            //assert response != null;
            Debug.Assert(response != null);
            return this.processSendResponse(brokerName, msg, response, addr);
        }

        ///<exception cref="RemotingException"/>
        ///<exception cref="InterruptedException"/>
        private void sendMessageAsync(
        String addr,
        String brokerName,
        Message msg,
        long timeoutMillis,
        RemotingCommand request,
        SendCallback sendCallback,
        TopicPublishInfo topicPublishInfo,
        MQClientInstance instance,
        int retryTimesWhenSendFailed,
        AtomicInteger times,
        SendMessageContext context,
        DefaultMQProducerImpl producer
    )
        {
            long beginStartTime = Sys.currentTimeMillis();
            try
            {
                this.remotingClient.invokeAsync(addr, request, timeoutMillis, new InvokeCallback()
                {

                    OperationComplete = (responseFuture) =>
                    {
                        long cost = Sys.currentTimeMillis() - beginStartTime;
                        RemotingCommand response = responseFuture.getResponseCommand();
                        if (null == sendCallback && response != null)
                        {

                            try
                            {
                                SendResult sendResult = /*MQClientAPIImpl.this.*/processSendResponse(brokerName, msg, response, addr);
                                if (context != null && sendResult != null)
                                {
                                    context.setSendResult(sendResult);
                                    context.getProducer().executeSendMessageHookAfter(context);
                                }
                            }
                            catch (Exception e)
                            {
                            }

                            producer.updateFaultItem(brokerName, Sys.currentTimeMillis() - responseFuture.getBeginTimestamp(), false);
                            return;
                        }

                        if (response != null)
                        {
                            try
                            {
                                SendResult sendResult = /*MQClientAPIImpl.this.*/processSendResponse(brokerName, msg, response, addr);
                                //assert sendResult != null;
                                Debug.Assert(sendResult != null);
                                if (context != null)
                                {
                                    context.setSendResult(sendResult);
                                    context.getProducer().executeSendMessageHookAfter(context);
                                }

                                try
                                {
                                    sendCallback.OnSuccess(sendResult);
                                }
                                catch (Exception e)
                                {
                                }

                                producer.updateFaultItem(brokerName, Sys.currentTimeMillis() - responseFuture.getBeginTimestamp(), false);
                            }
                            catch (Exception e)
                            {
                                producer.updateFaultItem(brokerName, Sys.currentTimeMillis() - responseFuture.getBeginTimestamp(), true);
                                onExceptionImpl(brokerName, msg, timeoutMillis - cost, request, sendCallback, topicPublishInfo, instance,
                                    retryTimesWhenSendFailed, times, e, context, false, producer);
                            }
                        }
                        else
                        {
                            producer.updateFaultItem(brokerName, Sys.currentTimeMillis() - responseFuture.getBeginTimestamp(), true);
                            if (!responseFuture.isSendRequestOK())
                            {
                                MQClientException ex = new MQClientException("send request failed", responseFuture.getCause());
                                onExceptionImpl(brokerName, msg, timeoutMillis - cost, request, sendCallback, topicPublishInfo, instance,
                                    retryTimesWhenSendFailed, times, ex, context, true, producer);
                            }
                            else if (responseFuture.isTimeout())
                            {
                                MQClientException ex = new MQClientException("wait response timeout " + responseFuture.getTimeoutMillis() + "ms",
                                    responseFuture.getCause());
                                onExceptionImpl(brokerName, msg, timeoutMillis - cost, request, sendCallback, topicPublishInfo, instance,
                                    retryTimesWhenSendFailed, times, ex, context, true, producer);
                            }
                            else
                            {
                                MQClientException ex = new MQClientException("unknow reseaon", responseFuture.getCause());
                                onExceptionImpl(brokerName, msg, timeoutMillis - cost, request, sendCallback, topicPublishInfo, instance,
                                    retryTimesWhenSendFailed, times, ex, context, true, producer);
                            }
                        }
                    }
                });
            }
            catch (Exception ex)
            {
                long cost = Sys.currentTimeMillis() - beginStartTime;
                producer.updateFaultItem(brokerName, cost, true);
                onExceptionImpl(brokerName, msg, timeoutMillis - cost, request, sendCallback, topicPublishInfo, instance,
                        retryTimesWhenSendFailed, times, ex, context, true, producer);
            }
        }

        private void onExceptionImpl(String brokerName,
         Message msg,
         long timeoutMillis,
         RemotingCommand request,
         SendCallback sendCallback,
         TopicPublishInfo topicPublishInfo,
         MQClientInstance instance,
         int timesTotal,
         AtomicInteger curTimes,
         Exception e,
         SendMessageContext context,
         bool needRetry,
         DefaultMQProducerImpl producer
    )
        {
            int tmp = curTimes.getAndIncrement();
            if (needRetry && tmp <= timesTotal)
            {
                String retryBrokerName = brokerName;//by default, it will send to the same broker
                if (topicPublishInfo != null)
                { //select one message queue accordingly, in order to determine which broker to send
                    MessageQueue mqChosen = producer.selectOneMessageQueue(topicPublishInfo, brokerName);
                    retryBrokerName = mqChosen.getBrokerName();
                }
                String addr = instance.findBrokerAddressInPublish(retryBrokerName);
                log.Warn($"async send msg by retry {tmp} times. topic={msg.getTopic()}, brokerAddr={addr}, brokerName={retryBrokerName}", e.ToString());
                try
                {
                    request.setOpaque(RemotingCommand.createNewRequestId());
                    sendMessageAsync(addr, retryBrokerName, msg, timeoutMillis, request, sendCallback, topicPublishInfo, instance,
                        timesTotal, curTimes, context, producer);
                }
                catch (ThreadInterruptedException e1)
                {
                    onExceptionImpl(retryBrokerName, msg, timeoutMillis, request, sendCallback, topicPublishInfo, instance, timesTotal, curTimes, e1,
                        context, false, producer);
                }
                catch (RemotingTooMuchRequestException e1)
                {
                    onExceptionImpl(retryBrokerName, msg, timeoutMillis, request, sendCallback, topicPublishInfo, instance, timesTotal, curTimes, e1,
                        context, false, producer);
                }
                catch (RemotingException e1)
                {
                    producer.updateFaultItem(brokerName, 3000, true);
                    onExceptionImpl(retryBrokerName, msg, timeoutMillis, request, sendCallback, topicPublishInfo, instance, timesTotal, curTimes, e1,
                        context, true, producer);
                }
            }
            else
            {

                if (context != null)
                {
                    context.setException(e);
                    context.getProducer().executeSendMessageHookAfter(context);
                }

                try
                {
                    sendCallback.OnException(e);
                }
                catch (Exception ignored)
                {
                }
            }
        }

        ///<exception cref="MQBrokerException"/>
        ///<exception cref="RemotingCommandException"/>
        private SendResult processSendResponse(
    String brokerName,
    Message msg,
    RemotingCommand response,
    String addr

)
        {
            SendStatus sendStatus;
            switch (response.getCode())
            {
                case ResponseCode.FLUSH_DISK_TIMEOUT:
                    {
                        sendStatus = SendStatus.FLUSH_DISK_TIMEOUT;
                        break;
                    }
                case ResponseCode.FLUSH_SLAVE_TIMEOUT:
                    {
                        sendStatus = SendStatus.FLUSH_SLAVE_TIMEOUT;
                        break;
                    }
                case ResponseCode.SLAVE_NOT_AVAILABLE:
                    {
                        sendStatus = SendStatus.SLAVE_NOT_AVAILABLE;
                        break;
                    }
                case ResponseCode.SUCCESS:
                    {
                        sendStatus = SendStatus.SEND_OK;
                        break;
                    }
                default:
                    {
                        throw new MQBrokerException(response.getCode(), response.getRemark(), addr);
                    }
            }

            SendMessageResponseHeader responseHeader = response.decodeCommandCustomHeader<SendMessageResponseHeader>();

            //If namespace not null , reset Topic without namespace.
            String topic = msg.getTopic();
            if (UtilAll.isNotEmpty(this.clientConfig.getNamespace()))
            {
                topic = NamespaceUtil.withoutNamespace(topic, this.clientConfig.getNamespace());
            }

            MessageQueue messageQueue = new MessageQueue(topic, brokerName, responseHeader.queueId);

            String uniqMsgId = MessageClientIDSetter.getUniqID(msg);
            if (msg is MessageBatch)
            {
                StringBuilder sb = new StringBuilder();
                foreach (Message message in (MessageBatch)msg)
                {
                    sb.Append(sb.Length == 0 ? "" : ",").Append(MessageClientIDSetter.getUniqID(message));
                }
                uniqMsgId = sb.ToString();
            }
            SendResult sendResult = new SendResult(sendStatus, uniqMsgId, responseHeader.msgId, messageQueue, responseHeader.queueOffset);
            sendResult.setTransactionId(responseHeader.transactionId);
            String regionId = response.getExtFields().get(MessageConst.PROPERTY_MSG_REGION);
            String traceOn = response.getExtFields().get(MessageConst.PROPERTY_TRACE_SWITCH);
            if (regionId == null || regionId.isEmpty())
            {
                regionId = MixAll.DEFAULT_TRACE_REGION_ID;
            }
            if (traceOn != null && traceOn.Equals("false"))
            {
                sendResult.setTraceOn(false);
            }
            else
            {
                sendResult.setTraceOn(true);
            }
            sendResult.setRegionId(regionId);
            return sendResult;
        }

        ///<exception cref="RemotingException"/>
        ///<exception cref="MQBrokerException"/>
        ///<exception cref="InterruptedException"/>
        public PullResult pullMessage(
        String addr,
        PullMessageRequestHeader requestHeader,
        long timeoutMillis,
        CommunicationMode communicationMode,
        PullCallback pullCallback
    )
        {
            RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.PULL_MESSAGE, requestHeader);

            switch (communicationMode)
            {
                case CommunicationMode.ONEWAY:
                    //assert false;
                    Debug.Assert(false);
                    return null;
                case CommunicationMode.ASYNC:
                    this.pullMessageAsync(addr, request, timeoutMillis, pullCallback);
                    return null;
                case CommunicationMode.SYNC:
                    return this.pullMessageSync(addr, request, timeoutMillis);
                default:
                    //assert false;
                    Debug.Assert(false);
                    break;
            }

            return null;
        }


        ///<exception cref="RemotingException"/>
        ///<exception cref="InterruptedException"/>
        private void pullMessageAsync(string addr, RemotingCommand request, long timeoutMillis, PullCallback pullCallback)
        {
            this.remotingClient.invokeAsync(addr, request, timeoutMillis, new InvokeCallback()
            {

                OperationComplete = (responseFuture) =>
                //public void operationComplete(ResponseFuture responseFuture)
                {
                    RemotingCommand response = responseFuture.getResponseCommand();
                    if (response != null)
                    {
                        try
                        {
                            PullResult pullResult = processPullResponse(response, addr);
                            //assert pullResult != null;
                            Debug.Assert(pullResult != null);
                            //pullCallback.onSuccess(pullResult);
                            pullCallback.OnSuccess(pullResult);
                        }
                        catch (Exception e)
                        {
                            //pullCallback.onException(e);
                            pullCallback.OnException(e);
                        }
                    }
                    else
                    {
                        if (!responseFuture.isSendRequestOK())
                        {
                            pullCallback.OnException(new MQClientException("send request failed to " + addr + ". Request: " + request, responseFuture.getCause()));
                        }
                        else if (responseFuture.isTimeout())
                        {
                            pullCallback.OnException(new MQClientException("wait response from " + addr + " timeout :" + responseFuture.getTimeoutMillis() + "ms" + ". Request: " + request,
                                responseFuture.getCause()));
                        }
                        else
                        {
                            pullCallback.OnException(new MQClientException("unknown reason. addr: " + addr + ", timeoutMillis: " + timeoutMillis + ". Request: " + request, responseFuture.getCause()));
                        }
                    }
                }
            });
        }

        ///<exception cref="RemotingException"/>
        ///<exception cref="InterruptedException"/>
        ///<exception cref="MQBrokerException"/>
        private PullResult pullMessageSync(
        String addr,
        RemotingCommand request,
        long timeoutMillis
    )
        {
            RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);
            //assert response != null;
            Debug.Assert(response != null);
            return this.processPullResponse(response, addr);
        }

        ///<exception cref="RemotingCommandException"/>
        ///<exception cref="MQBrokerException"/>
        private PullResult processPullResponse(
        RemotingCommand response,
        String addr)
        {
            PullStatus pullStatus = PullStatus.NO_NEW_MSG;
            switch (response.getCode())
            {
                case ResponseCode.SUCCESS:
                    pullStatus = PullStatus.FOUND;
                    break;
                case ResponseCode.PULL_NOT_FOUND:
                    pullStatus = PullStatus.NO_NEW_MSG;
                    break;
                case ResponseCode.PULL_RETRY_IMMEDIATELY:
                    pullStatus = PullStatus.NO_MATCHED_MSG;
                    break;
                case ResponseCode.PULL_OFFSET_MOVED:
                    pullStatus = PullStatus.OFFSET_ILLEGAL;
                    break;

                default:
                    throw new MQBrokerException(response.getCode(), response.getRemark(), addr);
            }

            PullMessageResponseHeader responseHeader = response.decodeCommandCustomHeader<PullMessageResponseHeader>();

            return new PullResultExt(pullStatus, responseHeader.nextBeginOffset, responseHeader.minOffset,
                responseHeader.maxOffset, null, responseHeader.suggestWhichBrokerId, response.getBody());
        }

        ///<exception cref="RemotingException"/>
        ///<exception cref="MQBrokerException"/>
        ///<exception cref="InterruptedException"/>
        public MessageExt viewMessage(String addr, long phyoffset, long timeoutMillis)
        {
            ViewMessageRequestHeader requestHeader = new ViewMessageRequestHeader();
            requestHeader.offset = phyoffset;
            RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.VIEW_MESSAGE_BY_ID, requestHeader);

            RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr),
                request, timeoutMillis);
            //assert response != null;
            Debug.Assert(response != null);
            switch (response.getCode())
            {
                case ResponseCode.SUCCESS:
                    {
                        ByteBuffer byteBuffer = ByteBuffer.wrap(response.getBody());
                        MessageExt messageExt = MessageDecoder.clientDecode(byteBuffer, true);
                        //If namespace not null , reset Topic without namespace.
                        if (UtilAll.isNotEmpty(this.clientConfig.getNamespace()))
                        {
                            messageExt.setTopic(NamespaceUtil.withoutNamespace(messageExt.getTopic(), this.clientConfig.getNamespace()));
                        }
                        return messageExt;
                    }
                default:
                    break;
            }

            throw new MQBrokerException(response.getCode(), response.getRemark(), addr);
        }

        ///<exception cref="RemotingException"/>
        ///<exception cref="MQBrokerException"/>
        ///<exception cref="InterruptedException"/>
        public long searchOffset(String addr, String topic, int queueId, long timestamp,
        long timeoutMillis)
        {
            SearchOffsetRequestHeader requestHeader = new SearchOffsetRequestHeader();
            requestHeader.topic = topic;
            requestHeader.queueId = queueId;
            requestHeader.timestamp = timestamp;
            RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.SEARCH_OFFSET_BY_TIMESTAMP, requestHeader);

            RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr),
                request, timeoutMillis);
            //assert response != null;
            Debug.Assert(response != null);
            switch (response.getCode())
            {
                case ResponseCode.SUCCESS:
                    {
                        SearchOffsetResponseHeader responseHeader = response.decodeCommandCustomHeader<SearchOffsetResponseHeader>();
                        return responseHeader.offset;
                    }
                default:
                    break;
            }

            throw new MQBrokerException(response.getCode(), response.getRemark(), addr);
        }

        ///<exception cref="RemotingException"/>
        ///<exception cref="MQBrokerException"/>
        ///<exception cref="InterruptedException"/>
        public long getMaxOffset(String addr, String topic, int queueId, long timeoutMillis)
        {
            GetMaxOffsetRequestHeader requestHeader = new GetMaxOffsetRequestHeader();
            requestHeader.topic = topic;
            requestHeader.queueId = queueId;
            RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_MAX_OFFSET, requestHeader);

            RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr),
                request, timeoutMillis);
            //assert response != null;
            Debug.Assert(response != null);
            switch (response.getCode())
            {
                case ResponseCode.SUCCESS:
                    {
                        GetMaxOffsetResponseHeader responseHeader = response.decodeCommandCustomHeader<GetMaxOffsetResponseHeader>();

                        return responseHeader.offset;
                    }
                default:
                    break;
            }

            throw new MQBrokerException(response.getCode(), response.getRemark(), addr);
        }

        ///<exception cref="RemotingConnectException"/>
        ///<exception cref="RemotingSendRequestException"/>
        ///<exception cref="RemotingTimeoutException"/>
        ///<exception cref="MQBrokerException"/>
        ///<exception cref="InterruptedException"/>
        public List<String> getConsumerIdListByGroup(
        String addr,
        String consumerGroup,
        long timeoutMillis)
        {
            GetConsumerListByGroupRequestHeader requestHeader = new GetConsumerListByGroupRequestHeader();
            requestHeader.consumerGroup = consumerGroup;
            RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_CONSUMER_LIST_BY_GROUP, requestHeader);

            RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr),
                request, timeoutMillis);
            //assert response != null;
            Debug.Assert(response != null);
            switch (response.getCode())
            {
                case ResponseCode.SUCCESS:
                    {
                        if (response.getBody() != null)
                        {
                            GetConsumerListByGroupResponseBody body =
                                RemotingSerializable.decode<GetConsumerListByGroupResponseBody>(response.getBody());
                            return body.consumerIdList;
                        }
                        break;
                    }
                default:
                    break;
            }

            throw new MQBrokerException(response.getCode(), response.getRemark(), addr);
        }


        ///<exception cref="RemotingException"/>
        ///<exception cref="MQBrokerException"/>
        ///<exception cref="InterruptedException"/>
        public long getMinOffset(String addr, String topic, int queueId, long timeoutMillis)
        {
            GetMinOffsetRequestHeader requestHeader = new GetMinOffsetRequestHeader();
            requestHeader.topic = topic;
            requestHeader.queueId = queueId;
            RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_MIN_OFFSET, requestHeader);

            RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr),
                request, timeoutMillis);
            //assert response != null;
            Debug.Assert(response != null);
            switch (response.getCode())
            {
                case ResponseCode.SUCCESS:
                    {
                        GetMinOffsetResponseHeader responseHeader =
                            response.decodeCommandCustomHeader<GetMinOffsetResponseHeader>(/*.class*/);

                        return responseHeader.offset;
                    }
                default:
                    break;
            }

            throw new MQBrokerException(response.getCode(), response.getRemark(), addr);
        }

        ///<exception cref="RemotingException"/>
        ///<exception cref="MQBrokerException"/>
        ///<exception cref="InterruptedException"/>
        public long getEarliestMsgStoretime(String addr, String topic, int queueId,
        long timeoutMillis)
        {
            GetEarliestMsgStoretimeRequestHeader requestHeader = new GetEarliestMsgStoretimeRequestHeader();
            requestHeader.topic = topic;
            requestHeader.queueId = queueId;
            RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_EARLIEST_MSG_STORETIME, requestHeader);

            RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr),
                request, timeoutMillis);
            //assert response != null;
            Debug.Assert(response != null);
            switch (response.getCode())
            {
                case ResponseCode.SUCCESS:
                    {
                        GetEarliestMsgStoretimeResponseHeader responseHeader =
                            response.decodeCommandCustomHeader<GetEarliestMsgStoretimeResponseHeader>(/*.class*/);

                        return responseHeader.timestamp;
                    }
                default:
                    break;
            }

            throw new MQBrokerException(response.getCode(), response.getRemark(), addr);
        }

        ///<exception cref="RemotingException"/>
        ///<exception cref="MQBrokerException"/>
        ///<exception cref="InterruptedException"/>
        public long queryConsumerOffset(
        String addr,
        QueryConsumerOffsetRequestHeader requestHeader,
        long timeoutMillis
    )
        {
            RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.QUERY_CONSUMER_OFFSET, requestHeader);

            RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr),
                request, timeoutMillis);
            //assert response != null;
            Debug.Assert(response != null);
            switch (response.getCode())
            {
                case ResponseCode.SUCCESS:
                    {
                        QueryConsumerOffsetResponseHeader responseHeader =
                            response.decodeCommandCustomHeader<QueryConsumerOffsetResponseHeader>(/*.class*/);

                        return responseHeader.offset;
                    }
                default:
                    break;
            }

            throw new MQBrokerException(response.getCode(), response.getRemark(), addr);
        }

        ///<exception cref="RemotingException"/>
        ///<exception cref="MQBrokerException"/>
        ///<exception cref="InterruptedException"/>
        public void updateConsumerOffset(
        String addr,
        UpdateConsumerOffsetRequestHeader requestHeader,
        long timeoutMillis
    )
        {
            RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.UPDATE_CONSUMER_OFFSET, requestHeader);

            RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr),
                request, timeoutMillis);
            //assert response != null;
            Debug.Assert(response != null);
            switch (response.getCode())
            {
                case ResponseCode.SUCCESS:
                    {
                        return;
                    }
                default:
                    break;
            }

            throw new MQBrokerException(response.getCode(), response.getRemark(), addr);
        }

        ///<exception cref="RemotingConnectException"/>
        ///<exception cref="RemotingTooMuchRequestException"/>
        ///<exception cref="RemotingTimeoutException"/>
        ///<exception cref="RemotingSendRequestException"/>
        ///<exception cref="InterruptedException"/>
        public void updateConsumerOffsetOneway(
        String addr,
        UpdateConsumerOffsetRequestHeader requestHeader,
        long timeoutMillis
    )
        {
            RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.UPDATE_CONSUMER_OFFSET, requestHeader);

            this.remotingClient.invokeOneway(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr), request, timeoutMillis);
        }

        ///<exception cref="RemotingException"/>
        ///<exception cref="MQBrokerException"/>
        ///<exception cref="InterruptedException"/>
        public int sendHearbeat(
        String addr,
        HeartbeatData heartbeatData,
        long timeoutMillis
    )
        {
            RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.HEART_BEAT, null);
            request.setLanguage(clientConfig.getLanguage());
            request.setBody(heartbeatData.encode());
            RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);
            //assert response != null;
            Debug.Assert(response != null);
            switch (response.getCode())
            {
                case ResponseCode.SUCCESS:
                    {
                        return response.getVersion();
                    }
                default:
                    break;
            }

            throw new MQBrokerException(response.getCode(), response.getRemark(), addr);
        }
        ///<exception cref="RemotingException"/>
        ///<exception cref="MQBrokerException"/>
        ///<exception cref="InterruptedException"/>

        public void unregisterClient(
        String addr,
        String clientID,
        String producerGroup,
        String consumerGroup,
        long timeoutMillis
    )
        {
            UnregisterClientRequestHeader requestHeader = new UnregisterClientRequestHeader();
            requestHeader.clientID = clientID;
            requestHeader.producerGroup = producerGroup;
            requestHeader.consumerGroup = consumerGroup;
            RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.UNREGISTER_CLIENT, requestHeader);

            RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);
            //assert response != null;
            Debug.Assert(response != null);
            switch (response.getCode())
            {
                case ResponseCode.SUCCESS:
                    {
                        return;
                    }
                default:
                    break;
            }

            throw new MQBrokerException(response.getCode(), response.getRemark(), addr);
        }

        ///<exception cref="RemotingException"/>
        ///<exception cref="MQBrokerException"/>
        ///<exception cref="InterruptedException"/>
        public void endTransactionOneway(
        String addr,
        EndTransactionRequestHeader requestHeader,
        String remark,
        long timeoutMillis
    )
        {
            RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.END_TRANSACTION, requestHeader);

            request.setRemark(remark);
            this.remotingClient.invokeOneway(addr, request, timeoutMillis);
        }

        ///<exception cref="RemotingException"/>
        ///<exception cref="MQBrokerException"/>
        ///<exception cref="InterruptedException"/>
        public void queryMessage(
        String addr,
        QueryMessageRequestHeader requestHeader,
        long timeoutMillis,
        InvokeCallback invokeCallback,
        bool isUnqiueKey
    )
        {
            RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.QUERY_MESSAGE, requestHeader);
            request.addExtField(MixAll.UNIQUE_MSG_QUERY_FLAG, isUnqiueKey.ToString());
            this.remotingClient.invokeAsync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr), request, timeoutMillis,
                invokeCallback);
        }

        ///<exception cref="RemotingException"/>
        ///<exception cref="InterruptedException"/>
        public bool registerClient(String addr, HeartbeatData heartbeat, long timeoutMillis)
        {
            RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.HEART_BEAT, null);

            request.setBody(heartbeat.encode());
            RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);
            return response.getCode() == ResponseCode.SUCCESS;
        }

        ///<exception cref="RemotingException"/>
        ///<exception cref="MQBrokerException"/>
        ///<exception cref="InterruptedException"/>
        public void consumerSendMessageBack(
        String addr,
        MessageExt msg,
        String consumerGroup,
        int delayLevel,
        long timeoutMillis,
        int maxConsumeRetryTimes
    )
        {
            ConsumerSendMsgBackRequestHeader requestHeader = new ConsumerSendMsgBackRequestHeader();
            RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.CONSUMER_SEND_MSG_BACK, requestHeader);

            requestHeader.group = consumerGroup;
            requestHeader.originTopic = msg.getTopic();
            requestHeader.offset = msg.getCommitLogOffset();
            requestHeader.delayLevel = delayLevel;
            requestHeader.originMsgId = msg.getMsgId();
            requestHeader.maxReconsumeTimes = (maxConsumeRetryTimes);

            RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr),
                request, timeoutMillis);
            //assert response != null;
            Debug.Assert(response != null);
            switch (response.getCode())
            {
                case ResponseCode.SUCCESS:
                    {
                        return;
                    }
                default:
                    break;
            }

            throw new MQBrokerException(response.getCode(), response.getRemark(), addr);
        }

        ///<exception cref="RemotingException"/>
        ///<exception cref="MQBrokerException"/>
        ///<exception cref="InterruptedException"/>
        public HashSet<MessageQueue> lockBatchMQ(
        String addr,
        LockBatchRequestBody requestBody,
        long timeoutMillis)
        {
            RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.LOCK_BATCH_MQ, null);

            request.setBody(requestBody.encode());
            RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr),
                request, timeoutMillis);
            switch (response.getCode())
            {
                case ResponseCode.SUCCESS:
                    {
                        LockBatchResponseBody responseBody =
                            RemotingSerializable.decode<LockBatchResponseBody>(response.getBody()/*.class*/);
                        HashSet<MessageQueue> messageQueues = responseBody.lockOKMQSet;
                        return messageQueues;
                    }
                default:
                    break;
            }

            throw new MQBrokerException(response.getCode(), response.getRemark(), addr);
        }

        ///<exception cref="RemotingException"/>
        ///<exception cref="MQBrokerException"/>
        ///<exception cref="InterruptedException"/>
        public void unlockBatchMQ(
        String addr,
        UnlockBatchRequestBody requestBody,
        long timeoutMillis,
        bool oneway
    )
        {
            RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.UNLOCK_BATCH_MQ, null);

            request.setBody(requestBody.encode());

            if (oneway)
            {
                this.remotingClient.invokeOneway(addr, request, timeoutMillis);
            }
            else
            {
                RemotingCommand response = this.remotingClient
                    .invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr), request, timeoutMillis);
                switch (response.getCode())
                {
                    case ResponseCode.SUCCESS:
                        {
                            return;
                        }
                    default:
                        break;
                }

                throw new MQBrokerException(response.getCode(), response.getRemark(), addr);
            }
        }

        ///<exception cref="RemotingTimeoutException"/>
        ///<exception cref="RemotingSendRequestException"/>
        ///<exception cref="RemotingConnectException"/>
        ///<exception cref="MQBrokerException"/>
        public TopicStatsTable getTopicStatsInfo(String addr, String topic,
        long timeoutMillis)
        {
            GetTopicStatsInfoRequestHeader requestHeader = new GetTopicStatsInfoRequestHeader();
            requestHeader.topic = topic;

            RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_TOPIC_STATS_INFO, requestHeader);

            RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr),
                request, timeoutMillis);
            switch (response.getCode())
            {
                case ResponseCode.SUCCESS:
                    {
                        TopicStatsTable topicStatsTable = RemotingSerializable.decode<TopicStatsTable>(response.getBody()/*.class*/);
                        return topicStatsTable;
                    }
                default:
                    break;
            }

            throw new MQBrokerException(response.getCode(), response.getRemark(), addr);
        }

        ///<exception cref="InterruptedException"/>
        ///<exception cref="RemotingTimeoutException"/>
        ///<exception cref="RemotingSendRequestException"/>
        ///<exception cref="RemotingConnectException"/>
        ///<exception cref="MQBrokerException"/>
        public ConsumeStats getConsumeStats(String addr, String consumerGroup, long timeoutMillis)
        {
            return getConsumeStats(addr, consumerGroup, null, timeoutMillis);
        }

        ///<exception cref="InterruptedException"/>
        ///<exception cref="RemotingTimeoutException"/>
        ///<exception cref="RemotingSendRequestException"/>
        ///<exception cref="RemotingConnectException"/>
        ///<exception cref="MQBrokerException"/>
        public ConsumeStats getConsumeStats(String addr, String consumerGroup, String topic, long timeoutMillis)
        {
            GetConsumeStatsRequestHeader requestHeader = new GetConsumeStatsRequestHeader();
            requestHeader.consumerGroup = consumerGroup;
            requestHeader.topic = topic;

            RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_CONSUME_STATS, requestHeader);

            RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr),
                request, timeoutMillis);
            switch (response.getCode())
            {
                case ResponseCode.SUCCESS:
                    {
                        ConsumeStats consumeStats = RemotingSerializable.decode<ConsumeStats>(response.getBody()/*.class*/);
                        return consumeStats;
                    }
                default:
                    break;
            }

            throw new MQBrokerException(response.getCode(), response.getRemark(), addr);
        }

        ///<exception cref="RemotingConnectException"/>
        ///<exception cref="RemotingSendRequestException"/>
        ///<exception cref="RemotingTimeoutException"/>
        ///<exception cref="InterruptedException"/>
        ///<exception cref="MQBrokerException"/>
        public ProducerConnection getProducerConnectionList(String addr, String producerGroup, long timeoutMillis)
        {
            GetProducerConnectionListRequestHeader requestHeader = new GetProducerConnectionListRequestHeader();
            requestHeader.producerGroup = producerGroup;

            RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_PRODUCER_CONNECTION_LIST, requestHeader);

            RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr),
                request, timeoutMillis);
            switch (response.getCode())
            {
                case ResponseCode.SUCCESS:
                    {
                        return ProducerConnection.decode<ProducerConnection>(response.getBody()/*.class*/);
                    }
                default:
                    break;
            }

            throw new MQBrokerException(response.getCode(), response.getRemark(), addr);
        }

        ///<exception cref="RemotingConnectException"/>
        ///<exception cref="RemotingSendRequestException"/>
        ///<exception cref="RemotingTimeoutException"/>
        ///<exception cref="InterruptedException"/>
        ///<exception cref="MQBrokerException"/>
        public ConsumerConnection getConsumerConnectionList(String addr, String consumerGroup, long timeoutMillis)
        {
            GetConsumerConnectionListRequestHeader requestHeader = new GetConsumerConnectionListRequestHeader();
            requestHeader.consumerGroup = consumerGroup;

            RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_CONSUMER_CONNECTION_LIST, requestHeader);

            RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr),
                request, timeoutMillis);
            switch (response.getCode())
            {
                case ResponseCode.SUCCESS:
                    {
                        return ConsumerConnection.decode<ConsumerConnection>(response.getBody()/*.class*/);
                    }
                default:
                    break;
            }

            throw new MQBrokerException(response.getCode(), response.getRemark(), addr);
        }

        ///<exception cref="RemotingConnectException"/>
        ///<exception cref="RemotingSendRequestException"/>
        ///<exception cref="RemotingTimeoutException"/>
        ///<exception cref="InterruptedException"/>
        ///<exception cref="MQBrokerException"/>
        public KVTable getBrokerRuntimeInfo(String addr, long timeoutMillis)
        {

            RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_BROKER_RUNTIME_INFO, null);

            RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr),
                request, timeoutMillis);
            switch (response.getCode())
            {
                case ResponseCode.SUCCESS:
                    {
                        return KVTable.decode<KVTable>(response.getBody()/*.class*/);
                    }
                default:
                    break;
            }

            throw new MQBrokerException(response.getCode(), response.getRemark(), addr);
        }

        ///<exception cref="RemotingConnectException"/>
        ///<exception cref="RemotingSendRequestException"/>
        ///<exception cref="RemotingTimeoutException"/>
        ///<exception cref="InterruptedException"/>
        ///<exception cref="MQBrokerException"/>
        ///<exception cref="UnsupportedEncodingException"/>

        public void updateBrokerConfig(String addr, Properties properties, long timeoutMillis)
        {

            RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.UPDATE_BROKER_CONFIG, null);

            string str = MixAll.properties2String(properties);
            if (str != null && str.Length > 0)
            {
                request.setBody(str.getBytes(MixAll.DEFAULT_CHARSET));
                RemotingCommand response = this.remotingClient
                    .invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr), request, timeoutMillis);
                switch (response.getCode())
                {
                    case ResponseCode.SUCCESS:
                        {
                            return;
                        }
                    default:
                        break;
                }

                throw new MQBrokerException(response.getCode(), response.getRemark(), addr);
            }
        }

        ///<exception cref="RemotingConnectException"/>
        ///<exception cref="RemotingSendRequestException"/>
        ///<exception cref="RemotingTimeoutException"/>
        ///<exception cref="InterruptedException"/>
        ///<exception cref="MQBrokerException"/>
        ///<exception cref="UnsupportedEncodingException"/>
        public Properties getBrokerConfig(String addr, long timeoutMillis)
        {
            RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_BROKER_CONFIG, null);

            RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);
            //assert response != null;
            Debug.Assert(response != null);
            switch (response.getCode())
            {
                case ResponseCode.SUCCESS:
                    {
                        //return MixAll.string2Properties(new String(response.getBody(), MixAll.DEFAULT_CHARSET));
                        return MixAll.string2Properties(MixAll.DEFAULT_CHARSET.GetString(response.getBody()));
                    }
                default:
                    break;
            }

            throw new MQBrokerException(response.getCode(), response.getRemark(), addr);
        }

        ///<exception cref="InterruptedException"/>
        ///<exception cref="RemotingTimeoutException"/>
        ///<exception cref="RemotingSendRequestException"/>
        ///<exception cref="RemotingConnectException"/>
        ///<exception cref="MQBrokerException"/>
        public ClusterInfo getBrokerClusterInfo(long timeoutMillis)
        {
            RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_BROKER_CLUSTER_INFO, null);

            RemotingCommand response = this.remotingClient.invokeSync(null, request, timeoutMillis);
            //assert response != null;
            Debug.Assert(response != null);
            switch (response.getCode())
            {
                case ResponseCode.SUCCESS:
                    {
                        return ClusterInfo.decode<ClusterInfo>(response.getBody()/*.class*/);
                    }
                default:
                    break;
            }

            throw new MQBrokerException(response.getCode(), response.getRemark());
        }

        ///<exception cref="RemotingException"/>
        ///<exception cref="MQClientException"/>
        ///<exception cref="InterruptedException"/>
        public TopicRouteData getDefaultTopicRouteInfoFromNameServer(String topic, long timeoutMillis)
        {

            return getTopicRouteInfoFromNameServer(topic, timeoutMillis, false);
        }

        ///<exception cref="RemotingException"/>
        ///<exception cref="MQClientException"/>
        ///<exception cref="InterruptedException"/>
        public TopicRouteData getTopicRouteInfoFromNameServer(String topic, long timeoutMillis)
        {

            return getTopicRouteInfoFromNameServer(topic, timeoutMillis, true);
        }

        ///<exception cref="MQClientException"/>
        ///<exception cref="InterruptedException"/>
        ///<exception cref="RemotingTimeoutException"/>
        ///<exception cref="RemotingSendRequestException"/>
        ///<exception cref="RemotingConnectException"/>
        public TopicRouteData getTopicRouteInfoFromNameServer(String topic, long timeoutMillis, bool allowTopicNotExist)
        {
            GetRouteInfoRequestHeader requestHeader = new GetRouteInfoRequestHeader();
            requestHeader.topic = topic;

            RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_ROUTEINFO_BY_TOPIC, requestHeader);

            RemotingCommand response = this.remotingClient.invokeSync(null, request, timeoutMillis);
            //assert response != null;
            Debug.Assert(response != null);
            switch (response.getCode())
            {
                case ResponseCode.TOPIC_NOT_EXIST:
                    {
                        if (allowTopicNotExist)
                        {
                            log.Warn("get Topic [{}] RouteInfoFromNameServer is not exist value", topic);
                        }

                        break;
                    }
                case ResponseCode.SUCCESS:
                    {
                        byte[] body = response.getBody();
                        if (body != null)
                        {
                            return TopicRouteData.decode<TopicRouteData>(body/*.class*/);
                        }
                        break; //???
                    }
                default:
                    break;
            }


            throw new MQClientException(response.getCode(), response.getRemark());
        }

        ///<exception cref="RemotingException"/>
        ///<exception cref="RemotingException"/>
        ///<exception cref="InterruptedException"/>
        public TopicList getTopicListFromNameServer(long timeoutMillis)
        {
            RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_ALL_TOPIC_LIST_FROM_NAMESERVER, null);

            RemotingCommand response = this.remotingClient.invokeSync(null, request, timeoutMillis);
            //assert response != null;
            Debug.Assert(response != null);
            switch (response.getCode())
            {
                case ResponseCode.SUCCESS:
                    {
                        byte[] body = response.getBody();
                        if (body != null)
                        {
                            return TopicList.decode<TopicList>(body/*.class*/);
                        }
                        break;//???
                    }
                default:
                    break;
            }

            throw new MQClientException(response.getCode(), response.getRemark());
        }

        ///<exception cref="RemotingCommandException"/>
        ///<exception cref="RemotingConnectException"/>
        ///<exception cref="RemotingSendRequestException"/>
        ///<exception cref="RemotingTimeoutException"/>
        ///<exception cref="InterruptedException"/>
        ///<exception cref="MQClientException"/>
        public int wipeWritePermOfBroker(String namesrvAddr, String brokerName, long timeoutMillis)
        {
            WipeWritePermOfBrokerRequestHeader requestHeader = new WipeWritePermOfBrokerRequestHeader();
            requestHeader.brokerName = brokerName;

            RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.WIPE_WRITE_PERM_OF_BROKER, requestHeader);

            RemotingCommand response = this.remotingClient.invokeSync(namesrvAddr, request, timeoutMillis);
            //assert response != null;
            Debug.Assert(response != null);
            switch (response.getCode())
            {
                case ResponseCode.SUCCESS:
                    {
                        WipeWritePermOfBrokerResponseHeader responseHeader =
                            response.decodeCommandCustomHeader<WipeWritePermOfBrokerResponseHeader>(/*.class*/);
                        return responseHeader.wipeTopicCount;
                    }
                default:
                    break;
            }

            throw new MQClientException(response.getCode(), response.getRemark());
        }

        ///<exception cref="RemotingCommandException"/>
        ///<exception cref="RemotingConnectException"/>
        ///<exception cref="RemotingSendRequestException"/>
        ///<exception cref="RemotingTimeoutException"/>
        ///<exception cref="InterruptedException"/>
        ///<exception cref="MQClientException"/>
        public int addWritePermOfBroker(String nameSrvAddr, String brokerName, long timeoutMillis)
        {
            AddWritePermOfBrokerRequestHeader requestHeader = new AddWritePermOfBrokerRequestHeader();
            requestHeader.brokerName = brokerName;

            RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.ADD_WRITE_PERM_OF_BROKER, requestHeader);

            RemotingCommand response = this.remotingClient.invokeSync(nameSrvAddr, request, timeoutMillis);
            //assert response != null;
            Debug.Assert(response != null);
            switch (response.getCode())
            {
                case ResponseCode.SUCCESS:
                    {
                        AddWritePermOfBrokerResponseHeader responseHeader =
                            response.decodeCommandCustomHeader<AddWritePermOfBrokerResponseHeader>(/*.class*/);
                        return responseHeader.addTopicCount;
                    }
                default:
                    break;
            }
            throw new MQClientException(response.getCode(), response.getRemark());
        }

        ///<exception cref="RemotingException"/>
        ///<exception cref="InterruptedException"/>
        ///<exception cref="MQClientException"/>
        public void deleteTopicInBroker(String addr, String topic, long timeoutMillis)
        {
            DeleteTopicRequestHeader requestHeader = new DeleteTopicRequestHeader();
            requestHeader.topic = topic;
            RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.DELETE_TOPIC_IN_BROKER, requestHeader);

            RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr),
                request, timeoutMillis);
            //assert response != null;
            Debug.Assert(response != null);
            switch (response.getCode())
            {
                case ResponseCode.SUCCESS:
                    {
                        return;
                    }
                default:
                    break;
            }

            throw new MQClientException(response.getCode(), response.getRemark());
        }

        ///<exception cref="RemotingException"/>
        ///<exception cref="InterruptedException"/>
        ///<exception cref="MQClientException"/>
        public void deleteTopicInNameServer(String addr, String topic, long timeoutMillis)
        {
            DeleteTopicFromNamesrvRequestHeader requestHeader = new DeleteTopicFromNamesrvRequestHeader();
            requestHeader.topic = topic;
            RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.DELETE_TOPIC_IN_NAMESRV, requestHeader);

            RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);
            //assert response != null;
            Debug.Assert(response != null);
            switch (response.getCode())
            {
                case ResponseCode.SUCCESS:
                    {
                        return;
                    }
                default:
                    break;
            }

            throw new MQClientException(response.getCode(), response.getRemark());
        }

        ///<exception cref="RemotingException"/>
        ///<exception cref="InterruptedException"/>
        ///<exception cref="MQClientException"/>
        public void deleteSubscriptionGroup(String addr, String groupName, bool removeOffset, long timeoutMillis)
        {
            DeleteSubscriptionGroupRequestHeader requestHeader = new DeleteSubscriptionGroupRequestHeader();
            requestHeader.groupName = groupName;
            requestHeader.removeOffset = removeOffset;
            RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.DELETE_SUBSCRIPTIONGROUP, requestHeader);

            RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr),
                request, timeoutMillis);
            //assert response != null;
            Debug.Assert(response != null);
            switch (response.getCode())
            {
                case ResponseCode.SUCCESS:
                    {
                        return;
                    }
                default:
                    break;
            }

            throw new MQClientException(response.getCode(), response.getRemark());
        }

        ///<exception cref="RemotingException"/>
        ///<exception cref="InterruptedException"/>
        ///<exception cref="MQClientException"/>
        public String getKVConfigValue(String nameSpace, String key, long timeoutMillis)
        {
            GetKVConfigRequestHeader requestHeader = new GetKVConfigRequestHeader();
            requestHeader.nameSpace = nameSpace;
            requestHeader.key = key;

            RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_KV_CONFIG, requestHeader);

            RemotingCommand response = this.remotingClient.invokeSync(null, request, timeoutMillis);
            //assert response != null;
            Debug.Assert(response != null);
            switch (response.getCode())
            {
                case ResponseCode.SUCCESS:
                    {
                        GetKVConfigResponseHeader responseHeader =
                            response.decodeCommandCustomHeader<GetKVConfigResponseHeader>(/*.class*/);
                        return responseHeader.value;
                    }
                default:
                    break;
            }

            throw new MQClientException(response.getCode(), response.getRemark());
        }

        ///<exception cref="RemotingException"/>
        ///<exception cref="InterruptedException"/>
        ///<exception cref="MQClientException"/>
        public void putKVConfigValue(String nameSpace, String key, String value, long timeoutMillis)
        {
            PutKVConfigRequestHeader requestHeader = new PutKVConfigRequestHeader();
            requestHeader.nameSpace = nameSpace;
            requestHeader.key = key;
            requestHeader.value = value;

            RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.PUT_KV_CONFIG, requestHeader);

            List<String> nameServerAddressList = this.remotingClient.getNameServerAddressList();
            if (nameServerAddressList != null)
            {
                RemotingCommand errResponse = null;
                foreach (String namesrvAddr in nameServerAddressList)
                {
                    RemotingCommand response = this.remotingClient.invokeSync(namesrvAddr, request, timeoutMillis);
                    //assert response != null;
                    Debug.Assert(response != null);
                    switch (response.getCode())
                    {
                        case ResponseCode.SUCCESS:
                            {
                                break;
                            }
                        default:
                            errResponse = response;
                            break;//???
                    }
                }

                if (errResponse != null)
                {
                    throw new MQClientException(errResponse.getCode(), errResponse.getRemark());
                }
            }
        }

        ///<exception cref="RemotingException"/>
        ///<exception cref="InterruptedException"/>
        ///<exception cref="MQClientException"/>
        public void deleteKVConfigValue(String nameSpace, String key, long timeoutMillis)
        {
            DeleteKVConfigRequestHeader requestHeader = new DeleteKVConfigRequestHeader();
            requestHeader.nameSpace = nameSpace;
            requestHeader.key = key;

            RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.DELETE_KV_CONFIG, requestHeader);

            List<String> nameServerAddressList = this.remotingClient.getNameServerAddressList();
            if (nameServerAddressList != null)
            {
                RemotingCommand errResponse = null;
                foreach (String namesrvAddr in nameServerAddressList)
                {
                    RemotingCommand response = this.remotingClient.invokeSync(namesrvAddr, request, timeoutMillis);
                    //assert response != null;
                    Debug.Assert(response != null);
                    switch (response.getCode())
                    {
                        case ResponseCode.SUCCESS:
                            {
                                break;
                            }
                        default:
                            errResponse = response;
                            break; //???
                    }
                }
                if (errResponse != null)
                {
                    throw new MQClientException(errResponse.getCode(), errResponse.getRemark());
                }
            }
        }
        ///<exception cref="RemotingException"/>
        ///<exception cref="InterruptedException"/>
        ///<exception cref="MQClientException"/>
        public KVTable getKVListByNamespace(String nameSpace, long timeoutMillis)
        {
            GetKVListByNamespaceRequestHeader requestHeader = new GetKVListByNamespaceRequestHeader();
            requestHeader.nameSpace = nameSpace;

            RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_KVLIST_BY_NAMESPACE, requestHeader);

            RemotingCommand response = this.remotingClient.invokeSync(null, request, timeoutMillis);
            //assert response != null;
            Debug.Assert(response != null);
            switch (response.getCode())
            {
                case ResponseCode.SUCCESS:
                    {
                        return KVTable.decode<KVTable>(response.getBody()/*.class*/);
                    }
                default:
                    break;
            }

            throw new MQClientException(response.getCode(), response.getRemark());
        }

        ///<exception cref="RemotingException"/>
        ///<exception cref="InterruptedException"/>
        ///<exception cref="MQClientException"/>
        public Dictionary<MessageQueue, long> invokeBrokerToResetOffset(String addr, String topic, String group,
        long timestamp, bool isForce, long timeoutMillis)
        {
            return invokeBrokerToResetOffset(addr, topic, group, timestamp, isForce, timeoutMillis, false);
        }

        ///<exception cref="RemotingException"/>
        ///<exception cref="InterruptedException"/>
        ///<exception cref="MQClientException"/>
        public Dictionary<MessageQueue, long> invokeBrokerToResetOffset(String addr, String topic, String group,
        long timestamp, bool isForce, long timeoutMillis, bool isC)
        {
            ResetOffsetRequestHeader requestHeader = new ResetOffsetRequestHeader();
            requestHeader.topic = topic;
            requestHeader.group = group;
            requestHeader.timestamp = timestamp;
            requestHeader.isForce = isForce;

            RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.INVOKE_BROKER_TO_RESET_OFFSET, requestHeader);
            if (isC)
            {
                request.setLanguage(LanguageCode.CPP);
            }

            RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr),
                request, timeoutMillis);
            //assert response != null;
            Debug.Assert(response != null);
            switch (response.getCode())
            {
                case ResponseCode.SUCCESS:
                    {
                        if (response.getBody() != null)
                        {
                            ResetOffsetBody body = ResetOffsetBody.decode<ResetOffsetBody>(response.getBody()/*.class*/);
                            return body.offsetTable;
                        }
                        break; //???
                    }
                default:
                    break;
            }

            throw new MQClientException(response.getCode(), response.getRemark());
        }

        ///<exception cref="RemotingException"/>
        ///<exception cref="InterruptedException"/>
        ///<exception cref="MQClientException"/>
        public Dictionary<String, Dictionary<MessageQueue, long>> invokeBrokerToGetConsumerStatus(String addr, String topic,
        String group,
        String clientAddr,
        long timeoutMillis)
        {
            GetConsumerStatusRequestHeader requestHeader = new GetConsumerStatusRequestHeader();
            requestHeader.topic = topic;
            requestHeader.group = group;
            requestHeader.clientAddr = clientAddr;

            RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.INVOKE_BROKER_TO_GET_CONSUMER_STATUS, requestHeader);

            RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr),
                request, timeoutMillis);
            //assert response != null;
            Debug.Assert(response != null);
            switch (response.getCode())
            {
                case ResponseCode.SUCCESS:
                    {
                        if (response.getBody() != null)
                        {
                            GetConsumerStatusBody body = GetConsumerStatusBody.decode<GetConsumerStatusBody>(response.getBody()/*.class*/);
                            return body.consumerTable;
                        }
                        break; //???
                    }
                default:
                    break;
            }

            throw new MQClientException(response.getCode(), response.getRemark());
        }

        ///<exception cref="RemotingConnectException"/>
        ///<exception cref="RemotingSendRequestException"/>
        ///<exception cref="RemotingTimeoutException"/>
        ///<exception cref="InterruptedException"/>
        ///<exception cref="MQBrokerException"/>
        public GroupList queryTopicConsumeByWho(String addr, String topic, long timeoutMillis)
        {
            QueryTopicConsumeByWhoRequestHeader requestHeader = new QueryTopicConsumeByWhoRequestHeader();
            requestHeader.topic = topic;

            RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.QUERY_TOPIC_CONSUME_BY_WHO, requestHeader);

            RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr),
                request, timeoutMillis);
            switch (response.getCode())
            {
                case ResponseCode.SUCCESS:
                    {
                        GroupList groupList = GroupList.decode<GroupList>(response.getBody()/*.class*/);
                        return groupList;
                    }
                default:
                    break;
            }

            throw new MQBrokerException(response.getCode(), response.getRemark(), addr);
        }

        ///<exception cref="RemotingConnectException"/>
        ///<exception cref="RemotingSendRequestException"/>
        ///<exception cref="RemotingTimeoutException"/>
        ///<exception cref="InterruptedException"/>
        ///<exception cref="MQBrokerException"/>
        public List<QueueTimeSpan> queryConsumeTimeSpan(String addr, String topic, String group, long timeoutMillis)
        {
            QueryConsumeTimeSpanRequestHeader requestHeader = new QueryConsumeTimeSpanRequestHeader();
            requestHeader.topic = topic;
            requestHeader.group = group;

            RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.QUERY_CONSUME_TIME_SPAN, requestHeader);

            RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr),
                request, timeoutMillis);
            switch (response.getCode())
            {
                case ResponseCode.SUCCESS:
                    {
                        QueryConsumeTimeSpanBody consumeTimeSpanBody = GroupList.decode<QueryConsumeTimeSpanBody>(response.getBody()/*.class*/);
                        return consumeTimeSpanBody.consumeTimeSpanSet;
                    }
                default:
                    break;
            }

            throw new MQBrokerException(response.getCode(), response.getRemark(), addr);
        }

        ///<exception cref="RemotingException"/>
        ///<exception cref="MQClientException"/>
        ///<exception cref="MQClientException"/>
        public TopicList getTopicsByCluster(String cluster, long timeoutMillis)
        {
            GetTopicsByClusterRequestHeader requestHeader = new GetTopicsByClusterRequestHeader();
            requestHeader.cluster = cluster;
            RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_TOPICS_BY_CLUSTER, requestHeader);

            RemotingCommand response = this.remotingClient.invokeSync(null, request, timeoutMillis);
            //assert response != null;
            Debug.Assert(response != null);
            switch (response.getCode())
            {
                case ResponseCode.SUCCESS:
                    {
                        byte[] body = response.getBody();
                        if (body != null)
                        {
                            TopicList topicList = TopicList.decode<TopicList>(body/*.class*/);
                            return topicList;
                        }
                        break;//???
                    }
                default:
                    break;
            }

            throw new MQClientException(response.getCode(), response.getRemark());
        }

        ///<exception cref="RemotingConnectException"/>
        ///<exception cref="RemotingSendRequestException"/>
        ///<exception cref="RemotingTimeoutException"/>
        ///<exception cref="InterruptedException"/>
        ///<exception cref="MQBrokerException"/>
        public void registerMessageFilterClass(String addr,
        String consumerGroup,
        String topic,
        String className,
        int classCRC,
        byte[] classBody,
        long timeoutMillis)
        {
            RegisterMessageFilterClassRequestHeader requestHeader = new RegisterMessageFilterClassRequestHeader();
            requestHeader.consumerGroup = consumerGroup;
            requestHeader.className = className;
            requestHeader.topic = topic;
            requestHeader.classCRC = classCRC;

            RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.REGISTER_MESSAGE_FILTER_CLASS, requestHeader);
            request.setBody(classBody);
            RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);
            switch (response.getCode())
            {
                case ResponseCode.SUCCESS:
                    {
                        return;
                    }
                default:
                    break;
            }

            throw new MQBrokerException(response.getCode(), response.getRemark(), addr);
        }

        ///<exception cref="RemotingException"/>
        ///<exception cref="MQClientException"/>
        ///<exception cref="InterruptedException"/>
        public TopicList getSystemTopicList(long timeoutMillis)
        {
            RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_SYSTEM_TOPIC_LIST_FROM_NS, null);

            RemotingCommand response = this.remotingClient.invokeSync(null, request, timeoutMillis);
            //assert response != null;
            Debug.Assert(response != null);
            switch (response.getCode())
            {
                case ResponseCode.SUCCESS:
                    {
                        byte[] body = response.getBody();
                        if (body != null)
                        {
                            TopicList topicList = TopicList.decode<TopicList>(response.getBody()/*.class*/);
                            if (topicList.topicList != null && !topicList.topicList.isEmpty()
                                && !UtilAll.isBlank(topicList.brokerAddr))
                            {
                                TopicList tmp = getSystemTopicListFromBroker(topicList.brokerAddr, timeoutMillis);
                                if (tmp.topicList != null && !tmp.topicList.isEmpty())
                                {
                                    //topicList.getTopicList().addAll(tmp.getTopicList());
                                    topicList.topicList.addAll(tmp.topicList);
                                }
                            }
                            return topicList;
                        }
                        break;//???
                    }
                default:
                    break;
            }

            throw new MQClientException(response.getCode(), response.getRemark());
        }

        ///<exception cref="RemotingException"/>
        ///<exception cref="MQClientException"/>
        ///<exception cref="InterruptedException"/>
        public TopicList getSystemTopicListFromBroker(String addr, long timeoutMillis)
        {
            RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_SYSTEM_TOPIC_LIST_FROM_BROKER, null);

            RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr),
                request, timeoutMillis);
            //assert response != null;
            Debug.Assert(response != null);
            switch (response.getCode())
            {
                case ResponseCode.SUCCESS:
                    {
                        byte[] body = response.getBody();
                        if (body != null)
                        {
                            TopicList topicList = TopicList.decode<TopicList>(body/*.class*/);
                            return topicList;
                        }
                        break;
                    }
                default:
                    break;
            }

            throw new MQClientException(response.getCode(), response.getRemark());
        }

        ///<exception cref="MQClientException"/>
        ///<exception cref="RemotingConnectException"/>
        ///<exception cref="RemotingSendRequestException"/>
        ///<exception cref="RemotingTimeoutException"/>
        ///<exception cref="InterruptedException"/>
        public bool cleanExpiredConsumeQueue(String addr,
        long timeoutMillis)
        {
            RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.CLEAN_EXPIRED_CONSUMEQUEUE, null);
            RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr),
                request, timeoutMillis);
            switch (response.getCode())
            {
                case ResponseCode.SUCCESS:
                    {
                        return true;
                    }
                default:
                    break;
            }

            throw new MQClientException(response.getCode(), response.getRemark());
        }

        ///<exception cref="MQClientException"/>
        ///<exception cref="RemotingConnectException"/>
        ///<exception cref="RemotingSendRequestException"/>
        ///<exception cref="RemotingTimeoutException"/>
        ///<exception cref="InterruptedException"/>
        public bool cleanUnusedTopicByAddr(String addr,
        long timeoutMillis)
        {
            RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.CLEAN_UNUSED_TOPIC, null);
            RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr),
                request, timeoutMillis);
            switch (response.getCode())
            {
                case ResponseCode.SUCCESS:
                    {
                        return true;
                    }
                default:
                    break;
            }

            throw new MQClientException(response.getCode(), response.getRemark());
        }

        ///<exception cref="RemotingException"/>
        ///<exception cref="MQClientException"/>
        ///<exception cref="InterruptedException"/>
        public ConsumerRunningInfo getConsumerRunningInfo(String addr, String consumerGroup, String clientId, bool jstack, long timeoutMillis)
        {
            GetConsumerRunningInfoRequestHeader requestHeader = new GetConsumerRunningInfoRequestHeader();
            requestHeader.consumerGroup = consumerGroup;
            requestHeader.clientId = clientId;
            requestHeader.jstackEnable = jstack;

            RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_CONSUMER_RUNNING_INFO, requestHeader);

            RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr),
                request, timeoutMillis);
            //assert response != null;
            Debug.Assert(response != null);
            switch (response.getCode())
            {
                case ResponseCode.SUCCESS:
                    {
                        byte[] body = response.getBody();
                        if (body != null)
                        {
                            ConsumerRunningInfo info = ConsumerRunningInfo.decode<ConsumerRunningInfo>(body/*.class*/);
                            return info;
                        }
                        break;//???
                    }
                default:
                    break;
            }

            throw new MQClientException(response.getCode(), response.getRemark());
        }

        ///<exception cref="RemotingException"/>
        ///<exception cref="MQClientException"/>
        ///<exception cref="InterruptedException"/>
        public ConsumeMessageDirectlyResult consumeMessageDirectly(String addr,
        String consumerGroup,
        String clientId,
        String msgId,
        long timeoutMillis)
        {
            ConsumeMessageDirectlyResultRequestHeader requestHeader = new ConsumeMessageDirectlyResultRequestHeader();
            requestHeader.consumerGroup = consumerGroup;
            requestHeader.clientId = clientId;
            requestHeader.msgId = msgId;

            RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.CONSUME_MESSAGE_DIRECTLY, requestHeader);

            RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr),
                request, timeoutMillis);
            //assert response != null;
            Debug.Assert(response != null);
            switch (response.getCode())
            {
                case ResponseCode.SUCCESS:
                    {
                        byte[] body = response.getBody();
                        if (body != null)
                        {
                            ConsumeMessageDirectlyResult info = ConsumeMessageDirectlyResult.decode<ConsumeMessageDirectlyResult>(body/*.class*/);
                            return info;
                        }
                        break;//???
                    }
                default:
                    break;
            }

            throw new MQClientException(response.getCode(), response.getRemark());
        }

        ///<exception cref="MQClientException"/>
        ///<exception cref="RemotingConnectException"/>
        ///<exception cref="RemotingSendRequestException"/>
        ///<exception cref="RemotingTimeoutException"/>
        ///<exception cref="InterruptedException"/>
        public Dictionary<int, long> queryCorrectionOffset(String addr, String topic, String group,
        HashSet<String> filterGroup,
        long timeoutMillis)
        {
            QueryCorrectionOffsetHeader requestHeader = new QueryCorrectionOffsetHeader();
            requestHeader.compareGroup = group;
            requestHeader.topic = topic;
            if (filterGroup != null)
            {
                StringBuilder sb = new StringBuilder();
                String splitor = "";
                foreach (String s in filterGroup)
                {
                    sb.Append(splitor).Append(s);
                    splitor = ",";
                }
                requestHeader.filterGroups = sb.ToString();
            }
            RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.QUERY_CORRECTION_OFFSET, requestHeader);

            RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr),
                request, timeoutMillis);
            //assert response != null;
            Debug.Assert(response != null);
            switch (response.getCode())
            {
                case ResponseCode.SUCCESS:
                    {
                        if (response.getBody() != null)
                        {
                            QueryCorrectionOffsetBody body = QueryCorrectionOffsetBody.decode<QueryCorrectionOffsetBody>(response.getBody()/*.class*/);
                            return body.correctionOffsets;
                        }
                        break;//???
                    }
                default:
                    break;
            }

            throw new MQClientException(response.getCode(), response.getRemark());
        }

        ///<exception cref="RemotingException"/>
        ///<exception cref="MQClientException"/>
        ///<exception cref="InterruptedException"/>
        public TopicList getUnitTopicList(bool containRetry, long timeoutMillis)
        {
            RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_UNIT_TOPIC_LIST, null);

            RemotingCommand response = this.remotingClient.invokeSync(null, request, timeoutMillis);
            //assert response != null;
            Debug.Assert(response != null);
            switch (response.getCode())
            {
                case ResponseCode.SUCCESS:
                    {
                        byte[] body = response.getBody();
                        if (body != null)
                        {
                            TopicList topicList = TopicList.decode<TopicList>(response.getBody()/*.class*/);
                            if (!containRetry)
                            {
                                //Iterator<String> it = topicList.getTopicList().iterator();
                                //while (it.hasNext())
                                foreach (var topic in topicList.topicList)
                                {
                                    //String topic = it.next();
                                    if (topic.StartsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX))
                                        topicList.topicList.Remove(topic);//it.remove();  //net5.0 hashset 可以遍历删除
                                }
                            }

                            return topicList;
                        }
                        break;//???
                    }
                default:
                    break;
            }

            throw new MQClientException(response.getCode(), response.getRemark());
        }

        ///<exception cref="RemotingException"/>
        ///<exception cref="MQClientException"/>
        ///<exception cref="InterruptedException"/>
        public TopicList getHasUnitSubTopicList(bool containRetry, long timeoutMillis)
        {
            RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_HAS_UNIT_SUB_TOPIC_LIST, null);

            RemotingCommand response = this.remotingClient.invokeSync(null, request, timeoutMillis);
            //assert response != null;
            Debug.Assert(response != null);
            switch (response.getCode())
            {
                case ResponseCode.SUCCESS:
                    {
                        byte[] body = response.getBody();
                        if (body != null)
                        {
                            TopicList topicList = TopicList.decode<TopicList>(response.getBody()/*.class*/);
                            if (!containRetry)
                            {
                                //Iterator<String> it = topicList.getTopicList().iterator();
                                //while (it.hasNext())
                                foreach (var topic in topicList.topicList)
                                {
                                    //String topic = it.next();
                                    if (topic.StartsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX))
                                        topicList.topicList.Remove(topic);//it.remove();  //net5.0 hashset 可以遍历删除
                                }
                            }
                            return topicList;
                        }
                        break;//???
                    }
                default:
                    break;
            }

            throw new MQClientException(response.getCode(), response.getRemark());
        }

        ///<exception cref="RemotingException"/>
        ///<exception cref="MQClientException"/>
        ///<exception cref="InterruptedException"/>
        public TopicList getHasUnitSubUnUnitTopicList(bool containRetry, long timeoutMillis)
        {
            RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_HAS_UNIT_SUB_UNUNIT_TOPIC_LIST, null);

            RemotingCommand response = this.remotingClient.invokeSync(null, request, timeoutMillis);
            //assert response != null;
            Debug.Assert(response != null);
            switch (response.getCode())
            {
                case ResponseCode.SUCCESS:
                    {
                        byte[] body = response.getBody();
                        if (body != null)
                        {
                            TopicList topicList = TopicList.decode<TopicList>(response.getBody()/*.class*/);
                            if (!containRetry)
                            {
                                //Iterator<String> it = topicList.getTopicList().iterator();
                                //while (it.hasNext())
                                foreach (var topic in topicList.topicList)
                                {
                                    //String topic = it.next();
                                    if (topic.StartsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX))
                                        topicList.topicList.Remove(topic);//it.remove();  //net5.0 hashset 可以遍历删除
                                }
                            }
                            return topicList;
                        }
                        break;//???
                    }
                default:
                    break;
            }

            throw new MQClientException(response.getCode(), response.getRemark());
        }

        ///<exception cref="RemotingException"/>
        ///<exception cref="MQClientException"/>
        ///<exception cref="InterruptedException"/>
        public void cloneGroupOffset(String addr, String srcGroup, String destGroup, String topic,
        bool isOffline,
        long timeoutMillis)
        {
            CloneGroupOffsetRequestHeader requestHeader = new CloneGroupOffsetRequestHeader();
            requestHeader.srcGroup = srcGroup;
            requestHeader.destGroup = destGroup;
            requestHeader.topic = topic;
            requestHeader.offline = isOffline;
            RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.CLONE_GROUP_OFFSET, requestHeader);

            RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr),
                request, timeoutMillis);
            //assert response != null;
            Debug.Assert(response != null);
            switch (response.getCode())
            {
                case ResponseCode.SUCCESS:
                    {
                        return;
                    }
                default:
                    break;
            }

            throw new MQClientException(response.getCode(), response.getRemark());
        }

        ///<exception cref="MQClientException"/>
        ///<exception cref="RemotingConnectException"/>
        ///<exception cref="RemotingSendRequestException"/>
        /// ///<exception cref="RemotingTimeoutException"/>
        ///<exception cref="InterruptedException"/>

        public BrokerStatsData viewBrokerStatsData(String brokerAddr, String statsName, String statsKey, long timeoutMillis)
        {
            ViewBrokerStatsDataRequestHeader requestHeader = new ViewBrokerStatsDataRequestHeader();
            requestHeader.statsName = statsName;
            requestHeader.statsKey = statsKey;

            RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.VIEW_BROKER_STATS_DATA, requestHeader);

            RemotingCommand response = this.remotingClient
                .invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), brokerAddr), request, timeoutMillis);
            //assert response != null;
            Debug.Assert(response != null);
            switch (response.getCode())
            {
                case ResponseCode.SUCCESS:
                    {
                        byte[] body = response.getBody();
                        if (body != null)
                        {
                            return BrokerStatsData.decode<BrokerStatsData>(body/*.class*/);
                        }
                        break;//???
                    }
                default:
                    break;
            }

            throw new MQClientException(response.getCode(), response.getRemark());
        }

        ///<exception cref="MQClientException"/>
        ///<exception cref="RemotingConnectException"/>
        ///<exception cref="RemotingSendRequestException"/>
        /// ///<exception cref="RemotingTimeoutException"/>
        ///<exception cref="InterruptedException"/>
        public HashSet<String> getClusterList(String topic, long timeoutMillis)
        {
            return new HashSet<string>();
            //return Collections.EMPTY_SET;
        }

        ///<exception cref="MQClientException"/>
        ///<exception cref="RemotingConnectException"/>
        ///<exception cref="RemotingSendRequestException"/>
        /// ///<exception cref="RemotingTimeoutException"/>
        ///<exception cref="InterruptedException"/>
        public ConsumeStatsList fetchConsumeStatsInBroker(String brokerAddr, bool isOrder, long timeoutMillis)
        {
            GetConsumeStatsInBrokerHeader requestHeader = new GetConsumeStatsInBrokerHeader();
            requestHeader.isOrder = isOrder;

            RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_BROKER_CONSUME_STATS, requestHeader);

            RemotingCommand response = this.remotingClient
                .invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), brokerAddr), request, timeoutMillis);
            //assert response != null;
            Debug.Assert(response != null);
            switch (response.getCode())
            {
                case ResponseCode.SUCCESS:
                    {
                        byte[] body = response.getBody();
                        if (body != null)
                        {
                            return ConsumeStatsList.decode<ConsumeStatsList>(body/*.class*/);
                        }
                        break;//???
                    }
                default:
                    break;
            }

            throw new MQClientException(response.getCode(), response.getRemark());
        }

        ///<exception cref="MQBrokerException"/>
        ///<exception cref="RemotingConnectException"/>
        ///<exception cref="RemotingSendRequestException"/>
        /// ///<exception cref="RemotingTimeoutException"/>
        ///<exception cref="InterruptedException"/>
        public SubscriptionGroupWrapper getAllSubscriptionGroup(String brokerAddr, long timeoutMillis)
        {
            RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_ALL_SUBSCRIPTIONGROUP_CONFIG, null);
            RemotingCommand response = this.remotingClient
                .invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), brokerAddr), request, timeoutMillis);
            //assert response != null;
            Debug.Assert(response != null);
            switch (response.getCode())
            {
                case ResponseCode.SUCCESS:
                    {
                        return SubscriptionGroupWrapper.decode<SubscriptionGroupWrapper>(response.getBody()/*.class*/);
                    }
                default:
                    break;
            }
            throw new MQBrokerException(response.getCode(), response.getRemark(), brokerAddr);
        }

        ///<exception cref="MQBrokerException"/>
        ///<exception cref="RemotingConnectException"/>
        ///<exception cref="RemotingSendRequestException"/>
        /// ///<exception cref="RemotingTimeoutException"/>
        ///<exception cref="InterruptedException"/>
        public TopicConfigSerializeWrapper getAllTopicConfig(String addr,
        long timeoutMillis)
        {
            RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_ALL_TOPIC_CONFIG, null);

            RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr),
                request, timeoutMillis);
            //assert response != null;
            Debug.Assert(response != null);
            switch (response.getCode())
            {
                case ResponseCode.SUCCESS:
                    {
                        return TopicConfigSerializeWrapper.decode<TopicConfigSerializeWrapper>(response.getBody()/*.class*/);
                    }
                default:
                    break;
            }

            throw new MQBrokerException(response.getCode(), response.getRemark(), addr);
        }

        ///<exception cref="MQClientException"/>
        ///<exception cref="RemotingConnectException"/>
        ///<exception cref="RemotingSendRequestException"/>
        ///<exception cref="RemotingTimeoutException"/>
        ///<exception cref="InterruptedException"/>
        ///<exception cref="UnsupportedEncodingException"/>
        public void updateNameServerConfig(Properties properties, List<String> nameServers, long timeoutMillis)
        {
            String str = MixAll.properties2String(properties);
            if (str == null || str.Length < 1)
            {
                return;
            }
            List<String> invokeNameServers = (nameServers == null || nameServers.isEmpty()) ?
                this.remotingClient.getNameServerAddressList() : nameServers;
            if (invokeNameServers == null || invokeNameServers.isEmpty())
            {
                return;
            }

            RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.UPDATE_NAMESRV_CONFIG, null);
            request.setBody(str.getBytes(MixAll.DEFAULT_CHARSET));

            RemotingCommand errResponse = null;
            foreach (String nameServer in invokeNameServers)
            {
                RemotingCommand response = this.remotingClient.invokeSync(nameServer, request, timeoutMillis);
                //assert response != null;
                Debug.Assert(response != null);
                switch (response.getCode())
                {
                    case ResponseCode.SUCCESS:
                        {
                            break;
                        }
                    default:
                        errResponse = response;
                        break;
                }
            }

            if (errResponse != null)
            {
                throw new MQClientException(errResponse.getCode(), errResponse.getRemark());
            }
        }

        ///<exception cref="MQClientException"/>
        ///<exception cref="RemotingConnectException"/>
        ///<exception cref="RemotingSendRequestException"/>
        ///<exception cref="RemotingTimeoutException"/>
        ///<exception cref="InterruptedException"/>
        ///<exception cref="UnsupportedEncodingException"/>
        public Dictionary<String, Properties> getNameServerConfig(List<String> nameServers, long timeoutMillis)
        {
            List<String> invokeNameServers = (nameServers == null || nameServers.isEmpty()) ?
                this.remotingClient.getNameServerAddressList() : nameServers;
            if (invokeNameServers == null || invokeNameServers.isEmpty())
            {
                return null;
            }

            RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_NAMESRV_CONFIG, null);

            Dictionary<String, Properties> configMap = new Dictionary<String, Properties>(4);
            foreach (String nameServer in invokeNameServers)
            {
                RemotingCommand response = this.remotingClient.invokeSync(nameServer, request, timeoutMillis);

                //assert response != null;
                Debug.Assert(response != null);

                if (ResponseCode.SUCCESS == response.getCode())
                {
                    //configMap.put(nameServer, MixAll.string2Properties(new String(response.getBody(), MixAll.DEFAULT_CHARSET)));
                    //configMap[nameServer] = MixAll.string2Properties(new String(response.getBody(), MixAll.DEFAULT_CHARSET));
                    configMap[nameServer] = MixAll.string2Properties(MixAll.DEFAULT_CHARSET.GetString(response.getBody()));
                }
                else
                {
                    throw new MQClientException(response.getCode(), response.getRemark());
                }
            }
            return configMap;
        }

        ///<exception cref="InterruptedException"/>
        ///<exception cref="RemotingTimeoutException"/>
        ///<exception cref="RemotingSendRequestException"/>
        ///<exception cref="RemotingConnectException"/>
        ///<exception cref="MQClientException"/>
        public QueryConsumeQueueResponseBody queryConsumeQueue(String brokerAddr, String topic,
            int queueId,
            long index, int count, String consumerGroup,
            long timeoutMillis)
        {

            QueryConsumeQueueRequestHeader requestHeader = new QueryConsumeQueueRequestHeader();
            requestHeader.topic = topic;
            requestHeader.queueId = queueId;
            requestHeader.index = index;
            requestHeader.count = count;
            requestHeader.consumerGroup = consumerGroup;

            RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.QUERY_CONSUME_QUEUE, requestHeader);

            RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), brokerAddr), request, timeoutMillis);

            //assert response != null;
            Debug.Assert(response != null);

            if (ResponseCode.SUCCESS == response.getCode())
            {
                return QueryConsumeQueueResponseBody.decode<QueryConsumeQueueResponseBody>(response.getBody()/*.class*/);
            }

            throw new MQClientException(response.getCode(), response.getRemark());
        }

        ///<exception cref="InterruptedException"/>
        ///<exception cref="RemotingTimeoutException"/>
        ///<exception cref="RemotingSendRequestException"/>
        ///<exception cref="RemotingConnectException"/>
        ///<exception cref="MQClientException"/>
        public void checkClientInBroker(String brokerAddr, String consumerGroup,
        String clientId, SubscriptionData subscriptionData,
        long timeoutMillis)
        {
            RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.CHECK_CLIENT_CONFIG, null);

            CheckClientRequestBody requestBody = new CheckClientRequestBody();
            requestBody.clientId = clientId;
            requestBody.group = consumerGroup;
            requestBody.subscriptionData = subscriptionData;

            request.setBody(requestBody.encode());

            RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), brokerAddr), request, timeoutMillis);

            //assert response != null;
            Debug.Assert(response != null);

            if (ResponseCode.SUCCESS != response.getCode())
            {
                throw new MQClientException(response.getCode(), response.getRemark());
            }
        }

        ///<exception cref="RemotingException"/>
        ///<exception cref="MQClientException"/>
        ///<exception cref="InterruptedException"/>
        public bool resumeCheckHalfMessage(String addr, String msgId,
            long timeoutMillis)
        {
            ResumeCheckHalfMessageRequestHeader requestHeader = new ResumeCheckHalfMessageRequestHeader();
            requestHeader.msgId = msgId;

            RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.RESUME_CHECK_HALF_MESSAGE, requestHeader);

            RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr),
                request, timeoutMillis);
            //assert response != null;
            Debug.Assert(response != null);
            switch (response.getCode())
            {
                case ResponseCode.SUCCESS:
                    {
                        return true;
                    }
                default:
                    log.Error("Failed to resume half message check logic. Remark={}", response.getRemark());
                    return false;
            }
        }
    }
}
