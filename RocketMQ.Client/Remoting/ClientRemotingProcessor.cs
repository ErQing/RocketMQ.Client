using DotNetty.Transport.Channels;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace RocketMQ.Client
{
    public class ClientRemotingProcessor : AsyncNettyRequestProcessor
    {
        //private readonly InternalLogger log = ClientLogger.getLog();
        static NLog.Logger log = NLog.LogManager.GetCurrentClassLogger();
        private readonly MQClientInstance mqClientFactory;

        public ClientRemotingProcessor(MQClientInstance mqClientFactory)
        {
            this.mqClientFactory = mqClientFactory;
        }

        //@Override
        ///<exception cref="RemotingCommandException"/>
        public override RemotingCommand processRequest(IChannelHandlerContext ctx, RemotingCommand request)
        {
            switch (request.getCode())
            {
                case RequestCode.CHECK_TRANSACTION_STATE:
                    return this.checkTransactionState(ctx, request);
                case RequestCode.NOTIFY_CONSUMER_IDS_CHANGED:
                    return this.notifyConsumerIdsChanged(ctx, request);
                case RequestCode.RESET_CONSUMER_CLIENT_OFFSET:
                    return this.resetOffset(ctx, request);
                case RequestCode.GET_CONSUMER_STATUS_FROM_CLIENT:
                    return this.getConsumeStatus(ctx, request);

                case RequestCode.GET_CONSUMER_RUNNING_INFO:
                    return this.getConsumerRunningInfo(ctx, request);

                case RequestCode.CONSUME_MESSAGE_DIRECTLY:
                    return this.consumeMessageDirectly(ctx, request);

                case RequestCode.PUSH_REPLY_MESSAGE_TO_CLIENT:
                    return this.receiveReplyMessage(ctx, request);
                default:
                    break;
            }
            return null;
        }

        //@Override
        public override bool rejectRequest()
        {
            return false;
        }
        ///<exception cref="RemotingCommandException"/>
        public RemotingCommand checkTransactionState(IChannelHandlerContext ctx, RemotingCommand request)
        {
            CheckTransactionStateRequestHeader requestHeader =
                    request.decodeCommandCustomHeader<CheckTransactionStateRequestHeader>(/*.class*/);
            ByteBuffer byteBuffer = ByteBuffer.wrap(request.getBody());
            MessageExt messageExt = MessageDecoder.decode(byteBuffer);
            if (messageExt != null)
            {
                if (StringUtils.isNotEmpty(this.mqClientFactory.getClientConfig().getNamespace()))
                {
                    messageExt.setTopic(NamespaceUtil
                        .withoutNamespace(messageExt.getTopic(), this.mqClientFactory.getClientConfig().getNamespace()));
                }
                String transactionId = messageExt.getProperty(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX);
                if (null != transactionId && !"".Equals(transactionId))
                {
                    messageExt.setTransactionId(transactionId);
                }
                String group = messageExt.getProperty(MessageConst.PROPERTY_PRODUCER_GROUP);
                if (group != null)
                {
                    MQProducerInner producer = this.mqClientFactory.selectProducer(group);
                    if (producer != null)
                    {
                        String addr = RemotingHelper.parseChannelRemoteAddr(ctx.Channel);
                        producer.checkTransactionState(addr, messageExt, requestHeader);
                    }
                    else
                    {
                        log.Debug("checkTransactionState, pick producer by group[{}] failed", group);
                    }
                }
                else
                {
                    log.Warn("checkTransactionState, pick producer group failed");
                }
            }
            else
            {
                log.Warn("checkTransactionState, decode message failed");
            }

            return null;
        }

        ///<exception cref="RemotingCommandException"/>
        public RemotingCommand notifyConsumerIdsChanged(IChannelHandlerContext ctx, RemotingCommand request)
        {
            try
            {
                NotifyConsumerIdsChangedRequestHeader requestHeader =
                    request.decodeCommandCustomHeader<NotifyConsumerIdsChangedRequestHeader>(/*.class*/);
                log.Info("receive broker's notification[{}], the consumer group: {} changed, rebalance immediately",
    RemotingHelper.parseChannelRemoteAddr(ctx.Channel),
    requestHeader.consumerGroup);
                this.mqClientFactory.rebalanceImmediately();
            }
            catch (Exception e)
            {
                log.Error("notifyConsumerIdsChanged exception", RemotingHelper.exceptionSimpleDesc(e));
            }
            return null;
        }

        ///<exception cref="RemotingCommandException"/>
        public RemotingCommand resetOffset(IChannelHandlerContext ctx,
        RemotingCommand request)
        {
            ResetOffsetRequestHeader requestHeader =
                    request.decodeCommandCustomHeader<ResetOffsetRequestHeader>(/*.class*/);
            log.Info("invoke reset offset operation from broker. brokerAddr={}, topic={}, group={}, timestamp={}",
    RemotingHelper.parseChannelRemoteAddr(ctx.Channel), requestHeader.topic, requestHeader.group, requestHeader.timestamp);
            var offsetTable = new HashMap<MessageQueue, long>();
            if (request.getBody() != null)
            {
                ResetOffsetBody body = RemotingSerializable.decode<ResetOffsetBody>(request.getBody()/*.class*/);
                offsetTable = body.offsetTable;
            }
            this.mqClientFactory.resetOffset(requestHeader.topic, requestHeader.group, offsetTable);
            return null;
        }

        [Obsolete]//@Deprecated
        ///<exception cref="RemotingCommandException"/>
        public RemotingCommand getConsumeStatus(IChannelHandlerContext ctx,
        RemotingCommand request)
        {
            RemotingCommand response = RemotingCommand.createResponseCommand();
            GetConsumerStatusRequestHeader requestHeader =
                    request.decodeCommandCustomHeader<GetConsumerStatusRequestHeader>(/*.class*/);

            var offsetTable = this.mqClientFactory.getConsumerStatus(requestHeader.topic, requestHeader.group);
            GetConsumerStatusBody body = new GetConsumerStatusBody();
            body.messageQueueTable = offsetTable;
            response.setBody(body.encode());
            response.setCode(ResponseCode.SUCCESS);
            return response;
        }

        ///<exception cref="RemotingCommandException"/>
        private RemotingCommand getConsumerRunningInfo(IChannelHandlerContext ctx,
            RemotingCommand request)
        {
            RemotingCommand response = RemotingCommand.createResponseCommand();
            GetConsumerRunningInfoRequestHeader requestHeader =
                request.decodeCommandCustomHeader<GetConsumerRunningInfoRequestHeader>(/*.class*/);

            ConsumerRunningInfo consumerRunningInfo = this.mqClientFactory.consumerRunningInfo(requestHeader.consumerGroup);
            if (null != consumerRunningInfo)
            {
                if (requestHeader.jstackEnable)
                {
                    //Map<Thread, StackTraceElement[]> map = Thread.getAllStackTraces();
                    //String jstack = UtilAll.jstack(map);
                    //consumerRunningInfo.setJstack(jstack);
                    //TODO
                }

                response.setCode(ResponseCode.SUCCESS);
                response.setBody(consumerRunningInfo.encode());
            }
            else
            {
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark(String.Format("The Consumer Group <%s> not exist in this consumer", requestHeader.consumerGroup));
            }

            return response;
        }

        ///<exception cref="RemotingCommandException"/>
        private RemotingCommand consumeMessageDirectly(IChannelHandlerContext ctx,
            RemotingCommand request)
        {
            RemotingCommand response = RemotingCommand.createResponseCommand();
            ConsumeMessageDirectlyResultRequestHeader requestHeader =
                request.decodeCommandCustomHeader<ConsumeMessageDirectlyResultRequestHeader>(/*.class*/);

            MessageExt msg = MessageDecoder.decode(ByteBuffer.wrap(request.getBody()));

            ConsumeMessageDirectlyResult result =
        this.mqClientFactory.consumeMessageDirectly(msg, requestHeader.consumerGroup, requestHeader.brokerName);

            if (null != result)
            {
                response.setCode(ResponseCode.SUCCESS);
                response.setBody(result.encode());
            }
            else
            {
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark(String.Format("The Consumer Group <%s> not exist in this consumer", requestHeader.consumerGroup));
            }

            return response;
        }

        ///<exception cref="RemotingCommandException"/>
        private RemotingCommand receiveReplyMessage(IChannelHandlerContext ctx,
            RemotingCommand request)
        {

            RemotingCommand response = RemotingCommand.createResponseCommand();
            long receiveTime = Sys.currentTimeMillis();
            ReplyMessageRequestHeader requestHeader = request.decodeCommandCustomHeader<ReplyMessageRequestHeader>(/*.class*/);

            try
            {
                MessageExt msg = new MessageExt();
                msg.setTopic(requestHeader.topic);
                msg.setQueueId(requestHeader.queueId);
                msg.setStoreTimestamp(requestHeader.storeTimestamp);

                if (requestHeader.bornHost != null)
                {
                    msg.setBornHost(RemotingUtil.string2SocketAddress(requestHeader.bornHost));
                }

                if (requestHeader.storeHost != null)
                {
                    msg.setStoreHost(RemotingUtil.string2SocketAddress(requestHeader.storeHost));
                }

                byte[] body = request.getBody();
                if ((requestHeader.sysFlag & MessageSysFlag.COMPRESSED_FLAG) == MessageSysFlag.COMPRESSED_FLAG)
                {
                    try
                    {
                        body = UtilAll.uncompress(body);
                    }
                    catch (IOException e)
                    {
                        log.Warn("err when uncompress constant", e);
                    }
                }
                msg.setBody(body);
                msg.setFlag(requestHeader.flag);
                MessageAccessor.setProperties(msg, MessageDecoder.string2messageProperties(requestHeader.properties));
                MessageAccessor.putProperty(msg, MessageConst.PROPERTY_REPLY_MESSAGE_ARRIVE_TIME, Str.valueOf(receiveTime));
                msg.setBornTimestamp(requestHeader.bornTimestamp);
                msg.setReconsumeTimes(requestHeader.reconsumeTimes == null ? 0 : requestHeader.reconsumeTimes);
                log.Debug("receive reply message :{}", msg);

                processReplyMessage(msg);

                response.setCode(ResponseCode.SUCCESS);
                response.setRemark(null);
            }
            catch (Exception e)
            {
                log.Warn("unknown err when receiveReplyMsg", e.ToString());
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark("process reply message fail");
            }
            return response;
        }

        private void processReplyMessage(MessageExt replyMsg)
        {
            String correlationId = replyMsg.getUserProperty(MessageConst.PROPERTY_CORRELATION_ID);
            RequestResponseFuture requestResponseFuture = RequestFutureHolder.getInstance().getRequestFutureTable().get(correlationId);
            if (requestResponseFuture != null)
            {
                requestResponseFuture.putResponseMessage(replyMsg);

                RequestFutureHolder.getInstance().getRequestFutureTable().remove(correlationId);

                if (requestResponseFuture.getRequestCallback() != null)
                {
                    requestResponseFuture.getRequestCallback().onSuccess(replyMsg);
                }
            }
            else
            {
                String bornHost = replyMsg.getBornHostString();
                log.Warn(String.Format("receive reply message, but not matched any request, CorrelationId: %s , reply from host: %s",
                    correlationId, bornHost));
            }
        }
    }
}
