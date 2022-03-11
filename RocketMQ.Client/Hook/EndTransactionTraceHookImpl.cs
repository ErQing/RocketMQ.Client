using System;

namespace RocketMQ.Client
{
    public class EndTransactionTraceHookImpl : EndTransactionHook
    {
        private TraceDispatcher localDispatcher;

        public EndTransactionTraceHookImpl(TraceDispatcher localDispatcher)
        {
            this.localDispatcher = localDispatcher;
        }

        //@Override
        public string hookName()
        {
            return "EndTransactionTraceHook";
        }

        //@Override
        public void endTransaction(EndTransactionContext context)
        {
            //if it is message trace data,then it doesn't recorded
            if (context == null || context.getMessage().getTopic().StartsWith(((AsyncTraceDispatcher)localDispatcher).getTraceTopicName()))
            {
                return;
            }
            Message msg = context.getMessage();
            //build the context content of TuxeTraceContext
            TraceContext tuxeContext = new TraceContext();
            tuxeContext.setTraceBeans(new ArrayList<TraceBean>(1));
            tuxeContext.setTraceType(TraceType.EndTransaction);
            tuxeContext.setGroupName(NamespaceUtil.withoutNamespace(context.getProducerGroup()));
            //build the data bean object of message trace
            TraceBean traceBean = new TraceBean();
            traceBean.setTopic(NamespaceUtil.withoutNamespace(context.getMessage().getTopic()));
            traceBean.setTags(context.getMessage().getTags());
            traceBean.setKeys(context.getMessage().getKeys());
            traceBean.setStoreHost(context.getBrokerAddr());
            traceBean.setMsgType(MessageType.Trans_msg_Commit);
            traceBean.setClientHost(((AsyncTraceDispatcher)localDispatcher).getHostProducer().getmQClientFactory().getClientId());
            traceBean.setMsgId(context.getMsgId());
            traceBean.setTransactionState(context.getTransactionState());
            traceBean.setTransactionId(context.getTransactionId());
            traceBean.setFromTransactionCheck(context.isFromTransactionCheck());
            string regionId = msg.getProperty(MessageConst.PROPERTY_MSG_REGION);
            if (regionId == null || regionId.isEmpty())
            {
                regionId = MixAll.DEFAULT_TRACE_REGION_ID;
            }
            tuxeContext.setRegionId(regionId);
            tuxeContext.getTraceBeans().Add(traceBean);
            tuxeContext.setTimeStamp(Sys.currentTimeMillis());
            localDispatcher.append(tuxeContext);
        }
    }
}
