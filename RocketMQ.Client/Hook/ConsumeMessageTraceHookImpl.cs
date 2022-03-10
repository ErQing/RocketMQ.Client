using System;
using System.Collections.Generic;

namespace RocketMQ.Client
{
    public class ConsumeMessageTraceHookImpl : ConsumeMessageHook
    {
        private TraceDispatcher localDispatcher;

        public ConsumeMessageTraceHookImpl(TraceDispatcher localDispatcher)
        {
            this.localDispatcher = localDispatcher;
        }

        //@Override
        public String hookName()
        {
            return "ConsumeMessageTraceHook";
        }

        //@Override
        public void consumeMessageBefore(ConsumeMessageContext context)
        {
            if (context == null || context.getMsgList() == null || context.getMsgList().isEmpty())
            {
                return;
            }
            TraceContext traceContext = new TraceContext();
            context.setMqTraceContext(traceContext);
            traceContext.setTraceType(TraceType.SubBefore);//
            traceContext.setGroupName(NamespaceUtil.withoutNamespace(context.getConsumerGroup()));//
            List<TraceBean> beans = new ArrayList<TraceBean>();
            foreach (MessageExt msg in context.getMsgList())
            {
                if (msg == null)
                {
                    continue;
                }
                String regionId = msg.getProperty(MessageConst.PROPERTY_MSG_REGION);
                String traceOn = msg.getProperty(MessageConst.PROPERTY_TRACE_SWITCH);

                if (traceOn != null && traceOn.Equals("false"))
                {
                    // If trace switch is false ,skip it
                    continue;
                }
                TraceBean traceBean = new TraceBean();
                traceBean.setTopic(NamespaceUtil.withoutNamespace(msg.getTopic()));//
                traceBean.setMsgId(msg.getMsgId());//
                traceBean.setTags(msg.getTags());//
                traceBean.setKeys(msg.getKeys());//
                traceBean.setStoreTime(msg.getStoreTimestamp());//
                traceBean.setBodyLength(msg.getStoreSize());//
                traceBean.setRetryTimes(msg.getReconsumeTimes());//
                traceContext.setRegionId(regionId);//
                beans.Add(traceBean);
            }
            if (beans.Count > 0)
            {
                traceContext.setTraceBeans(beans);
                traceContext.setTimeStamp(Sys.currentTimeMillis());
                localDispatcher.append(traceContext);
            }
        }

        //@Override
        public void consumeMessageAfter(ConsumeMessageContext context)
        {
            if (context == null || context.getMsgList() == null || context.getMsgList().isEmpty())
            {
                return;
            }
            TraceContext subBeforeContext = (TraceContext)context.getMqTraceContext();

            if (subBeforeContext.getTraceBeans() == null || subBeforeContext.getTraceBeans().Count < 1)
            {
                // If subBefore bean is null ,skip it
                return;
            }
            TraceContext subAfterContext = new TraceContext();
            subAfterContext.setTraceType(TraceType.SubAfter);//
            subAfterContext.setRegionId(subBeforeContext.getRegionId());//
            subAfterContext.setGroupName(NamespaceUtil.withoutNamespace(subBeforeContext.getGroupName()));//
            subAfterContext.setRequestId(subBeforeContext.getRequestId());//
            subAfterContext.setSuccess(context.isSuccess());//

            // Calculate the cost time for processing messages
            int costTime = (int)((Sys.currentTimeMillis() - subBeforeContext.getTimeStamp()) / context.getMsgList().Count);
            subAfterContext.setCostTime(costTime);//
            subAfterContext.setTraceBeans(subBeforeContext.getTraceBeans());
            var props = context.getProps();
            if (props != null)
            {
                String contextType = props.get(MixAll.CONSUME_CONTEXT_TYPE);
                if (contextType != null)
                {
                    //subAfterContext.setContextCode(ConsumeReturnType.valueOf(contextType).ordinal()); ///???
                    var ctype = contextType.ToEnum(ConsumeReturnType.FAILED);
                    subAfterContext.setContextCode((int)ctype);
                }
            }
            localDispatcher.append(subAfterContext);
        }
    }
}
