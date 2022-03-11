using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RocketMQ.Client
{
    public class TraceDataEncoder
    {
        /**
     * Resolving traceContext list From trace data String
     *
     * @param traceData
     * @return
     */
        public static List<TraceContext> decoderFromTraceDataString(String traceData)
        {
            List<TraceContext> resList = new ArrayList<TraceContext>();
            if (traceData == null || traceData.length() <= 0)
            {
                return resList;
            }
            String[] contextList = traceData.Split(Str.valueOf(TraceConstants.FIELD_SPLITOR));
            foreach (String context in contextList)
            {
                String[] line = context.Split(Str.valueOf(TraceConstants.CONTENT_SPLITOR));
                if (line[0].Equals(TraceType.Pub.name()))
                {
                    TraceContext pubContext = new TraceContext();
                    pubContext.setTraceType(TraceType.Pub);
                    pubContext.setTimeStamp(long.Parse(line[1]));
                    pubContext.setRegionId(line[2]);
                    pubContext.setGroupName(line[3]);
                    TraceBean bean = new TraceBean();
                    bean.setTopic(line[4]);
                    bean.setMsgId(line[5]);
                    bean.setTags(line[6]);
                    bean.setKeys(line[7]);
                    bean.setStoreHost(line[8]);
                    bean.setBodyLength(int.Parse(line[9]));
                    pubContext.setCostTime(int.Parse(line[10]));
                    //bean.setMsgType(MessageType.values()[int.Parse(line[11])]);
                    bean.setMsgType((MessageType)int.Parse(line[11]));//???

                    if (line.Length == 13)
                    {
                        pubContext.setSuccess(bool.Parse(line[12]));
                    }
                    else if (line.Length == 14)
                    {
                        bean.setOffsetMsgId(line[12]);
                        pubContext.setSuccess(bool.Parse(line[13]));
                    }

                    // compatible with the old version
                    if (line.Length >= 15)
                    {
                        bean.setOffsetMsgId(line[12]);
                        pubContext.setSuccess(bool.Parse(line[13]));
                        bean.setClientHost(line[14]);
                    }

                    pubContext.setTraceBeans(new ArrayList<TraceBean>(1));
                    pubContext.getTraceBeans().Add(bean);
                    resList.Add(pubContext);
                }
                else if (line[0].Equals(TraceType.SubBefore.name()))
                {
                    TraceContext subBeforeContext = new TraceContext();
                    subBeforeContext.setTraceType(TraceType.SubBefore);
                    subBeforeContext.setTimeStamp(long.Parse(line[1]));
                    subBeforeContext.setRegionId(line[2]);
                    subBeforeContext.setGroupName(line[3]);
                    subBeforeContext.setRequestId(line[4]);
                    TraceBean bean = new TraceBean();
                    bean.setMsgId(line[5]);
                    bean.setRetryTimes(int.Parse(line[6]));
                    bean.setKeys(line[7]);
                    subBeforeContext.setTraceBeans(new ArrayList<TraceBean>(1));
                    subBeforeContext.getTraceBeans().Add(bean);
                    resList.Add(subBeforeContext);
                }
                else if (line[0].Equals(TraceType.SubAfter.name()))
                {
                    TraceContext subAfterContext = new TraceContext();
                    subAfterContext.setTraceType(TraceType.SubAfter);
                    subAfterContext.setRequestId(line[1]);
                    TraceBean bean = new TraceBean();
                    bean.setMsgId(line[2]);
                    bean.setKeys(line[5]);
                    subAfterContext.setTraceBeans(new ArrayList<TraceBean>(1));
                    subAfterContext.getTraceBeans().Add(bean);
                    subAfterContext.setCostTime(int.Parse(line[3]));
                    subAfterContext.setSuccess(bool.Parse(line[4]));
                    if (line.Length >= 7)
                    {
                        // add the context type
                        subAfterContext.setContextCode(int.Parse(line[6]));
                    }
                    // compatible with the old version
                    if (line.Length >= 9)
                    {
                        subAfterContext.setTimeStamp(long.Parse(line[7]));
                        subAfterContext.setGroupName(line[8]);
                    }
                    resList.Add(subAfterContext);
                }
                else if (line[0].Equals(TraceType.EndTransaction.name()))
                {
                    TraceContext endTransactionContext = new TraceContext();
                    endTransactionContext.setTraceType(TraceType.EndTransaction);
                    endTransactionContext.setTimeStamp(long.Parse(line[1]));
                    endTransactionContext.setRegionId(line[2]);
                    endTransactionContext.setGroupName(line[3]);
                    TraceBean bean = new TraceBean();
                    bean.setTopic(line[4]);
                    bean.setMsgId(line[5]);
                    bean.setTags(line[6]);
                    bean.setKeys(line[7]);
                    bean.setStoreHost(line[8]);
                    //bean.setMsgType(MessageType.values()[int.Parse(line[9])]);
                    bean.setMsgType((MessageType)int.Parse(line[9]));//???
                    bean.setTransactionId(line[10]);
                    //bean.setTransactionState(LocalTransactionState.valueOf(line[11]));
                    bean.setTransactionState((LocalTransactionState)int.Parse(line[11]));//???
                    bean.setFromTransactionCheck(bool.Parse(line[12]));

                    endTransactionContext.setTraceBeans(new ArrayList<TraceBean>(1));
                    endTransactionContext.getTraceBeans().Add(bean);
                    resList.Add(endTransactionContext);
                }
            }
            return resList;
        }

        /**
         * Encoding the trace context into data strings and keyset sets
         *
         * @param ctx
         * @return
         */
        public static TraceTransferBean encoderFromContextBean(TraceContext ctx)
        {
            if (ctx == null)
            {
                return null;
            }
            //build message trace of the transfering entity content bean
            TraceTransferBean transferBean = new TraceTransferBean();
            StringBuilder sb = new StringBuilder(256);
            switch (ctx.getTraceType())
            {
                case TraceType.Pub:
                    {
                        TraceBean bean = ctx.getTraceBeans().Get(0);
                        //append the content of context and traceBean to transferBean's TransData
                        sb.Append(ctx.getTraceType()).Append(TraceConstants.CONTENT_SPLITOR)//
                            .Append(ctx.getTimeStamp()).Append(TraceConstants.CONTENT_SPLITOR)//
                            .Append(ctx.getRegionId()).Append(TraceConstants.CONTENT_SPLITOR)//
                            .Append(ctx.getGroupName()).Append(TraceConstants.CONTENT_SPLITOR)//
                            .Append(bean.getTopic()).Append(TraceConstants.CONTENT_SPLITOR)//
                            .Append(bean.getMsgId()).Append(TraceConstants.CONTENT_SPLITOR)//
                            .Append(bean.getTags()).Append(TraceConstants.CONTENT_SPLITOR)//
                            .Append(bean.getKeys()).Append(TraceConstants.CONTENT_SPLITOR)//
                            .Append(bean.getStoreHost()).Append(TraceConstants.CONTENT_SPLITOR)//
                            .Append(bean.getBodyLength()).Append(TraceConstants.CONTENT_SPLITOR)//
                            .Append(ctx.getCostTime()).Append(TraceConstants.CONTENT_SPLITOR)//
                            .Append((int)bean.getMsgType()).Append(TraceConstants.CONTENT_SPLITOR)//???
                            .Append(bean.getOffsetMsgId()).Append(TraceConstants.CONTENT_SPLITOR)//
                            .Append(ctx.GetIsSuccess()).Append(TraceConstants.FIELD_SPLITOR);//
                    }
                    break;
                case TraceType.SubBefore:
                    {
                        foreach (TraceBean bean in ctx.getTraceBeans())
                        {
                            sb.Append(ctx.getTraceType()).Append(TraceConstants.CONTENT_SPLITOR)//
                                .Append(ctx.getTimeStamp()).Append(TraceConstants.CONTENT_SPLITOR)//
                                .Append(ctx.getRegionId()).Append(TraceConstants.CONTENT_SPLITOR)//
                                .Append(ctx.getGroupName()).Append(TraceConstants.CONTENT_SPLITOR)//
                                .Append(ctx.getRequestId()).Append(TraceConstants.CONTENT_SPLITOR)//
                                .Append(bean.getMsgId()).Append(TraceConstants.CONTENT_SPLITOR)//
                                .Append(bean.getRetryTimes()).Append(TraceConstants.CONTENT_SPLITOR)//
                                .Append(bean.getKeys()).Append(TraceConstants.FIELD_SPLITOR);//
                        }
                    }
                    break;
                case TraceType.SubAfter:
                    {
                        foreach (TraceBean bean in ctx.getTraceBeans())
                        {
                            sb.Append(ctx.getTraceType()).Append(TraceConstants.CONTENT_SPLITOR)//
                                .Append(ctx.getRequestId()).Append(TraceConstants.CONTENT_SPLITOR)//
                                .Append(bean.getMsgId()).Append(TraceConstants.CONTENT_SPLITOR)//
                                .Append(ctx.getCostTime()).Append(TraceConstants.CONTENT_SPLITOR)//
                                .Append(ctx.GetIsSuccess()).Append(TraceConstants.CONTENT_SPLITOR)//
                                .Append(bean.getKeys()).Append(TraceConstants.CONTENT_SPLITOR)//
                                .Append(ctx.getContextCode()).Append(TraceConstants.CONTENT_SPLITOR)
                                .Append(ctx.getTimeStamp()).Append(TraceConstants.CONTENT_SPLITOR)
                                .Append(ctx.getGroupName()).Append(TraceConstants.FIELD_SPLITOR);
                        }
                    }
                    break;
                case TraceType.EndTransaction:
                    {
                        TraceBean bean = ctx.getTraceBeans().Get(0);
                        sb.Append(ctx.getTraceType()).Append(TraceConstants.CONTENT_SPLITOR)//
                            .Append(ctx.getTimeStamp()).Append(TraceConstants.CONTENT_SPLITOR)//
                            .Append(ctx.getRegionId()).Append(TraceConstants.CONTENT_SPLITOR)//
                            .Append(ctx.getGroupName()).Append(TraceConstants.CONTENT_SPLITOR)//
                            .Append(bean.getTopic()).Append(TraceConstants.CONTENT_SPLITOR)//
                            .Append(bean.getMsgId()).Append(TraceConstants.CONTENT_SPLITOR)//
                            .Append(bean.getTags()).Append(TraceConstants.CONTENT_SPLITOR)//
                            .Append(bean.getKeys()).Append(TraceConstants.CONTENT_SPLITOR)//
                            .Append(bean.getStoreHost()).Append(TraceConstants.CONTENT_SPLITOR)//
                            .Append((int)bean.getMsgType()).Append(TraceConstants.CONTENT_SPLITOR)//
                            .Append(bean.getTransactionId()).Append(TraceConstants.CONTENT_SPLITOR)//
                            .Append(bean.getTransactionState().name()).Append(TraceConstants.CONTENT_SPLITOR)//
                            .Append(bean.isFromTransactionCheck()).Append(TraceConstants.FIELD_SPLITOR);
                    }
                    break;
                default:
                    break; //???
            }
            transferBean.setTransData(sb.ToString());
            foreach (TraceBean bean in ctx.getTraceBeans())
            {

                transferBean.getTransKey().Add(bean.getMsgId());
                if (bean.getKeys() != null && bean.getKeys().length() > 0)
                {
                    String[] keys = bean.getKeys().Split(MessageConst.KEY_SEPARATOR);
                    //transferBean.getTransKey().addAll(Arrays.asList(keys));
                    transferBean.getTransKey().AddAll(keys);
                }
            }
            return transferBean;
        }
    }
}
