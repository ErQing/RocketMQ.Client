using System;
using System.Collections.Generic;
using System.Net;
using System.Text;

namespace RocketMQ.Client
{
    public class MessageExt : Message
    {
        private String brokerName;

        private int queueId;

        private int storeSize;

        private long queueOffset;
        private int sysFlag;
        private long bornTimestamp;
        private IPEndPoint bornHost;

        private long storeTimestamp;
        private IPEndPoint storeHost;
        private string msgId;
        private long commitLogOffset;
        private int bodyCRC;
        private int reconsumeTimes;

        private long preparedTransactionOffset;

        public MessageExt()
        {
        }

        public MessageExt(int queueId, long bornTimestamp, IPEndPoint bornHost, long storeTimestamp, IPEndPoint storeHost, string msgId)
        {
            this.queueId = queueId;
            this.bornTimestamp = bornTimestamp;
            this.bornHost = bornHost;
            this.storeTimestamp = storeTimestamp;
            this.storeHost = storeHost;
            this.msgId = msgId;
        }

        public static TopicFilterType parseTopicFilterType(int sysFlag)
        {
            if ((sysFlag & MessageSysFlag.MULTI_TAGS_FLAG) == MessageSysFlag.MULTI_TAGS_FLAG)
            {
                return TopicFilterType.MULTI_TAG;
            }

            return TopicFilterType.SINGLE_TAG;
        }

        public static ByteBuffer socketAddress2ByteBuffer(IPEndPoint endPoint, ByteBuffer byteBuffer)
        {
            byteBuffer.put(endPoint.Address.GetAddressBytes());
            byteBuffer.putInt(endPoint.Port);
            byteBuffer.flip();
            return byteBuffer;
        }

        public static ByteBuffer socketAddress2ByteBuffer(IPEndPoint endPoint)
        {
            ByteBuffer byteBuffer;
            if (endPoint.AddressFamily is System.Net.Sockets.AddressFamily.InterNetwork) //ipv4
            {
                byteBuffer = ByteBuffer.allocate(4 + 4);
            }
            else
            {
                byteBuffer = ByteBuffer.allocate(16 + 4);
            }
            return socketAddress2ByteBuffer(endPoint, byteBuffer);
        }

        public ByteBuffer getBornHostBytes()
        {
            return socketAddress2ByteBuffer(this.bornHost);
        }

        public ByteBuffer getBornHostBytes(ByteBuffer byteBuffer)
        {
            return socketAddress2ByteBuffer(this.bornHost, byteBuffer);
        }

        public ByteBuffer getStoreHostBytes()
        {
            return socketAddress2ByteBuffer(this.storeHost);
        }

        public ByteBuffer getStoreHostBytes(ByteBuffer byteBuffer)
        {
            return socketAddress2ByteBuffer(this.storeHost, byteBuffer);
        }

        public String getBrokerName()
        {
            return brokerName;
        }

        public void setBrokerName(String brokerName)
        {
            this.brokerName = brokerName;
        }

        public int getQueueId()
        {
            return queueId;
        }

        public void setQueueId(int queueId)
        {
            this.queueId = queueId;
        }

        public long getBornTimestamp()
        {
            return bornTimestamp;
        }

        public void setBornTimestamp(long bornTimestamp)
        {
            this.bornTimestamp = bornTimestamp;
        }

        public IPEndPoint getBornHost()
        {
            return bornHost;
        }

        public void setBornHost(IPEndPoint bornHost)
        {
            this.bornHost = bornHost;
        }

        /// <summary>
        /// 主机ip地址（不包含端口）
        /// </summary>
        /// <returns></returns>
        public String getBornHostString()
        {
            if (null != this.bornHost)
            {
                //InetAddress inetAddress = ((InetSocketAddress)this.bornHost).getAddress();
                //return null != inetAddress ? inetAddress.getHostAddress() : null;
                return bornHost.Address.ToString();
            }

            return null;
        }

        /// <summary>
        /// 主机名字
        /// </summary>
        /// <returns></returns>
        public String getBornHostNameString()
        {
            if (null != this.bornHost)
            {
                //if (bornHost instanceof InetSocketAddress) {
                //    // without reverse dns lookup
                //    return ((InetSocketAddress)bornHost).getHostString();
                //}
                //InetAddress inetAddress = ((InetSocketAddress)this.bornHost).getAddress();
                //return null != inetAddress ? inetAddress.getHostName() : null;
                return bornHost.Address.ToString();
            }
            return null;
        }

        public long getStoreTimestamp()
        {
            return storeTimestamp;
        }

        public void setStoreTimestamp(long storeTimestamp)
        {
            this.storeTimestamp = storeTimestamp;
        }

        public IPEndPoint getStoreHost()
        {
            return storeHost;
        }

        public void setStoreHost(IPEndPoint storeHost)
        {
            this.storeHost = storeHost;
        }

        public virtual String getMsgId()
        {
            return msgId;
        }

        public void setMsgId(String msgId)
        {
            this.msgId = msgId;
        }

        public int getSysFlag()
        {
            return sysFlag;
        }

        public void setSysFlag(int sysFlag)
        {
            this.sysFlag = sysFlag;
        }

        public void setStoreHostAddressV6Flag() { this.sysFlag = this.sysFlag | MessageSysFlag.STOREHOSTADDRESS_V6_FLAG; }

        public void setBornHostV6Flag() { this.sysFlag = this.sysFlag | MessageSysFlag.BORNHOST_V6_FLAG; }

        public int getBodyCRC()
        {
            return bodyCRC;
        }

        public void setBodyCRC(int bodyCRC)
        {
            this.bodyCRC = bodyCRC;
        }

        public long getQueueOffset()
        {
            return queueOffset;
        }

        public void setQueueOffset(long queueOffset)
        {
            this.queueOffset = queueOffset;
        }

        public long getCommitLogOffset()
        {
            return commitLogOffset;
        }

        public void setCommitLogOffset(long physicOffset)
        {
            this.commitLogOffset = physicOffset;
        }

        public int getStoreSize()
        {
            return storeSize;
        }

        public void setStoreSize(int storeSize)
        {
            this.storeSize = storeSize;
        }

        public int getReconsumeTimes()
        {
            return reconsumeTimes;
        }

        public void setReconsumeTimes(int reconsumeTimes)
        {
            this.reconsumeTimes = reconsumeTimes;
        }

        public long getPreparedTransactionOffset()
        {
            return preparedTransactionOffset;
        }

        public void setPreparedTransactionOffset(long preparedTransactionOffset)
        {
            this.preparedTransactionOffset = preparedTransactionOffset;
        }

        public override String ToString()
        {
            return "MessageExt [brokerName=" + brokerName + ", queueId=" + queueId + ", storeSize=" + storeSize + ", queueOffset=" + queueOffset
                + ", sysFlag=" + sysFlag + ", bornTimestamp=" + bornTimestamp + ", bornHost=" + bornHost
                + ", storeTimestamp=" + storeTimestamp + ", storeHost=" + storeHost + ", msgId=" + msgId
                + ", commitLogOffset=" + commitLogOffset + ", bodyCRC=" + bodyCRC + ", reconsumeTimes="
                + reconsumeTimes + ", preparedTransactionOffset=" + preparedTransactionOffset
                + ", toString()=" + base.ToString() + "]";
        }
    }
}
