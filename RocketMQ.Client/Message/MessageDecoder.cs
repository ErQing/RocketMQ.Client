using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;

namespace RocketMQ.Client
{
    public class MessageDecoder
    {
        //    public final static int MSG_ID_LENGTH = 8 + 8;

        //public readonly static Charset CHARSET_UTF8 = Charset.forName("UTF-8");
        public readonly static Encoding CHARSET_UTF8 = Encoding.UTF8;
        public readonly static int MESSAGE_MAGIC_CODE_POSTION = 4;
        public readonly static int MESSAGE_FLAG_POSTION = 16;
        public readonly static int MESSAGE_PHYSIC_OFFSET_POSTION = 28;
        //    public final static int MESSAGE_STORE_TIMESTAMP_POSTION = 56;
        public readonly static int MESSAGE_MAGIC_CODE = -626843481;
        public static readonly char NAME_VALUE_SEPARATOR = '1';
        public static readonly char PROPERTY_SEPARATOR = '2';   
        public static readonly int PHY_POS_POSITION = 4 + 4 + 4 + 4 + 4 + 8;
        public static readonly int QUEUE_OFFSET_POSITION = 4 + 4 + 4 + 4 + 4;
        public static readonly int SYSFLAG_POSITION = 4 + 4 + 4 + 4 + 4 + 8 + 8;
        //    public static final int BODY_SIZE_POSITION = 4 // 1 TOTALSIZE
        //        + 4 // 2 MAGICCODE
        //        + 4 // 3 BODYCRC
        //        + 4 // 4 QUEUEID
        //        + 4 // 5 FLAG
        //        + 8 // 6 QUEUEOFFSET
        //        + 8 // 7 PHYSICALOFFSET
        //        + 4 // 8 SYSFLAG
        //        + 8 // 9 BORNTIMESTAMP
        //        + 8 // 10 BORNHOST
        //        + 8 // 11 STORETIMESTAMP
        //        + 8 // 12 STOREHOSTADDRESS
        //        + 4 // 13 RECONSUMETIMES
        //        + 8; // 14 Prepared Transaction Offset

        public static String createMessageId(ByteBuffer input, ByteBuffer addr, long offset)
        {
            input.flip();
            int msgIDLength = addr.limit == 8 ? 16 : 28;
            input.ResetLimit(msgIDLength);

            input.put(addr);
            input.putLong(offset);

            return UtilAll.bytes2string(input.Data);
        }

        public static string createMessageId(IPEndPoint socketAddress, long transactionIdhashCode)
        {
            IPAddress inetSocketAddress = socketAddress.Address;
            //int msgIDLength = inetSocketAddress.getAddress() is Inet4Address ? 16 : 28;
            int msgIDLength = inetSocketAddress.AddressFamily == System.Net.Sockets.AddressFamily.InterNetwork ? 16 : 28;
            ByteBuffer byteBuffer = ByteBuffer.allocate(msgIDLength);
            byteBuffer.put(inetSocketAddress.GetAddressBytes());
            byteBuffer.putInt(socketAddress.Port);
            byteBuffer.putLong(transactionIdhashCode);
            byteBuffer.flip();
            return UtilAll.bytes2string(byteBuffer.array());
        }

        ///<exception cref="UnknownHostException"/>
        public static MessageId decodeMessageId(String msgId)
        {
            IPEndPoint address;
            long offset;
            int ipLength = msgId.length() == 32 ? 4 * 2 : 16 * 2;

            byte[] ip = UtilAll.string2bytes(msgId.Substring(0, ipLength));
            byte[] port = UtilAll.string2bytes(msgId.Substring(ipLength, ipLength + 8));
            ByteBuffer bb = ByteBuffer.wrap(port);
            int portInt = bb.getInt(0);
            //address = new InetSocketAddress(InetAddress.getByAddress(ip), portInt);
            address = new IPEndPoint(new IPAddress(ip), portInt);

            // offset
            byte[] data = UtilAll.string2bytes(msgId.Substring(ipLength + 8, ipLength + 8 + 16));
            bb = ByteBuffer.wrap(data);
            offset = bb.getLong(0);

            return new MessageId(address, offset);
        }

        /**
         * Just decode properties from msg buffer.
         *
         * @param byteBuffer msg commit log buffer.
         */
        public static Dictionary<String, String> decodeProperties(ByteBuffer byteBuffer)
        {
            int sysFlag = byteBuffer.getInt(SYSFLAG_POSITION);
            int bornhostLength = (sysFlag & MessageSysFlag.BORNHOST_V6_FLAG) == 0 ? 8 : 20;
            int storehostAddressLength = (sysFlag & MessageSysFlag.STOREHOSTADDRESS_V6_FLAG) == 0 ? 8 : 20;
            int bodySizePosition = 4 // 1 TOTALSIZE
                + 4 // 2 MAGICCODE
                + 4 // 3 BODYCRC
                + 4 // 4 QUEUEID
                + 4 // 5 FLAG
                + 8 // 6 QUEUEOFFSET
                + 8 // 7 PHYSICALOFFSET
                + 4 // 8 SYSFLAG
                + 8 // 9 BORNTIMESTAMP
                + bornhostLength // 10 BORNHOST
                + 8 // 11 STORETIMESTAMP
                + storehostAddressLength // 12 STOREHOSTADDRESS
                + 4 // 13 RECONSUMETIMES
                + 8; // 14 Prepared Transaction Offset
            int topicLengthPosition = bodySizePosition + 4 + byteBuffer.getInt(bodySizePosition);

            byte topicLength = byteBuffer.get(topicLengthPosition);

            short propertiesLength = byteBuffer.getShort(topicLengthPosition + 1 + topicLength);

            byteBuffer.ResetPosition(topicLengthPosition + 1 + topicLength + 2);

            if (propertiesLength > 0)
            {
                byte[] properties = new byte[propertiesLength];
                byteBuffer.get(properties);
                //String propertiesString = new String(properties, CHARSET_UTF8);
                String propertiesString = CHARSET_UTF8.GetString(properties);
                var map = string2messageProperties(propertiesString);
                return map;
            }
            return null;
        }

        public static MessageExt decode(ByteBuffer byteBuffer)
        {
            return decode(byteBuffer, true, true, false);
        }

        public static MessageExt clientDecode(ByteBuffer byteBuffer, bool readBody)
        {
            return decode(byteBuffer, readBody, true, true);
        }

        public static MessageExt decode(ByteBuffer byteBuffer, bool readBody)
        {
            return decode(byteBuffer, readBody, true, false);
        }

        ///<exception cref="Exception"/>
        public static byte[] encode(MessageExt messageExt, bool needCompress)
        {
            byte[]
            body = messageExt.getBody();
            byte[]
            topics = messageExt.getTopic().getBytes(CHARSET_UTF8);
            byte topicLen = (byte)topics.Length;
            String properties = messageProperties2String(messageExt.getProperties());
            byte[]
            propertiesBytes = properties.getBytes(CHARSET_UTF8);
            short propertiesLength = (short)propertiesBytes.Length;
            int sysFlag = messageExt.getSysFlag();
            int bornhostLength = (sysFlag & MessageSysFlag.BORNHOST_V6_FLAG) == 0 ? 8 : 20;
            int storehostAddressLength = (sysFlag & MessageSysFlag.STOREHOSTADDRESS_V6_FLAG) == 0 ? 8 : 20;
            byte[]
            newBody = messageExt.getBody();
            if (needCompress && (sysFlag & MessageSysFlag.COMPRESSED_FLAG) == MessageSysFlag.COMPRESSED_FLAG)
            {
                newBody = UtilAll.compress(body, 5);
            }
            int bodyLength = newBody.Length;
            int storeSize = messageExt.getStoreSize();
            ByteBuffer byteBuffer;
            if (storeSize > 0)
            {
                byteBuffer = ByteBuffer.allocate(storeSize);
            }
            else
            {
                storeSize = 4 // 1 TOTALSIZE
                    + 4 // 2 MAGICCODE
                    + 4 // 3 BODYCRC
                    + 4 // 4 QUEUEID
                    + 4 // 5 FLAG
                    + 8 // 6 QUEUEOFFSET
                    + 8 // 7 PHYSICALOFFSET
                    + 4 // 8 SYSFLAG
                    + 8 // 9 BORNTIMESTAMP
                    + bornhostLength // 10 BORNHOST
                    + 8 // 11 STORETIMESTAMP
                    + storehostAddressLength // 12 STOREHOSTADDRESS
                    + 4 // 13 RECONSUMETIMES
                    + 8 // 14 Prepared Transaction Offset
                    + 4 + bodyLength // 14 BODY
                    + 1 + topicLen // 15 TOPIC
                    + 2 + propertiesLength // 16 propertiesLength
                    + 0;
                byteBuffer = ByteBuffer.allocate(storeSize);
            }
            // 1 TOTALSIZE
            byteBuffer.putInt(storeSize);

            // 2 MAGICCODE
            byteBuffer.putInt(MESSAGE_MAGIC_CODE);

            // 3 BODYCRC
            int bodyCRC = messageExt.getBodyCRC();
            byteBuffer.putInt(bodyCRC);

            // 4 QUEUEID
            int queueId = messageExt.getQueueId();
            byteBuffer.putInt(queueId);

            // 5 FLAG
            int flag = messageExt.getFlag();
            byteBuffer.putInt(flag);

            // 6 QUEUEOFFSET
            long queueOffset = messageExt.getQueueOffset();
            byteBuffer.putLong(queueOffset);

            // 7 PHYSICALOFFSET
            long physicOffset = messageExt.getCommitLogOffset();
            byteBuffer.putLong(physicOffset);

            // 8 SYSFLAG
            byteBuffer.putInt(sysFlag);

            // 9 BORNTIMESTAMP
            long bornTimeStamp = messageExt.getBornTimestamp();
            byteBuffer.putLong(bornTimeStamp);

            // 10 BORNHOST
            IPEndPoint bornHost = messageExt.getBornHost();
            byteBuffer.put(bornHost.Address.GetAddressBytes());
            byteBuffer.putInt(bornHost.Port);

            // 11 STORETIMESTAMP
            long storeTimestamp = messageExt.getStoreTimestamp();
            byteBuffer.putLong(storeTimestamp);

            // 12 STOREHOST
            IPEndPoint serverHost = messageExt.getStoreHost();
            byteBuffer.put(serverHost.Address.GetAddressBytes());
            byteBuffer.putInt(serverHost.Port);

            // 13 RECONSUMETIMES
            int reconsumeTimes = messageExt.getReconsumeTimes();
            byteBuffer.putInt(reconsumeTimes);

            // 14 Prepared Transaction Offset
            long preparedTransactionOffset = messageExt.getPreparedTransactionOffset();
            byteBuffer.putLong(preparedTransactionOffset);

            // 15 BODY
            byteBuffer.putInt(bodyLength);
            byteBuffer.put(newBody);

            // 16 TOPIC
            byteBuffer.put(topicLen);
            byteBuffer.put(topics);

            // 17 properties
            byteBuffer.putShort(propertiesLength);
            byteBuffer.put(propertiesBytes);

            return byteBuffer.array();
        }

        public static MessageExt decode(
            ByteBuffer byteBuffer, bool readBody, bool deCompressBody)
        {
            return decode(byteBuffer, readBody, deCompressBody, false);
        }

        public static MessageExt decode(ByteBuffer byteBuffer, bool readBody, bool deCompressBody, bool isClient)
        {
            try
            {

                MessageExt msgExt;
                if (isClient)
                {
                    msgExt = new MessageClientExt();
                }
                else
                {
                    msgExt = new MessageExt();
                }

                // 1 TOTALSIZE
                int storeSize = byteBuffer.getInt();
                msgExt.setStoreSize(storeSize);

                // 2 MAGICCODE
                byteBuffer.getInt();

                // 3 BODYCRC
                int bodyCRC = byteBuffer.getInt();
                msgExt.setBodyCRC(bodyCRC);

                // 4 QUEUEID
                int queueId = byteBuffer.getInt();
                msgExt.setQueueId(queueId);

                // 5 FLAG
                int flag = byteBuffer.getInt();
                msgExt.setFlag(flag);

                // 6 QUEUEOFFSET
                long queueOffset = byteBuffer.getLong();
                msgExt.setQueueOffset(queueOffset);

                // 7 PHYSICALOFFSET
                long physicOffset = byteBuffer.getLong();
                msgExt.setCommitLogOffset(physicOffset);

                // 8 SYSFLAG
                int sysFlag = byteBuffer.getInt();
                msgExt.setSysFlag(sysFlag);

                // 9 BORNTIMESTAMP
                long bornTimeStamp = byteBuffer.getLong();
                msgExt.setBornTimestamp(bornTimeStamp);

                // 10 BORNHOST
                int bornhostIPLength = (sysFlag & MessageSysFlag.BORNHOST_V6_FLAG) == 0 ? 4 : 16;
                byte[] bornHost = new byte[bornhostIPLength];
                byteBuffer.get(bornHost, 0, bornhostIPLength);
                int port = byteBuffer.getInt();
                //msgExt.setBornHost(new InetSocketAddress(InetAddress.getByAddress(bornHost), port));
                msgExt.setBornHost(new IPEndPoint(new IPAddress(bornHost), port));

                // 11 STORETIMESTAMP
                long storeTimestamp = byteBuffer.getLong();
                msgExt.setStoreTimestamp(storeTimestamp);

                // 12 STOREHOST
                int storehostIPLength = (sysFlag & MessageSysFlag.STOREHOSTADDRESS_V6_FLAG) == 0 ? 4 : 16;
                byte[] storeHost = new byte[storehostIPLength];
                byteBuffer.get(storeHost, 0, storehostIPLength);
                port = byteBuffer.getInt();
                //msgExt.setStoreHost(new InetSocketAddress(InetAddress.getByAddress(storeHost), port));
                msgExt.setStoreHost(new IPEndPoint(new IPAddress(storeHost), port));

                // 13 RECONSUMETIMES
                int reconsumeTimes = byteBuffer.getInt();
                msgExt.setReconsumeTimes(reconsumeTimes);

                // 14 Prepared Transaction Offset
                long preparedTransactionOffset = byteBuffer.getLong();
                msgExt.setPreparedTransactionOffset(preparedTransactionOffset);

                // 15 BODY
                int bodyLen = byteBuffer.getInt();
                if (bodyLen > 0)
                {
                    if (readBody)
                    {
                        byte[] body = new byte[bodyLen];
                        byteBuffer.get(body);

                        // uncompress body
                        if (deCompressBody && (sysFlag & MessageSysFlag.COMPRESSED_FLAG) == MessageSysFlag.COMPRESSED_FLAG)
                        {
                            body = UtilAll.uncompress(body);
                        }

                        msgExt.setBody(body);
                    }
                    else
                    {
                        byteBuffer.ResetPosition(byteBuffer.Position + bodyLen);
                    }
                }

                // 16 TOPIC
                byte topicLen = byteBuffer.get();
                byte[] topic = new byte[(int)topicLen];
                byteBuffer.get(topic);
                //msgExt.setTopic(new String(topic, CHARSET_UTF8));
                msgExt.setTopic(CHARSET_UTF8.GetString(topic));

                // 17 properties
                short propertiesLength = byteBuffer.getShort();
                if (propertiesLength > 0)
                {
                    byte[] properties = new byte[propertiesLength];
                    byteBuffer.get(properties);
                    //String propertiesString = new String(properties, CHARSET_UTF8);
                    String propertiesString = CHARSET_UTF8.GetString(properties);
                    var map = string2messageProperties(propertiesString);
                    msgExt.setProperties(map);
                }

                int msgIDLength = storehostIPLength + 4 + 8;
                ByteBuffer byteBufferMsgId = ByteBuffer.allocate(msgIDLength);
                String msgId = createMessageId(byteBufferMsgId, msgExt.getStoreHostBytes(), msgExt.getCommitLogOffset());
                msgExt.setMsgId(msgId);

                if (isClient)
                {
                    ((MessageClientExt)msgExt).setOffsetMsgId(msgId);
                }

                return msgExt;
            }
            catch (Exception e)
            {
                byteBuffer.ResetPosition(byteBuffer.limit);
            }
            return null;
        }

        public static List<MessageExt> decodes(ByteBuffer byteBuffer)
        {
            return decodes(byteBuffer, true);
        }

        public static List<MessageExt> decodes(ByteBuffer byteBuffer, bool readBody)
        {
            List<MessageExt> msgExts = new List<MessageExt>();
            while (byteBuffer.hasRemaining())
            {
                MessageExt msgExt = clientDecode(byteBuffer, readBody);
                if (null != msgExt)
                {
                    msgExts.Add(msgExt);
                }
                else
                {
                    break;
                }
            }
            return msgExts;
        }

        public static String messageProperties2String(Dictionary<string, string> properties)
        {
            if (properties == null)
            {
                return "";
            }
            int len = 0;
            foreach (var entry in properties)
            {
                string name = entry.Key;
                string value = entry.Value;
                if (value == null)
                {
                    continue;
                }
                if (name != null)
                {
                    len += name.length();
                }
                len += value.length();
                len += 2; // separator
            }
            StringBuilder sb = new StringBuilder(len);
            if (properties != null)
            {
                foreach (var entry in properties)
                {
                    String name = entry.Key;
                    String value = entry.Value;

                    if (value == null)
                    {
                        continue;
                    }
                    sb.Append(name);
                    sb.Append(NAME_VALUE_SEPARATOR);
                    sb.Append(value);
                    sb.Append(PROPERTY_SEPARATOR);
                }
                if (sb.Length > 0)
                {
                    sb.deleteCharAt(sb.Length - 1);
                }
            }
            return sb.ToString();
        }

        public static Dictionary<String, String> string2messageProperties(String properties)
        {
            var map = new HashMap<String, String>();
            if (properties != null)
            {
                int len = properties.length();
                int index = 0;
                while (index < len)
                {
                    int newIndex = properties.IndexOf(PROPERTY_SEPARATOR, index);
                    if (newIndex < 0)
                    {
                        newIndex = len;
                    }
                    if (newIndex - index >= 3)
                    {
                        int kvSepIndex = properties.IndexOf(NAME_VALUE_SEPARATOR, index);
                        if (kvSepIndex > index && kvSepIndex < newIndex - 1)
                        {
                            String k = properties.JavaSubstring(index, kvSepIndex);
                            String v = properties.JavaSubstring(kvSepIndex + 1, newIndex);
                            map.put(k, v);
                        }
                    }
                    index = newIndex + 1;
                }
            }

            return map;
        }

        public static byte[] encodeMessage(Message message)
        {
            //only need flag, body, properties
            byte[] body = message.getBody();
            int bodyLen = body.Length;
            String properties = messageProperties2String(message.getProperties());
            byte[] propertiesBytes = properties.getBytes(CHARSET_UTF8);
            //note properties length must not more than Short.MAX
            int propsLen = propertiesBytes.Length;
            if (propsLen > short.MaxValue)
                throw new Exception(String.Format("Properties size of message exceeded, properties size: {}, maxSize: {}.", propsLen, short.MaxValue));
            short propertiesLength = (short)propsLen;
            int sysFlag = message.getFlag();
            int storeSize = 4 // 1 TOTALSIZE
                + 4 // 2 MAGICCOD
                + 4 // 3 BODYCRC
                + 4 // 4 FLAG
                + 4 + bodyLen // 4 BODY
                + 2 + propertiesLength;
            ByteBuffer byteBuffer = ByteBuffer.allocate(storeSize);
            // 1 TOTALSIZE
            byteBuffer.putInt(storeSize);

            // 2 MAGICCODE
            byteBuffer.putInt(0);

            // 3 BODYCRC
            byteBuffer.putInt(0);

            // 4 FLAG
            int flag = message.getFlag();
            byteBuffer.putInt(flag);

            // 5 BODY
            byteBuffer.putInt(bodyLen);
            byteBuffer.put(body);

            // 6 properties
            byteBuffer.putShort(propertiesLength);
            byteBuffer.put(propertiesBytes);

            return byteBuffer.array();
        }


        ///<exception cref="Exception"/>
        public static Message decodeMessage(ByteBuffer byteBuffer)
        {
            Message message = new Message();

            // 1 TOTALSIZE
            byteBuffer.getInt();

            // 2 MAGICCODE
            byteBuffer.getInt();

            // 3 BODYCRC
            byteBuffer.getInt();

            // 4 FLAG
            int flag = byteBuffer.getInt();
            message.setFlag(flag);

            // 5 BODY
            int bodyLen = byteBuffer.getInt();
            byte[] body = new byte[bodyLen];
            byteBuffer.get(body);
            message.setBody(body);

            // 6 properties
            short propertiesLen = byteBuffer.getShort();
            byte[] propertiesBytes = new byte[propertiesLen];
            byteBuffer.get(propertiesBytes);
            //message.setProperties(string2messageProperties(new String(propertiesBytes, CHARSET_UTF8)));
            message.setProperties(string2messageProperties(CHARSET_UTF8.GetString(propertiesBytes)));

            return message;
        }

        public static byte[] encodeMessages(List<Message> messages)
        {
            //TO DO refactor, accumulate in one buffer, avoid copies
            List<byte[]> encodedMessages = new ArrayList<byte[]>(messages.Count);
            int allSize = 0;
            foreach (Message message in messages)
            {
                byte[] tmp = encodeMessage(message);
                encodedMessages.Add(tmp);
                allSize += tmp.Length;
            }
            byte[] allBytes = new byte[allSize];
            int pos = 0;
            foreach (byte[] bytes in encodedMessages)
            {
                //System.arraycopy(bytes, 0, allBytes, pos, bytes.length);
                System.Array.Copy(bytes, 0, allBytes, pos, bytes.Length); //???
                pos += bytes.Length;
            }
            return allBytes;
        }

        ///<exception cref="Exception"/>
        public static List<Message> decodeMessages(ByteBuffer byteBuffer)
        {
            //TO DO add a callback for processing,  avoid creating lists
            List<Message> msgs = new ArrayList<Message>();
            while (byteBuffer.hasRemaining())
            {
                Message msg = decodeMessage(byteBuffer);
                msgs.Add(msg);
            }
            return msgs;
        }
    }
}
