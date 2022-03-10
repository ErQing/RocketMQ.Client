using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RocketMQ.Client
{
    public class RocketMQSerializable
    {
        //private static readonly Charset CHARSET_UTF8 = Charset.forName("UTF-8");
        private static readonly System.Text.Encoding CHARSET_UTF8 = System.Text.Encoding.UTF8;

        public static byte[] rocketMQProtocolEncode(RemotingCommand cmd)
        {
            // String remark
            byte[] remarkBytes = null;
            int remarkLen = 0;
            if (cmd.getRemark() != null && cmd.getRemark().length() > 0)
            {
                remarkBytes = cmd.getRemark().getBytes(CHARSET_UTF8);
                remarkLen = remarkBytes.Length;
            }

            // HashMap<String, String> extFields
            byte[] extFieldsBytes = null;
            int extLen = 0;
            if (cmd.getExtFields() != null && !cmd.getExtFields().isEmpty())
            {
                extFieldsBytes = mapSerialize(cmd.getExtFields());
                extLen = extFieldsBytes.Length;
            }

            int totalLen = calTotalLen(remarkLen, extLen);

            ByteBuffer headerBuffer = ByteBuffer.allocate(totalLen);
            // int code(~32767)
            headerBuffer.putShort((short)cmd.getCode());
            // LanguageCode language
            //headerBuffer.put(cmd.getLanguage().getCode());
            headerBuffer.put((byte)cmd.getLanguage()); //???
            // int version(~32767)
            headerBuffer.putShort((short)cmd.getVersion());
            // int opaque
            headerBuffer.putInt(cmd.getOpaque());
            // int flag
            headerBuffer.putInt(cmd.getFlag());
            // String remark
            if (remarkBytes != null)
            {
                headerBuffer.putInt(remarkBytes.Length);
                headerBuffer.put(remarkBytes);
            }
            else
            {
                headerBuffer.putInt(0);
            }
            // HashMap<String, String> extFields;
            if (extFieldsBytes != null)
            {
                headerBuffer.putInt(extFieldsBytes.Length);
                headerBuffer.put(extFieldsBytes);
            }
            else
            {
                headerBuffer.putInt(0);
            }

            return headerBuffer.array();
        }

        public static byte[] mapSerialize(HashMap<String, String> map)
        {
            // keySize+key+valSize+val
            if (null == map || map.isEmpty())
                return null;

            int totalLength = 0;
            int kvLength;
            //Iterator<Map.Entry<String, String>> it = map.entrySet().iterator();
            //while (it.hasNext())
            foreach (var entry in map)
            {
                //Map.Entry<String, String> entry = it.next();
                if (entry.Key != null && entry.Value != null)
                {
                    kvLength =
                        // keySize + Key
                        2 + entry.Key.getBytes(CHARSET_UTF8).Length
                            // valSize + val
                            + 4 + entry.Value.getBytes(CHARSET_UTF8).Length;
                    totalLength += kvLength;
                }
            }

            ByteBuffer content = ByteBuffer.allocate(totalLength);
            byte[] key;
            byte[] val;
            //it = map.entrySet().iterator();
            //while (it.hasNext())
            foreach (var entry in map)
            {
                //Map.Entry<String, String> entry = it.next();
                if (entry.Key != null && entry.Value != null)
                {
                    key = entry.Key.getBytes(CHARSET_UTF8);
                    val = entry.Value.getBytes(CHARSET_UTF8);

                    content.putShort((short)key.Length);
                    content.put(key);

                    content.putInt(val.Length);
                    content.put(val);
                }
            }

            return content.array();
        }

        private static int calTotalLen(int remark, int ext)
        {
            // int code(~32767)
            int length = 2
                // LanguageCode language
                + 1
                // int version(~32767)
                + 2
                // int opaque
                + 4
                // int flag
                + 4
                // String remark
                + 4 + remark
                // HashMap<String, String> extFields
                + 4 + ext;

            return length;
        }

        ///<exception cref="RemotingCommandException"/>
        public static RemotingCommand rocketMQProtocolDecode(byte[] headerArray)
        {
            RemotingCommand cmd = new RemotingCommand();
            ByteBuffer headerBuffer = ByteBuffer.wrap(headerArray);
            // int code(~32767)
            cmd.setCode(headerBuffer.getShort());
            // LanguageCode language
            //headerBuffer.put(cmd.getLanguage().getCode());
            cmd.setLanguage((LanguageCode)headerBuffer.get()); //???
            // int version(~32767)
            cmd.setVersion(headerBuffer.getShort());
            // int opaque
            cmd.setOpaque(headerBuffer.getInt());
            // int flag
            cmd.setFlag(headerBuffer.getInt());
            // String remark
            int remarkLength = headerBuffer.getInt();
            if (remarkLength > 0)
            {
                if (remarkLength > headerArray.Length)
                {
                    throw new RemotingCommandException("RocketMQ protocol decoding failed, remark length: " + remarkLength + ", but header length: " + headerArray.Length);
                }
                byte[] remarkContent = new byte[remarkLength];
                headerBuffer.get(remarkContent);
                //cmd.setRemark(new String(remarkContent, CHARSET_UTF8));
                cmd.setRemark(CHARSET_UTF8.GetString(remarkContent));
            }

            // HashMap<String, String> extFields
            int extFieldsLength = headerBuffer.getInt();
            if (extFieldsLength > 0)
            {
                if (extFieldsLength > headerArray.Length)
                {
                    throw new RemotingCommandException("RocketMQ protocol decoding failed, extFields length: " + extFieldsLength + ", but header length: " + headerArray.Length);
                }
                byte[] extFieldsBytes = new byte[extFieldsLength];
                headerBuffer.get(extFieldsBytes);
                cmd.setExtFields(mapDeserialize(extFieldsBytes));
            }
            return cmd;
        }

        public static HashMap<String, String> mapDeserialize(byte[] bytes)
        {
            if (bytes == null || bytes.Length <= 0)
                return null;

            HashMap<String, String> map = new HashMap<String, String>();
            ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);

            short keySize;
            byte[] keyContent;
            int valSize;
            byte[] valContent;
            while (byteBuffer.hasRemaining())
            {
                keySize = byteBuffer.getShort();
                keyContent = new byte[keySize];
                byteBuffer.get(keyContent);

                valSize = byteBuffer.getInt();
                valContent = new byte[valSize];
                byteBuffer.get(valContent);

                //map.put(new String(keyContent, CHARSET_UTF8), new String(valContent, CHARSET_UTF8));
                map.put(CHARSET_UTF8.GetString(keyContent), CHARSET_UTF8.GetString(valContent));
            }
            return map;
        }

        public static bool isBlank(String str)
        {
            return string.IsNullOrWhiteSpace(str);
            //int strLen;
            //if (str == null || (strLen = str.length()) == 0)
            //{
            //    return true;
            //}
            //for (int i = 0; i < strLen; i++)
            //{
            //    if (!Character.isWhitespace(str.charAt(i)))
            //    {
            //        return false;
            //    }
            //}
            //return true;
        }
    }
}
