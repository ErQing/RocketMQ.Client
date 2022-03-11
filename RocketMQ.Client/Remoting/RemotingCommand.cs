using System;
using System.Reflection;

namespace RocketMQ.Client
{

    [AttributeUsage(AttributeTargets.Property, AllowMultiple = true)]
    public class CFNotNull : Attribute { };

    [AttributeUsage(AttributeTargets.Property, AllowMultiple = true)]
    public class CFNullable : Attribute { };

    [AttributeUsage(AttributeTargets.Property, AllowMultiple = true)]
    public class ImportantField : Attribute { };

    public class RemotingCommand
    {
        public static readonly string SERIALIZE_TYPE_PROPERTY = "rocketmq.serialize.type";
        public static readonly string SERIALIZE_TYPE_ENV = "ROCKETMQ_SERIALIZE_TYPE";
        public static readonly string REMOTING_VERSION_KEY = "rocketmq.remoting.version";
        //private static readonly InternalLogger log = InternalLoggerFactory.getLogger(RemotingHelper.ROCKETMQ_REMOTING);
        static NLog.Logger log = NLog.LogManager.GetCurrentClassLogger();
        private static readonly int RPC_TYPE = 0; // 0, REQUEST_COMMAND
        private static readonly int RPC_ONEWAY = 1; // 0, RPC

        //缓存避免每次都反射获取
        //private static readonly HashMap<Class<? extends CommandCustomHeader>, Field[]> CLASS_HASH_MAP =
            //new HashMap<Class<? extends CommandCustomHeader>, Field[]>();
        private static readonly HashMap<Type, PropertyInfo[]> CLASS_HASH_MAP = new HashMap<Type, PropertyInfo[]>();
        private static readonly HashMap<Type, String> CANONICAL_NAME_CACHE = new HashMap<Type, String>();
        // 1, Oneway
        // 1, RESPONSE_COMMAND
        private static readonly HashMap<PropertyInfo, bool> NULLABLE_FIELD_CACHE = new HashMap<PropertyInfo, bool>();
        //private static readonly string STRING_CANONICAL_NAME = String.class.getCanonicalName();
        //private static readonly string DOUBLE_CANONICAL_NAME_1 = Double.class.getCanonicalName();
        //private static readonly string DOUBLE_CANONICAL_NAME_2 = double.class.getCanonicalName();
        //private static readonly string INTEGER_CANONICAL_NAME_1 = Integer.class.getCanonicalName();
        //private static readonly string INTEGER_CANONICAL_NAME_2 = int.class.getCanonicalName();
        //private static readonly string LONG_CANONICAL_NAME_1 = Long.class.getCanonicalName();
        //private static readonly string LONG_CANONICAL_NAME_2 = long.class.getCanonicalName();
        //private static readonly string BOOLEAN_CANONICAL_NAME_1 = bool.class.getCanonicalName();
        //private static readonly string BOOLEAN_CANONICAL_NAME_2 = bool.class.getCanonicalName();
        private static readonly Type STRING_CANONICAL_NAME = typeof(string);
        private static readonly Type DOUBLE_CANONICAL_NAME_1 = typeof(Double);
        private static readonly Type DOUBLE_CANONICAL_NAME_2 = typeof(double);
        private static readonly Type INTEGER_CANONICAL_NAME_1 = typeof(Int32);
        private static readonly Type INTEGER_CANONICAL_NAME_2 = typeof(int);
        private static readonly Type LONG_CANONICAL_NAME_1 = typeof(Int64);
        private static readonly Type LONG_CANONICAL_NAME_2 = typeof(long);
        private static readonly Type BOOLEAN_CANONICAL_NAME_1 = typeof(Boolean);
        private static readonly Type BOOLEAN_CANONICAL_NAME_2 = typeof(bool);


        private static volatile int configVersion = -1;
        private static AtomicInteger requestId = new AtomicInteger(0);

        private static SerializeType serializeTypeConfigInThisServer = SerializeType.JSON;

        static RemotingCommand()
        {
            string protocol = Sys.getProperty(SERIALIZE_TYPE_PROPERTY, Sys.getenv(SERIALIZE_TYPE_ENV));
            if (!isBlank(protocol))
            {
                serializeTypeConfigInThisServer = protocol.ToEnum(SerializeType.JSON);
                if (serializeTypeConfigInThisServer == SerializeType.UNKNOW)
                    throw new RuntimeException("parser specified protocol error. protocol=" + protocol);
                //serializeTypeConfigInThisServer = SerializeType.valueOf(protocol);
            }
        }

        private int code;
        private LanguageCode language = LanguageCode.JAVA;
        private int version = 0;
        private int opaque = requestId.getAndIncrement();
        private int flag = 0;
        private string remark;
        private HashMap<String, String> extFields;
        //private transient CommandCustomHeader customHeader; //???
        private CommandCustomHeader customHeader;

        private SerializeType serializeTypeCurrentRPC = serializeTypeConfigInThisServer;

        //private transient byte[] body; //???
        private byte[] body;

        internal RemotingCommand()
        {
        }

        public static RemotingCommand createRequestCommand(int code, CommandCustomHeader customHeader)
        {
            RemotingCommand cmd = new RemotingCommand();
            cmd.setCode(code);
            cmd.customHeader = customHeader;
            setCmdVersion(cmd);
            return cmd;
        }

        private static void setCmdVersion(RemotingCommand cmd)
        {
            if (configVersion >= 0)
            {
                cmd.setVersion(configVersion);
            }
            else
            {
                string v = Sys.getProperty(REMOTING_VERSION_KEY);
                if (v != null)
                {
                    int value = int.Parse(v);
                    cmd.setVersion(value);
                    configVersion = value;
                }
            }
        }

        //public static RemotingCommand createResponseCommand(Class<? extends CommandCustomHeader> classHeader)
        public static RemotingCommand createResponseCommand<T>() where T : CommandCustomHeader
        {
            return createResponseCommand(RemotingSysResponseCode.SYSTEM_ERROR, "not set any response code"/*, classHeader*/);
        }

        public static RemotingCommand createResponseCommand()
        {
            return createResponseCommand(RemotingSysResponseCode.SYSTEM_ERROR, "not set any response code");
        }

        //public static RemotingCommand createResponseCommand(int code, string remark, Class<? extends CommandCustomHeader> classHeader)
        public static RemotingCommand createResponseCommand<T>(int code, string remark) where T : CommandCustomHeader, new()
        {
            RemotingCommand cmd = new RemotingCommand();
            cmd.markResponseType();
            cmd.setCode(code);
            cmd.setRemark(remark);
            setCmdVersion(cmd);

            //if (classHeader != null)
            {
                try
                {
                    //CommandCustomHeader objectHeader = Activator.CreateInstance(typeof(T));
                    CommandCustomHeader objectHeader = new T();
                    //CommandCustomHeader objectHeader = classHeader.newInstance();
                    cmd.customHeader = objectHeader;
                }
                catch (Exception e)
                {
                    return null;
                }
            }

            return cmd;
        }

        public static RemotingCommand createResponseCommand(int code, string remark)
        {
            //return createResponseCommand(code, remark, null);
            RemotingCommand cmd = new RemotingCommand();
            cmd.markResponseType();
            cmd.setCode(code);
            cmd.setRemark(remark);
            setCmdVersion(cmd);
            return cmd;
        }


        ///<exception cref="RemotingCommandException"/>
        public static RemotingCommand decode(byte[] array)
        {
            ByteBuffer byteBuffer = ByteBuffer.wrap(array);
            return decode(byteBuffer);
        }

        ///<exception cref="RemotingCommandException"/>
        public static RemotingCommand decode(ByteBuffer byteBuffer)
        {
            int length = byteBuffer.limit;
            int oriHeaderLen = byteBuffer.getInt();
            int headerLength = getHeaderLength(oriHeaderLen);

            byte[] headerData = new byte[headerLength];
            byteBuffer.get(headerData);

            RemotingCommand cmd = headerDecode(headerData, getProtocolType(oriHeaderLen));

            int bodyLength = length - 4 - headerLength;
            byte[] bodyData = null;
            if (bodyLength > 0)
            {
                bodyData = new byte[bodyLength];
                byteBuffer.get(bodyData);
            }
            cmd.body = bodyData;

            return cmd;
        }

        public static int getHeaderLength(int length)
        {
            return length & 0xFFFFFF;
        }

        ///<exception cref="RemotingCommandException"/>
        private static RemotingCommand headerDecode(byte[] headerData, SerializeType type)
        {
            switch (type)
            {
                case SerializeType.JSON:
                    RemotingCommand resultJson = RemotingSerializable.decode<RemotingCommand>(headerData/*, RemotingCommand.class*/);
                    resultJson.setSerializeTypeCurrentRPC(type);
                    return resultJson;
                case SerializeType.ROCKETMQ:
                    RemotingCommand resultRMQ = RocketMQSerializable.rocketMQProtocolDecode(headerData);
                    resultRMQ.setSerializeTypeCurrentRPC(type);
                    return resultRMQ;
                default:
                    break;
            }

            return null;
        }

        public static SerializeType getProtocolType(int source)
        {
            //return SerializeType.valueOf((byte)((source >> 24) & 0xFF));
            return (SerializeType)(byte)((source >> 24) & 0xFF);
        }

        public static int createNewRequestId()
        {
            return requestId.getAndIncrement();
        }

        public static SerializeType getSerializeTypeConfigInThisServer()
        {
            return serializeTypeConfigInThisServer;
        }

        private static bool isBlank(String str)
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

        public static byte[] markProtocolType(int source, SerializeType type)
        {
            byte[] result = new byte[4];

            result[0] = (byte)type;//.getCode();//???
            result[1] = (byte)((source >> 16) & 0xFF);
            result[2] = (byte)((source >> 8) & 0xFF);
            result[3] = (byte)(source & 0xFF);
            return result;
        }

        public void markResponseType()
        {
            int bits = 1 << RPC_TYPE;
            this.flag |= bits;
        }

        public CommandCustomHeader readCustomHeader()
        {
            return customHeader;
        }

        public void writeCustomHeader(CommandCustomHeader customHeader)
        {
            this.customHeader = customHeader;
        }

        ///<exception cref="RemotingCommandException"/>
        //public CommandCustomHeader decodeCommandCustomHeader(Class<? extends CommandCustomHeader> classHeader)
        public T decodeCommandCustomHeader<T>() where T : CommandCustomHeader, new ()
        {
            CommandCustomHeader objectHeader;
            try
            {
                //objectHeader = classHeader.newInstance();
                objectHeader = new T();
            }
            catch (Exception e)
            {
                return default;
            }

            if (this.extFields != null)
            {
                var fields = getClazzFields<T>();
                foreach (var field in fields)
                {
                    //if (!Modifier.isStatic(field.getModifiers())) //getClazzFields不会获取到静态属性
                    {
                        string fieldName = field.Name;
                        //if (!fieldName.StartsWith("this"))  //多余的判断
                        {
                            try
                            {
                                string value = this.extFields.Get(fieldName);  //??? int long 类型怎么判断
                                if (null == value)
                                {
                                    if (!isFieldNullable(field))
                                    {
                                        throw new RemotingCommandException("the custom field <" + fieldName + "> is null");
                                    }
                                    continue;
                                }

                                //field.setAccessible(true);  //???
                                //string type = getCanonicalName(field.getType());
                                Type type = field.GetType();
                                Object valueParsed;

                                if (type.Equals(STRING_CANONICAL_NAME))
                                {
                                    valueParsed = value;
                                }
                                else if (type.Equals(INTEGER_CANONICAL_NAME_1) || type.Equals(INTEGER_CANONICAL_NAME_2))
                                {
                                    valueParsed = int.Parse(value);
                                }
                                else if (type.Equals(LONG_CANONICAL_NAME_1) || type.Equals(LONG_CANONICAL_NAME_2))
                                {
                                    valueParsed = long.Parse(value);
                                }
                                else if (type.Equals(BOOLEAN_CANONICAL_NAME_1) || type.Equals(BOOLEAN_CANONICAL_NAME_2))
                                {
                                    valueParsed = bool.Parse(value);
                                }
                                else if (type.Equals(DOUBLE_CANONICAL_NAME_1) || type.Equals(DOUBLE_CANONICAL_NAME_2))
                                {
                                    valueParsed = double.Parse(value);
                                }
                                else
                                {
                                    throw new RemotingCommandException("the custom field <" + fieldName + "> type is not supported");
                                }

                                //field.set(objectHeader, valueParsed);
                                field.SetValue(objectHeader, valueParsed);

                            }
                            catch (Exception e)
                            {
                                log.Error("Failed field [{}] decoding", fieldName, e);
                            }
                        }
                    }
                }

                objectHeader.checkFields();
            }

            return (T)objectHeader;
        }

        private PropertyInfo[] getClazzFields(Type type)
        {
            PropertyInfo[] pros = CLASS_HASH_MAP.Get(type);
            if (pros == null)
            {
                //pros = type.getDeclaredFields();
                pros = type.GetProperties(); //只会获取Public的属性
                lock (CLASS_HASH_MAP)
                {
                    CLASS_HASH_MAP.Put(type, pros);
                }
            }
            return pros;
        }

        //private Field[] getClazzFields(Class<? extends CommandCustomHeader> classHeader)
        private PropertyInfo[] getClazzFields<T>() where T : CommandCustomHeader
        {
            Type type = typeof(T);
            return getClazzFields(type);
        }

        private bool isFieldNullable(PropertyInfo field)
        {
            if (!NULLABLE_FIELD_CACHE.ContainsKey(field))
            {
                //Annotation annotation = field.getAnnotation(CFNotNull/*.class*/);
                //synchronized(NULLABLE_FIELD_CACHE) {
                //    NULLABLE_FIELD_CACHE.put(field, annotation == null);
                //}
                var notNull = field.GetCustomAttribute<CFNotNull>();
                lock (NULLABLE_FIELD_CACHE)
                {
                    NULLABLE_FIELD_CACHE.Put(field, notNull == null);
                }
            }
            return NULLABLE_FIELD_CACHE.Get(field);
        }

        //private string getCanonicalName(Class clazz)
        private string getCanonicalName<T>()
        {
            var type = typeof(T);
            string name = CANONICAL_NAME_CACHE.Get(type);
            if (name == null)
            {
                //name = clazz.getCanonicalName();
                name = type.FullName;
                lock(CANONICAL_NAME_CACHE) 
                {
                    CANONICAL_NAME_CACHE.Put(type, name);
                }
            }
            return name;
        }

        public ByteBuffer encode()
        {
            // 1> header length size
            int length = 4;

            // 2> header data length
            byte[] headerData = this.headerEncode();
            length += headerData.Length;

            // 3> body data length
            if (this.body != null)
            {
                length += body.Length;
            }

            ByteBuffer result = ByteBuffer.allocate(4 + length);

            // length
            result.putInt(length);

            // header length
            result.put(markProtocolType(headerData.Length, serializeTypeCurrentRPC));

            // header data
            result.put(headerData);

            // body data;
            if (this.body != null)
            {
                result.put(this.body);
            }

            result.flip();

            return result;
        }

        private byte[] headerEncode()
        {
            this.makeCustomHeaderToNet();
            if (SerializeType.ROCKETMQ == serializeTypeCurrentRPC)
            {
                return RocketMQSerializable.rocketMQProtocolEncode(this);
            }
            else
            {
                return RemotingSerializable.encode(this);
            }
        }

        public void makeCustomHeaderToNet()
        {
            if (this.customHeader != null)
            {
                PropertyInfo[] fields = getClazzFields(customHeader.GetType());
                if (null == this.extFields)
                {
                    this.extFields = new HashMap<String, String>();
                }

                foreach (var field in fields)
                {
                    //if (!Modifier.isStatic(field.getModifiers()))
                    {
                        string name = field.Name;
                        if (!name.StartsWith("this"))
                        {
                            Object value = null;
                            try
                            {
                                //field.setAccessible(true); //???
                                //value = field.get(this.customHeader);
                                value = field.GetValue(this.customHeader);
                            }
                            catch (Exception e)
                            {
                                log.Error("Failed to access field [{}]", name, e);
                            }

                            if (value != null)
                            {
                                this.extFields.Put(name, value.ToString());
                            }
                        }
                    }
                }
            }
        }

        public ByteBuffer encodeHeader()
        {
            return encodeHeader(body != null ? body.Length : 0);
        }

        public ByteBuffer encodeHeader(int bodyLength)
        {
            // 1> header length size
            int length = 4;

            // 2> header data length
            byte[] headerData;
            headerData = this.headerEncode();

            length += headerData.Length;

            // 3> body data length
            length += bodyLength;

            ByteBuffer result = ByteBuffer.allocate(4 + length - bodyLength);

            // length
            result.putInt(length);

            // header length
            result.put(markProtocolType(headerData.Length, serializeTypeCurrentRPC));

            // header data
            result.put(headerData);

            result.flip();

            return result;
        }

        public void markOnewayRPC()
        {
            int bits = 1 << RPC_ONEWAY;
            this.flag |= bits;
        }

        //@JSONField(serialize = false)
        public bool isOnewayRPC()
        {
            int bits = 1 << RPC_ONEWAY;
            return (this.flag & bits) == bits;
        }

        public int getCode()
        {
            return code;
        }

        public void setCode(int code)
        {
            this.code = code;
        }

        //@JSONField(serialize = false)
        public RemotingCommandType getType()
        {
            if (this.isResponseType())
            {
                return RemotingCommandType.RESPONSE_COMMAND;
            }

            return RemotingCommandType.REQUEST_COMMAND;
        }

        //@JSONField(serialize = false)
        public bool isResponseType()
        {
            int bits = 1 << RPC_TYPE;
            return (this.flag & bits) == bits;
        }

        public LanguageCode getLanguage()
        {
            return language;
        }

        public void setLanguage(LanguageCode language)
        {
            this.language = language;
        }

        public int getVersion()
        {
            return version;
        }

        public void setVersion(int version)
        {
            this.version = version;
        }

        public int getOpaque()
        {
            return opaque;
        }

        public void setOpaque(int opaque)
        {
            this.opaque = opaque;
        }

        public int getFlag()
        {
            return flag;
        }

        public void setFlag(int flag)
        {
            this.flag = flag;
        }

        public string getRemark()
        {
            return remark;
        }

        public void setRemark(String remark)
        {
            this.remark = remark;
        }

        public byte[] getBody()
        {
            return body;
        }

        public void setBody(byte[] body)
        {
            this.body = body;
        }

        public HashMap<String, String> getExtFields()
        {
            return extFields;
        }

        public void setExtFields(HashMap<String, String> extFields)
        {
            this.extFields = extFields;
        }

        public void addExtField(String key, string value)
        {
            if (null == extFields)
            {
                extFields = new HashMap<String, String>();
            }
            extFields.Put(key, value);
        }

        //@Override
        public override string ToString()
        {
            return "RemotingCommand [code=" + code + ", language=" + language + ", version=" + version + ", opaque=" + opaque + ", flag(B)="
                + Convert.ToString(flag, 2) + ", remark=" + remark + ", extFields=" + extFields + ", serializeTypeCurrentRPC="
                + serializeTypeCurrentRPC + "]";
            // Convert.ToString(value, 2); //???
        }

        public SerializeType getSerializeTypeCurrentRPC()
        {
            return serializeTypeCurrentRPC;
        }

        public void setSerializeTypeCurrentRPC(SerializeType serializeTypeCurrentRPC)
        {
            this.serializeTypeCurrentRPC = serializeTypeCurrentRPC;
        }
    }
}
