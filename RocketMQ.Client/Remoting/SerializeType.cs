using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RocketMQ.Client
{
    public enum SerializeType
    {
        UNKNOW = -1,
        JSON = 0,
        ROCKETMQ = 1
        //    JSON((byte) 0),
        //    ROCKETMQ((byte) 1);

        //private byte code;

        //SerializeType(byte code)
        //{
        //    this.code = code;
        //}

        //public static SerializeType valueOf(byte code)
        //{
        //    for (SerializeType serializeType : SerializeType.values())
        //    {
        //        if (serializeType.getCode() == code)
        //        {
        //            return serializeType;
        //        }
        //    }
        //    return null;
        //}

        //public byte getCode()
        //{
        //    return code;
        //}
    }
}
