using System;
using System.Collections.Generic;
using System.Text;

namespace RocketMQ.Client
{
    public class MessageSysFlag
    {
        public readonly static int COMPRESSED_FLAG = 0x1;
        public readonly static int MULTI_TAGS_FLAG = 0x1 << 1;
        public readonly static int TRANSACTION_NOT_TYPE = 0;
        public readonly static int TRANSACTION_PREPARED_TYPE = 0x1 << 2;
        public readonly static int TRANSACTION_COMMIT_TYPE = 0x2 << 2;
        public readonly static int TRANSACTION_ROLLBACK_TYPE = 0x3 << 2;
        public readonly static int BORNHOST_V6_FLAG = 0x1 << 4;
        public readonly static int STOREHOSTADDRESS_V6_FLAG = 0x1 << 5;

        public static int getTransactionValue(int flag)
        {
            return flag & TRANSACTION_ROLLBACK_TYPE;
        }

        public static int resetTransactionValue(int flag, int type)
        {
            return (flag & (~TRANSACTION_ROLLBACK_TYPE)) | type;
        }

        public static int clearCompressedFlag(int flag)
        {
            return flag & (~COMPRESSED_FLAG);
        }
    }
}
