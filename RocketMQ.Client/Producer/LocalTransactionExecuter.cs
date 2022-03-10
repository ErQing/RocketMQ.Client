using System;

namespace RocketMQ.Client
{
    [Obsolete]
    public interface LocalTransactionExecuter
    {
        LocalTransactionState executeLocalTransactionBranch(Message msg, object arg);
    }
}
