using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RocketMQ.Client
{
    [Obsolete]
    public interface TransactionCheckListener
    {
        LocalTransactionState checkLocalTransactionState(MessageExt msg);
    }
}
