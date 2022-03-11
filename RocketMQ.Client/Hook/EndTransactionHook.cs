using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RocketMQ.Client
{
    public interface EndTransactionHook
    {
        string hookName();

        void endTransaction(EndTransactionContext context);
    }
}
