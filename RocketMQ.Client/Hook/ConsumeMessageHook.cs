using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RocketMQ.Client
{
    public interface ConsumeMessageHook
    {
        string hookName();
        void consumeMessageBefore(ConsumeMessageContext context);

        void consumeMessageAfter(ConsumeMessageContext context);
    }
}
