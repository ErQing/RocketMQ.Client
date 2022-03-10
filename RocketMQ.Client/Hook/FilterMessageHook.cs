using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RocketMQ.Client
{
    public interface FilterMessageHook
    {
        string hookName();
        void filterMessage(FilterMessageContext context);
    }
}
