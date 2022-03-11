using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RocketMQ.Client
{
    public interface CheckForbiddenHook
    {
        string hookName();

        ///<exception cref="MQClientException"/>
        void checkForbidden(CheckForbiddenContext context);
    }
}
