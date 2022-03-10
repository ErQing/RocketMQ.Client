using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RocketMQ.Client
{
    public enum CMResult
    {
        CR_SUCCESS,
        CR_LATER,
        CR_ROLLBACK,
        CR_COMMIT,
        CR_THROW_EXCEPTION,
        CR_RETURN_NULL,
    }
}
