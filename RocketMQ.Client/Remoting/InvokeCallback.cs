using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RocketMQ.Client
{
    //public interface InvokeCallback
    //{
    //    void operationComplete(ResponseFuture responseFuture);
    //}

    public class InvokeCallback
    {
        public Action<ResponseFuture> OperationComplete { get; set; }
    }
}
