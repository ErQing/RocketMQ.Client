using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RocketMQ.Client
{
    public interface RemotingClient : RemotingService
    {
        void updateNameServerAddressList(List<string> addrs);

        List<string> getNameServerAddressList();

        ///<exception cref="RemotingConnectException"/>
        ///<exception cref="RemotingTimeoutException"/>
        ///<exception cref="RemotingSendRequestException"/>
        ///<exception cref="RemotingConnectException"/>
        ///<exception cref="ThreadInterruptedException"/>
        RemotingCommand invokeSync(string addr, RemotingCommand request, long timeoutMillis) ;

        ///<exception cref="RemotingConnectException"/>
        ///<exception cref="RemotingTimeoutException"/>
        ///<exception cref="RemotingSendRequestException"/>
        ///<exception cref="RemotingConnectException"/>
        ///<exception cref="ThreadInterruptedException"/>
        ///<exception cref="RemotingTooMuchRequestException"/>
        Task invokeAsync(string addr, RemotingCommand request, long timeoutMillis, InvokeCallback invokeCallback) ;

        ///<exception cref="RemotingConnectException"/>
        ///<exception cref="RemotingTimeoutException"/>
        ///<exception cref="RemotingSendRequestException"/>
        ///<exception cref="RemotingConnectException"/>
        ///<exception cref="ThreadInterruptedException"/>
        ///<exception cref="RemotingTooMuchRequestException"/>
        Task invokeOneway(string addr, RemotingCommand request, long timeoutMillis);

        void registerProcessor(int requestCode, NettyRequestProcessor processor,
            ExecutorService executor);

        void setCallbackExecutor(ExecutorService callbackExecutor);

        ExecutorService getCallbackExecutor();

        bool isChannelWritable(string addr);
    }
}
