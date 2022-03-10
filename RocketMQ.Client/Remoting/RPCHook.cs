namespace RocketMQ.Client
{
    public interface RPCHook
    {
        void doBeforeRequest(string remoteAddr, RemotingCommand request);

        void doAfterResponse(string remoteAddr, RemotingCommand request, RemotingCommand response);
    }
}
