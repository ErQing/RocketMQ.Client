namespace RocketMQ.Client
{
    public interface RemotingService
    {
        void start();

        void shutdown();

        void registerRPCHook(RPCHook rpcHook);
    }
}
