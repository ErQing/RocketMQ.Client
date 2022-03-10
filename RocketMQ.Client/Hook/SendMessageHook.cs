using System;

namespace RocketMQ.Client
{
    public interface SendMessageHook
    {
        String hookName();

        void sendMessageBefore(SendMessageContext context);

        void sendMessageAfter(SendMessageContext context);
    }
}
