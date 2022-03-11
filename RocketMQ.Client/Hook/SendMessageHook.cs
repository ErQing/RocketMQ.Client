using System;

namespace RocketMQ.Client
{
    public interface SendMessageHook
    {
        string hookName();

        void sendMessageBefore(SendMessageContext context);

        void sendMessageAfter(SendMessageContext context);
    }
}
