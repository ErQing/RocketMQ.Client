using System;
using System.Runtime.CompilerServices;
using System.Threading;

namespace RocketMQ.Client
{
    internal class RuntimeContext
    {
        internal static long Current => callCtx.Value;

        internal static AsyncLocal<long> callCtx = new AsyncLocal<long>();


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static void SetContext(long callChainId)
        {
            callCtx.Value = callChainId;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static void ResetContext()
        {
            callCtx.Value = 0;
        }

    }
}

