using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RocketMQ.Client
{
    public interface LatencyFaultTolerance<T>
    {
        void updateFaultItem(T name, long currentLatency, long notAvailableDuration);

        bool isAvailable(T name);

        void remove(T name);

        T pickOneAtLeast();
    }
}
