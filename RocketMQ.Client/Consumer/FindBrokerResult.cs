using System;

namespace RocketMQ.Client
{
    public class FindBrokerResult
    {
        private readonly String brokerAddr;
        private readonly bool slave;
        private readonly int brokerVersion;

        public FindBrokerResult(String brokerAddr, bool slave)
        {
            this.brokerAddr = brokerAddr;
            this.slave = slave;
            this.brokerVersion = 0;
        }

        public FindBrokerResult(String brokerAddr, bool slave, int brokerVersion)
        {
            this.brokerAddr = brokerAddr;
            this.slave = slave;
            this.brokerVersion = brokerVersion;
        }

        public String getBrokerAddr()
        {
            return brokerAddr;
        }

        public bool isSlave()
        {
            return slave;
        }

        public int getBrokerVersion()
        {
            return brokerVersion;
        }
    }
}
