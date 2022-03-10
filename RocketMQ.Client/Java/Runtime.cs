using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace RocketMQ.Client
{
    public class Runtime
    {
        private static Runtime singleton = new Runtime();

        public static Runtime getRuntime()
        {
            return singleton;
        }

        public int availableProcessors()
        {
            throw new NotImplementedException();
        }

        internal void addShutdownHook(Thread shutDownHook)
        {
            throw new NotImplementedException();
        }

        internal void removeShutdownHook(Thread shutDownHook)
        {
            throw new NotImplementedException();
        }
    }
}
