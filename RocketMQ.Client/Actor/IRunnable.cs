using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RocketMQ.Client
{
    public interface IRunnable
    {
        void run();
    }

    public class Runnable
    {
        public Action Run { get; set; }
    }


    public interface IRunnableAsync
    {
        Task Run();
    }

}
