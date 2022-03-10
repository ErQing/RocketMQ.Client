using DotNetty.Buffers;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RocketMQ.Client
{
    public class ByteBuffer2
    {
        private IByteBuffer buffer;


        public ByteBuffer2()
        {
            
            buffer = new UnpooledUnsafeDirectByteBuffer(null, 10, 1024);

            //buffer.GetInt
            //buffer.GetInt();
        }


    }
}
