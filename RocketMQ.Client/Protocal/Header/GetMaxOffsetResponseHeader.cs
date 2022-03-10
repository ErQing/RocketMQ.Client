using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RocketMQ.Client
{
    public class GetMaxOffsetResponseHeader : CommandCustomHeader
    {
        [CFNotNull]
        public long offset { get; set; }

        //@Override
        public void checkFields()
        {
        }

    }
}
