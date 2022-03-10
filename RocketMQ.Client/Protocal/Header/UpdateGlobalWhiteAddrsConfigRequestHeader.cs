using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RocketMQ.Client
{
    public class UpdateGlobalWhiteAddrsConfigRequestHeader : CommandCustomHeader
    {
        [CFNotNull]
        public String globalWhiteAddrs { get; set; }

        public void checkFields()
        {

        }
    }
}
