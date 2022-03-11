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
        public string globalWhiteAddrs { get; set; }

        public void checkFields()
        {

        }
    }
}
