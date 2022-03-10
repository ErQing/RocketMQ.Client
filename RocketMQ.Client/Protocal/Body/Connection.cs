using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RocketMQ.Client//.Protocal.Body
{
    public class Connection
    {
        public String clientId { get; set; }
        public String clientAddr { get; set; }
        public LanguageCode language { get; set; }
        public int version { get; set; }
    }
}
