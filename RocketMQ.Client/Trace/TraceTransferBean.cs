using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RocketMQ.Client
{
    public class TraceTransferBean
    {
        private String transData;
        private HashSet<String> transKey = new HashSet<String>();

        public String getTransData()
        {
            return transData;
        }

        public void setTransData(String transData)
        {
            this.transData = transData;
        }

        public HashSet<String> getTransKey()
        {
            return transKey;
        }

        public void setTransKey(HashSet<String> transKey)
        {
            this.transKey = transKey;
        }
    }
}
