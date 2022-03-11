using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RocketMQ.Client
{
    public class HttpTinyClient
    {
        public class HttpResult
        {
            readonly public int code;
            readonly public string content;

            public HttpResult(int code, string content)
            {
                this.code = code;
                this.content = content;
            }
        }

        internal static HttpResult httpGet(String url, List<String> headers, List<String> paramValues, string encoding, long readTimeoutMs)
        {
            throw new NotImplementedException();
        }
    }
}
