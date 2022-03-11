using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RocketMQ.Client
{
    public class TopAddressing
    {
        //private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.COMMON_LOGGER_NAME);
        static NLog.Logger log = NLog.LogManager.GetCurrentClassLogger();

        private string nsAddr;
        private string wsAddr;
        private string unitName;

        public TopAddressing(String wsAddr) : this(wsAddr, null)
        {
            
        }

        public TopAddressing(String wsAddr, string unitName)
        {
            this.wsAddr = wsAddr;
            this.unitName = unitName;
        }

        private static string clearNewLine(String str)
        {
            string newString = str.Trim();
            int index = newString.IndexOf("\r");
            if (index != -1)
            {
                return newString.JavaSubstring(0, index); //???
            }

            index = newString.IndexOf("\n");
            if (index != -1)
            {
                return newString.JavaSubstring(0, index); //???
            }

            return newString;
        }

        public string fetchNSAddr()
        {
            return fetchNSAddr(true, 3000);
        }

        public string fetchNSAddr(bool verbose, long timeoutMills)
        {
            string url = this.wsAddr;
            try
            {
                if (!UtilAll.isBlank(this.unitName))
                {
                    url = url + "-" + this.unitName + "?nofix=1";
                }
                HttpTinyClient.HttpResult result = HttpTinyClient.httpGet(url, null, null, "UTF-8", timeoutMills);
                if (200 == result.code)
                {
                    string responseStr = result.content;
                    if (responseStr != null)
                    {
                        return clearNewLine(responseStr);
                    }
                    else
                    {
                        log.Error("fetch nameserver address is null");
                    }
                }
                else
                {
                    log.Error("fetch nameserver address failed. statusCode=" + result.code);
                }
            }
            catch (IOException e)
            {
                if (verbose)
                {
                    log.Error("fetch name server address exception", e);
                }
            }

            if (verbose)
            {
                string errorMsg = "connect to " + url + " failed, maybe the domain name " + MixAll.getWSAddr() + " not bind in /etc/hosts";
                errorMsg += FAQUrl.suggestTodo(FAQUrl.NAME_SERVER_ADDR_NOT_EXIST_URL);

                log.Warn(errorMsg);
            }
            return null;
        }

        public string getNsAddr()
        {
            return nsAddr;
        }

        public void setNsAddr(String nsAddr)
        {
            this.nsAddr = nsAddr;
        }
    }
}
