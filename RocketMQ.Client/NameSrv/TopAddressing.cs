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

        private String nsAddr;
        private String wsAddr;
        private String unitName;

        public TopAddressing(String wsAddr) : this(wsAddr, null)
        {
            
        }

        public TopAddressing(String wsAddr, String unitName)
        {
            this.wsAddr = wsAddr;
            this.unitName = unitName;
        }

        private static String clearNewLine(String str)
        {
            String newString = str.Trim();
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

        public String fetchNSAddr()
        {
            return fetchNSAddr(true, 3000);
        }

        public String fetchNSAddr(bool verbose, long timeoutMills)
        {
            String url = this.wsAddr;
            try
            {
                if (!UtilAll.isBlank(this.unitName))
                {
                    url = url + "-" + this.unitName + "?nofix=1";
                }
                HttpTinyClient.HttpResult result = HttpTinyClient.httpGet(url, null, null, "UTF-8", timeoutMills);
                if (200 == result.code)
                {
                    String responseStr = result.content;
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
                String errorMsg = "connect to " + url + " failed, maybe the domain name " + MixAll.getWSAddr() + " not bind in /etc/hosts";
                errorMsg += FAQUrl.suggestTodo(FAQUrl.NAME_SERVER_ADDR_NOT_EXIST_URL);

                log.Warn(errorMsg);
            }
            return null;
        }

        public String getNsAddr()
        {
            return nsAddr;
        }

        public void setNsAddr(String nsAddr)
        {
            this.nsAddr = nsAddr;
        }
    }
}
