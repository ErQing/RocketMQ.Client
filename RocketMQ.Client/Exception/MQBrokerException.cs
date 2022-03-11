using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RocketMQ.Client
{
    public class MQBrokerException : Exception
    {
        //private static final long serialVersionUID = 5975020272601250368L;
        private readonly int responseCode;
        private readonly string errorMessage;
        private readonly string brokerAddr;

        public MQBrokerException(int responseCode, string errorMessage)
                : base(FAQUrl.attachDefaultURL("CODE: " + UtilAll.responseCode2String(responseCode) + "  DESC: " + errorMessage))
        {

            this.responseCode = responseCode;
            this.errorMessage = errorMessage;
            this.brokerAddr = null;
        }

        public MQBrokerException(int responseCode, string errorMessage, string brokerAddr)
            : base(FAQUrl.attachDefaultURL("CODE: " + UtilAll.responseCode2String(responseCode) + "  DESC: "
                + errorMessage + (brokerAddr != null ? " BROKER: " + brokerAddr : "")))
        {
            this.responseCode = responseCode;
            this.errorMessage = errorMessage;
            this.brokerAddr = brokerAddr;
        }

        public int getResponseCode()
        {
            return responseCode;
        }

        public string getErrorMessage()
        {
            return errorMessage;
        }

        public string getBrokerAddr()
        {
            return brokerAddr;
        }
    }
}
