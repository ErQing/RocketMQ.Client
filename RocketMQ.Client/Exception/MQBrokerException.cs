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
        private readonly String errorMessage;
        private readonly String brokerAddr;

        public MQBrokerException(int responseCode, String errorMessage)
                : base(FAQUrl.attachDefaultURL("CODE: " + UtilAll.responseCode2String(responseCode) + "  DESC: " + errorMessage))
        {

            this.responseCode = responseCode;
            this.errorMessage = errorMessage;
            this.brokerAddr = null;
        }

        public MQBrokerException(int responseCode, String errorMessage, String brokerAddr)
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

        public String getErrorMessage()
        {
            return errorMessage;
        }

        public String getBrokerAddr()
        {
            return brokerAddr;
        }
    }
}
