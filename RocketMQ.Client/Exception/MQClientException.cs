using System;

namespace RocketMQ.Client
{
    public class MQClientException : Exception
    {
        private int responseCode;
        private String errorMessage;

        public MQClientException(String errorMessage, Exception cause) 
            : base(FAQUrl.attachDefaultURL(errorMessage), cause)
        {
            this.responseCode = -1;
            this.errorMessage = errorMessage;
        }

        public MQClientException(int responseCode, String errorMessage)
            : base(FAQUrl.attachDefaultURL("CODE: " + responseCode + "  DESC: " + errorMessage))
        {
            this.responseCode = responseCode;
            this.errorMessage = errorMessage;
        }

        public int getResponseCode()
        {
            return responseCode;
        }

        public MQClientException setResponseCode(int responseCode)
        {
            this.responseCode = responseCode;
            return this;
        }

        public String getErrorMessage()
        {
            return errorMessage;
        }

        public void setErrorMessage(string errorMessage)
        {
            this.errorMessage = errorMessage;
        }
    }
}
