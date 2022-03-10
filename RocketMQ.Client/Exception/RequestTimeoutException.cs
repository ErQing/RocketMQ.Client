using System;

namespace RocketMQ.Client
{
    public class RequestTimeoutException : Exception
    {
        //private static final long serialVersionUID = -5758410930844185841L;
        private int responseCode;
        private String errorMessage;

        public RequestTimeoutException(String errorMessage, Exception cause) 
            : base(errorMessage, cause)
        {
            this.responseCode = -1;
            this.errorMessage = errorMessage;
        }

        public RequestTimeoutException(int responseCode, String errorMessage)
            :base("CODE: " + UtilAll.responseCode2String(responseCode) + "  DESC: " + errorMessage)
        {
            
            this.responseCode = responseCode;
            this.errorMessage = errorMessage;
        }

        public int getResponseCode()
        {
            return responseCode;
        }

        public RequestTimeoutException setResponseCode(int responseCode)
        {
            this.responseCode = responseCode;
            return this;
        }

        public String getErrorMessage()
        {
            return errorMessage;
        }

        public void setErrorMessage(String errorMessage)
        {
            this.errorMessage = errorMessage;
        }
    }
}
