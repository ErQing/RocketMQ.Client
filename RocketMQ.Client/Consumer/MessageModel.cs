namespace RocketMQ.Client
{
    public enum MessageModel
    {
        BROADCASTING,
        CLUSTERING,
        UNKNOWN  //永远放在最后
        /**
         * broadcast
         */
        //public static readonly string BROADCASTING = "BROADCASTING";
        /**
         * clustering
         */
        //public static readonly string CLUSTERING = "CLUSTERING";

        //private string modeCN;

        //public MessageModel(string modeCN)
        //{
        //    this.modeCN = modeCN;
        //}

        //public string getModeCN()
        //{
        //    return modeCN;
        //}
    }
}
