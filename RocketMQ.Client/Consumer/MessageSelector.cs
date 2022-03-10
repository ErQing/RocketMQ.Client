namespace RocketMQ.Client
{
    public class MessageSelector
    {
        /**
     * @see org.apache.rocketmq.common.filter.ExpressionType
     */
        private string type;

        /**
         * expression content.
         */
        private string expression;

        private MessageSelector(string type, string expression)
        {
            this.type = type;
            this.expression = expression;
        }

        /**
         * Use SQL92 to select message.
         *
         * @param sql if null or empty, will be treated as select all message.
         */
        public static MessageSelector bySql(string sql)
        {
            return new MessageSelector(ExpressionType.SQL92, sql);
        }

        /**
         * Use tag to select message.
         *
         * @param tag if null or empty or "*", will be treated as select all message.
         */
        public static MessageSelector byTag(string tag)
        {
            return new MessageSelector(ExpressionType.TAG, tag);
        }

        public string getExpressionType()
        {
            return type;
        }

        public string getExpression()
        {
            return expression;
        }
    }
}
