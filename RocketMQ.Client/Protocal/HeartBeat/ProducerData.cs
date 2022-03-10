namespace RocketMQ.Client
{
    public class ProducerData
    {
        public string groupName { get; set; }
        //@Override
        public override string ToString()
        {
            return "ProducerData [groupName=" + groupName + "]";
        }
    }
}
