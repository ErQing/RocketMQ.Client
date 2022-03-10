namespace RocketMQ.Client
{
    public class CreateAccessConfigRequestHeader : CommandCustomHeader
    {
        [CFNotNull]
        public string accessKey { get; set; }

        public string secretKey { get; set; }

        public string whiteRemoteAddress { get; set; }

        public bool admin { get; set; }

        public string defaultTopicPerm { get; set; }

        public string defaultGroupPerm { get; set; }

        // list string,eg: topicA=DENY,topicD=SUB
        public string topicPerms { get; set; }

        // list string,eg: groupD=DENY,groupD=SUB
        public string groupPerms { get; set; }

        public void checkFields() { }
    }
}
