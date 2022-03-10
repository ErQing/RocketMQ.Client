
namespace RocketMQ.Client
{
    public class DeleteAccessConfigRequestHeader : CommandCustomHeader
    {
        public string accessKey { get; set; }

        public void checkFields() { }

    }
}
