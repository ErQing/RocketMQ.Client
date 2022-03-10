using DotNetty.Handlers.Tls;
using DotNetty.Transport.Channels;
using System.Net.Security;
using System.Security.Cryptography.X509Certificates;

namespace RocketMQ.Client
{
    public class SslContext
    {
        public IChannelHandler newHandler()
        {
            var cert = new X509Certificate2("dotnetty.com.pfx", "password");
            string targetHost = cert.GetNameInfo(X509NameType.DnsName, false);
            return new TlsHandler(stream => new SslStream(stream, true, (sender, certificate, chain, errors) => true), new ClientTlsSettings(targetHost));
        }
    }
}
