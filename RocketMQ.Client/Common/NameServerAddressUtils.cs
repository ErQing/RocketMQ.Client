using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace RocketMQ.Client
{
    public class NameServerAddressUtils
    {
        public static readonly string INSTANCE_PREFIX = "MQ_INST_";
        public static readonly string INSTANCE_REGEX = INSTANCE_PREFIX + "\\w+_\\w+";
        public static readonly string ENDPOINT_PREFIX = "(\\w+://|)";
        //public static readonly Pattern NAMESRV_ENDPOINT_PATTERN = Pattern.compile("^http://.*");
        //public static readonly Pattern INST_ENDPOINT_PATTERN = Pattern.compile("^" + ENDPOINT_PREFIX + INSTANCE_REGEX + "\\..*");
        public static readonly Regex NAMESRV_ENDPOINT_PATTERN = new Regex("^http://.*");
        public static readonly Regex INST_ENDPOINT_PATTERN = new Regex("^" + ENDPOINT_PREFIX + INSTANCE_REGEX + "\\..*");

        public static string getNameServerAddresses()
        {
            //System.Environment.SetEnvironmentVariable("Test1", "Value1");
            return Sys.getProperty(MixAll.NAMESRV_ADDR_PROPERTY, Sys.getenv(MixAll.NAMESRV_ADDR_ENV));
        }

        public static bool validateInstanceEndpoint(String endpoint)
        {
            return INST_ENDPOINT_PATTERN.Match(endpoint).Success;
            //return INST_ENDPOINT_PATTERN.matcher(endpoint).matches();
        }

        public static string parseInstanceIdFromEndpoint(String endpoint)
        {
            if (string.IsNullOrEmpty(endpoint))
            {
                return null;
            }
            //String java.lang.String.substring(int beginIndex, int endIndex)
            return endpoint.JavaSubstring(endpoint.LastIndexOf("/") + 1, endpoint.IndexOf('.'));
        }

        public static string getNameSrvAddrFromNamesrvEndpoint(String nameSrvEndpoint)
        {
            if (string.IsNullOrEmpty(nameSrvEndpoint))
            {
                return null;
            }
            return nameSrvEndpoint.Substring(nameSrvEndpoint.LastIndexOf('/') + 1);
        }
    }
}
