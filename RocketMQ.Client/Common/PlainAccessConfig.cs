using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RocketMQ.Client
{
    public class PlainAccessConfig
    {
        private string accessKey;

        private string secretKey;

        private string whiteRemoteAddress;

        private bool admin;

        private string defaultTopicPerm;

        private string defaultGroupPerm;

        private List<String> topicPerms;

        private List<String> groupPerms;

        public string getAccessKey()
        {
            return accessKey;
        }

        public void setAccessKey(String accessKey)
        {
            this.accessKey = accessKey;
        }

        public string getSecretKey()
        {
            return secretKey;
        }

        public void setSecretKey(String secretKey)
        {
            this.secretKey = secretKey;
        }

        public string getWhiteRemoteAddress()
        {
            return whiteRemoteAddress;
        }

        public void setWhiteRemoteAddress(String whiteRemoteAddress)
        {
            this.whiteRemoteAddress = whiteRemoteAddress;
        }

        public bool isAdmin()
        {
            return admin;
        }

        public void setAdmin(bool admin)
        {
            this.admin = admin;
        }

        public string getDefaultTopicPerm()
        {
            return defaultTopicPerm;
        }

        public void setDefaultTopicPerm(String defaultTopicPerm)
        {
            this.defaultTopicPerm = defaultTopicPerm;
        }

        public string getDefaultGroupPerm()
        {
            return defaultGroupPerm;
        }

        public void setDefaultGroupPerm(String defaultGroupPerm)
        {
            this.defaultGroupPerm = defaultGroupPerm;
        }

        public List<String> getTopicPerms()
        {
            return topicPerms;
        }

        public void setTopicPerms(List<String> topicPerms)
        {
            this.topicPerms = topicPerms;
        }

        public List<String> getGroupPerms()
        {
            return groupPerms;
        }

        public void setGroupPerms(List<String> groupPerms)
        {
            this.groupPerms = groupPerms;
        }
    }
}
