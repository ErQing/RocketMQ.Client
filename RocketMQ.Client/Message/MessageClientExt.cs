using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RocketMQ.Client
{
    public class MessageClientExt : MessageExt
    {

        public string getOffsetMsgId()
        {
            return base.getMsgId();
        }

        public void setOffsetMsgId(String offsetMsgId)
        {
            base.setMsgId(offsetMsgId);
        }

        public override string getMsgId()
        {
            string uniqID = MessageClientIDSetter.getUniqID(this);
            if (uniqID == null)
            {
                return this.getOffsetMsgId();
            }
            else
            {
                return uniqID;
            }
        }

        public new void setMsgId(String msgId) //???
        {
            //DO NOTHING
            //MessageClientIDSetter.setUniqID(this);
        }

    }
}
