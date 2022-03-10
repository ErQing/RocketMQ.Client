using System;

namespace RocketMQ.Client
{
    public class SendMessageRequestHeaderV2 : CommandCustomHeader
    {
        [CFNotNull]
        public String a{ get; set; } // producerGroup;
                          [CFNotNull]
        public String b{ get; set; } // topic;
                          [CFNotNull]
        public String c{ get; set; } // defaultTopic;
                          [CFNotNull]
        public int d{ get; set; } // defaultTopicQueueNums;
                       [CFNotNull]
        public int e{ get; set; } // queueId;
                       [CFNotNull]
        public int f{ get; set; } // sysFlag;
                       [CFNotNull]
        public long g{ get; set; } // bornTimestamp;
                        [CFNotNull]
        public int h{ get; set; } // flag;
                       [CFNullable]
        public String i{ get; set; } // properties;
                          [CFNullable]
        public int j{ get; set; } // reconsumeTimes;
                       [CFNullable]
        public bool k{ get; set; } // unitMode = false;

        public int l{ get; set; } // consumeRetryTimes

        [CFNullable]
        public bool m{ get; set; } //batch

        public static SendMessageRequestHeader createSendMessageRequestHeaderV1(SendMessageRequestHeaderV2 v2)
        {
            SendMessageRequestHeader v1 = new SendMessageRequestHeader();
            v1.producerGroup = (v2.a);
            v1.topic = (v2.b);
            v1.defaultTopic = (v2.c);
            v1.defaultTopicQueueNums = (v2.d);
            v1.queueId = (v2.e);
            v1.sysFlag = (v2.f);
            v1.bornTimestamp = (v2.g);
            v1.flag = (v2.h);
            v1.properties = (v2.i);
            v1.reconsumeTimes = (v2.j);
            v1.unitMode = (v2.k);
            v1.maxReconsumeTimes = (v2.l);
            v1.batch = (v2.m);
            return v1;
        }

        public static SendMessageRequestHeaderV2 createSendMessageRequestHeaderV2(SendMessageRequestHeader v1)
        {
            SendMessageRequestHeaderV2 v2 = new SendMessageRequestHeaderV2();
            v2.a = v1.producerGroup;
            v2.b = v1.topic;
            v2.c = v1.defaultTopic;
            v2.d = v1.defaultTopicQueueNums;
            v2.e = v1.queueId;
            v2.f = v1.sysFlag;
            v2.g = v1.bornTimestamp;
            v2.h = v1.flag;
            v2.i = v1.properties;
            v2.j = v1.reconsumeTimes;
            v2.k = v1.unitMode;
            v2.l = v1.maxReconsumeTimes;
            v2.m = v1.batch;
            return v2;
        }

        public void checkFields()
        {
        }       
    }
}
