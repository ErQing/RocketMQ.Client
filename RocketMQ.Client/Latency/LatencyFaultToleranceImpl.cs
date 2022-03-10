using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;

namespace RocketMQ.Client
{
    public class LatencyFaultToleranceImpl : LatencyFaultTolerance<string>
    {
        private readonly ConcurrentDictionary<String, FaultItem> faultItemTable = new ConcurrentDictionary<String, FaultItem>();

        private readonly ThreadLocalIndex whichItemWorst = new ThreadLocalIndex();

        public void updateFaultItem(String name, long currentLatency, long notAvailableDuration)
        {
            //FaultItem old = this.faultItemTable.get(name);
            faultItemTable.TryGetValue(name, out FaultItem old);
            if (null == old)
            {
                FaultItem faultItem = new FaultItem(name);
                faultItem.SetCurrentLatency(currentLatency);
                faultItem.SetStartTimestamp(Sys.currentTimeMillis() + notAvailableDuration);

                //old = this.faultItemTable.putIfAbsent(name, faultItem);
                var flag = this.faultItemTable.TryAdd(name, faultItem);
                if (flag)
                {
                    old.SetCurrentLatency(currentLatency);
                    old.SetStartTimestamp(Sys.currentTimeMillis() + notAvailableDuration);
                }
            }
            else
            {
                old.SetCurrentLatency(currentLatency);
                old.SetStartTimestamp(Sys.currentTimeMillis() + notAvailableDuration);
            }
        }

        public bool isAvailable(String name)
        {
            //FaultItem faultItem = this.faultItemTable.get(name);
            faultItemTable.TryGetValue(name, out FaultItem faultItem);
            if (faultItem != null)
            {
                return faultItem.isAvailable();
            }
            return true;
        }

        public void remove(String name)
        {
            faultItemTable.TryRemove(name, out _);
            //this.faultItemTable.remove(name);
        }

        public String pickOneAtLeast()
        {
            //Enumeration<FaultItem> elements = this.faultItemTable.elements();
            //List<FaultItem> tmpList = new LinkedList<FaultItem>();
            //while (elements.hasMoreElements())
            //{
            //    FaultItem faultItem = elements.nextElement();
            //    tmpList.add(faultItem);
            //}
            List<FaultItem> tmpList = faultItemTable.Values.ToList();
            if (tmpList.Count > 0)
            {
                //Collections.shuffle(tmpList);
                //Collections.sort(tmpList);
                tmpList.shuffle();
                tmpList.Sort();

                int half = tmpList.Count / 2;
                if (half <= 0)
                {
                    return tmpList[0].getName();
                }
                else
                {
                    int i = this.whichItemWorst.incrementAndGet() % half;
                    return tmpList[i].getName();
                }
            }

            return null;
        }

        public override String ToString()
        {
            return "LatencyFaultToleranceImpl{" +
                "faultItemTable=" + faultItemTable +
                ", whichItemWorst=" + whichItemWorst +
                '}';
        }

        class FaultItem : IComparable<FaultItem>
        {
            private readonly String name;
            private /*volatile*/ ulong currentLatency;   //不要直接使用此属性读写
            public ulong CurrentLatency
            {
                get { return Volatile.Read(ref currentLatency); }
            }
            public void SetCurrentLatency(long val)
            {
                Volatile.Write(ref currentLatency, (ulong)val);
            }

            private /*volatile*/ ulong startTimestamp; //不要直接使用此属性读写
            public ulong StartTimestamp
            {
                get{ return Volatile.Read(ref startTimestamp); }
            }
            public void SetStartTimestamp(long val)
            {
                Volatile.Write(ref startTimestamp, (ulong)val);
            }

            public FaultItem(String name)
            {
                this.name = name;
            }

            public int CompareTo(FaultItem other)
            {
                if (this.isAvailable() != other.isAvailable())
                {
                    if (this.isAvailable())
                        return -1;

                    if (other.isAvailable())
                        return 1;
                }

                if (this.CurrentLatency < other.CurrentLatency)
                    return -1;
                else if (this.CurrentLatency > other.CurrentLatency)
                {
                    return 1;
                }

                if (this.startTimestamp < other.startTimestamp)
                    return -1;
                else if (this.startTimestamp > other.startTimestamp)
                {
                    return 1;
                }

                return 0;
            }

            public bool isAvailable()
            {
                return (Sys.currentTimeMillisUnsigned() - startTimestamp) >= 0;
            }

            public override int GetHashCode()
            {
                int result = getName() != null ? getName().GetHashCode() : 0;
                result = 31 * result + (int)(CurrentLatency ^ (CurrentLatency >> 32));  //ulong
                result = 31 * result + (int)(StartTimestamp ^ (StartTimestamp >> 32)); //ulong
                return result;
            }

            public override bool Equals(Object o)
            {
                if (this == o)
                    return true;
                if (!(o is FaultItem))
                    return false;

                FaultItem faultItem = (FaultItem)o;

                if (CurrentLatency != faultItem.CurrentLatency)
                    return false;
                if (StartTimestamp != faultItem.StartTimestamp)
                    return false;
                return getName() != null ? getName().Equals(faultItem.getName()) : faultItem.getName() == null;

            }

            public override String ToString()
            {
                return "FaultItem{" +
                    "name='" + name + '\'' +
                    ", currentLatency=" + CurrentLatency +
                    ", startTimestamp=" + StartTimestamp +
                    '}';
            }

            public String getName()
            {
                return name;
            }
        }
    }
}
