using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;

namespace RocketMQ.Client
{
    public static class ContainerExt
    {
        public static T Get<T>(this IList<T> dic, int index)
        {
            return dic[index];
        }

        /// <summary>
        /// 如果存在则替换，并返回原来的值
        /// 如果不存在添加，并返回null
        /// </summary>
        /// <typeparam name="K"></typeparam>
        /// <typeparam name="V"></typeparam>
        /// <param name="dic"></param>
        /// <returns></returns>
        public static V Put<K, V>(this IDictionary<K, V> dic, K key, V val)
        {
            dic.TryGetValue(key, out V orgVal);
            dic[key] = val;
            return orgVal;
        }

        /// <summary>
        ///  if (!map.containsKey(key)) 
        ///     return map.put(key, value); 
        ///  else 
        ///     return map.get(key);  
        /// </summary>
        /// <typeparam name="K"></typeparam>
        /// <typeparam name="V"></typeparam>
        /// <param name="dic"></param>
        /// <param name="key"></param>
        /// <param name="val"></param>
        /// <returns></returns>
        public static V PutIfAbsent<K, V>(this IDictionary<K, V> dic, K key, V val)
        {
            if (!dic.ContainsKey(key))
                return dic.Put(key, val);
            else
                return dic.Get(key);
        }

        public static V Get<K, V>(this IDictionary<K, V> dic, K key)
        {
            dic.TryGetValue(key, out V val);
            return val;
        }

        public static V JavaRemove<K, V>(this IDictionary<K, V> dic, K key)
        {
            dic.Remove(key, out V val);
            return val;
        }

        public static V JavaRemove<K, V>(this ConcurrentDictionary<K, V> dic, K key)
        {
            dic.TryRemove(key, out V val);
            return val;
        }


        public static KeyValuePair<K,V> PollFirstEntry<K, V>(this SortedDictionary<K, V> dic)
        {
            if (!dic.IsEmpty())
            {
                var first = dic.First();
                dic.Remove(first.Key);
                return first;
            }
            return default;
        }

        public static bool AddAll<T>(this ISet<T> source, IEnumerable<T> items)
        {
            bool allAdded = true;
            foreach (T item in items)
            {
                allAdded &= source.Add(item);
            }
            return allAdded;
        }

        public static List<T> Shuffle<T>(this List<T> col)
        {
            var rnd = new Random();
            return (List<T>)col.OrderBy(item => rnd.Next());
        }

        public static bool IsEmpty<T>(this ICollection<T> col)
        {
            if (col == null)
                return true;
            return col.Count <= 0;
        }

        public static List<T> Empty<T>()
        {
            //Array.Empty<MessageExt>().ToList();
            return new List<T>();
        }

        public static void PutAll<T, S>(this IDictionary<T, S> source, IDictionary<T, S> collection)
        {
            if (collection == null)
                return;
            foreach (var item in collection)
            {
                source[item.Key] = item.Value;
            }
        }


        public static T GetFirst<T>(this ICollection<T> collection)
        {
            if (collection == null && collection.Count > 0)
                return default;
            if (collection is List<T> list)
            {
                return list[0];
            }
            else if (collection is LinkedList<T> linkList)
            {
                return linkList.First();
            }
            else
            {
                throw new NotSupportedException();
            }
        }

    }
}
