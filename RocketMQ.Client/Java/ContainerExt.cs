using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RocketMQ.Client
{
    public static class ContainerExt
    {
        public static T get<T>(this IList<T> dic, int index)
        {
            return dic[index];
        }


        //public static V get<K, V>(this IDictionary<K, V> dic, K key)
        //{
        //    dic.TryGetValue(key, out V val);
        //    return val;
        //}

        /// <summary>
        /// 如果存在则替换，并返回原来的值
        /// 如果不存在添加，并返回null
        /// </summary>
        /// <typeparam name="K"></typeparam>
        /// <typeparam name="V"></typeparam>
        /// <param name="dic"></param>
        /// <returns></returns>
        public static V put<K, V>(this IDictionary<K, V> dic, K key, V val)
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
        public static V putIfAbsent<K, V>(this IDictionary<K, V> dic, K key, V val)
        {
            if (!dic.ContainsKey(key))
                return dic.put(key, val);
            else
                return dic.get(key);
        }

        public static V get<K, V>(this IDictionary<K, V> dic, K key)
        {
            dic.TryGetValue(key, out V val);
            return val;
        }

        public static V remove<K, V>(this IDictionary<K, V> dic, K key)
        {
            dic.Remove(key, out V val);
            return val;
        }

        public static V remove<K, V>(this ConcurrentDictionary<K, V> dic, K key)
        {
            dic.TryRemove(key, out V val);
            return val;
        }


        public static KeyValuePair<K,V> pollFirstEntry<K, V>(this SortedDictionary<K, V> dic)
        {
            if (!dic.isEmpty())
            {
                var first = dic.First();
                dic.Remove(first.Key);
                return first;
            }
            return default;
        }

        public static bool addAll<T>(this HashSet<T> source, IEnumerable<T> items)
        {
            bool allAdded = true;
            foreach (T item in items)
            {
                allAdded &= source.Add(item);
            }
            return allAdded;
        }

        public static bool addAll<T>(this ISet<T> source, IEnumerable<T> items)
        {
            bool allAdded = true;
            foreach (T item in items)
            {
                allAdded &= source.Add(item);
            }
            return allAdded;
        }


        public static void addAll<T>(this List<T> source, IEnumerable<T> items)
        {
            source.AddRange(items);
        }


        public static void shuffle<T>(this ICollection<T> col)
        {
            var rnd = new Random();
            col.OrderBy(item => rnd.Next());
        }

        public static bool isEmpty<T>(this ICollection<T> col)
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

        //public static void putAll<T, S>(this Dictionary<T, S> source, Dictionary<T, S> collection)
        //{
        //    if (collection == null)
        //        return;
        //    foreach (var item in collection)
        //    {
        //        source[item.Key] = item.Value;
        //    }
        //}

        //public static void putAll<T, S>(this ConcurrentDictionary<T, S> source, ConcurrentDictionary<T, S> collection)
        //{
        //    if (collection == null)
        //        return;
        //    foreach (var item in collection)
        //    {
        //        source[item.Key] = item.Value;
        //    }
        //}

        public static void putAll<T, S>(this IDictionary<T, S> source, IDictionary<T, S> collection)
        {
            if (collection == null)
                return;
            foreach (var item in collection)
            {
                source[item.Key] = item.Value;
            }
        }


        public static T getFirst<T>(this ICollection<T> collection)
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
