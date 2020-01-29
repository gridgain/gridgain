/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

namespace Apache.Ignite.Core.Impl.Cache.Near
{
    using System.Collections.Concurrent;
    using System.Diagnostics;
    using Apache.Ignite.Core.Impl.Binary;
    using Apache.Ignite.Core.Impl.Binary.IO;
    using Apache.Ignite.Core.Impl.Memory;

    /// <summary>
    /// Holds near cache data for a given cache, serves one or more <see cref="CacheImpl{TK,TV}"/> instances.
    /// </summary>
    internal sealed class NearCache<TK, TV> : INearCache
    {
        /** Generic map, used by default, should fit most use cases. */
        private volatile ConcurrentDictionary<TK, TV> _map = 
            new ConcurrentDictionary<TK, TV>();

        /** Non-generic map. Switched to when same cache is used with different generic arguments.
         * Less efficient because of boxing and casting. */
        private volatile ConcurrentDictionary<object, object> _fallbackMap;
        
        public bool TryGetValue<TKey, TVal>(TKey key, out TVal val)
        {
            // ReSharper disable once SuspiciousTypeConversion.Global (reviewed)
            var map = _map as ConcurrentDictionary<TKey, TVal>;
            if (map != null)
            {
                if (map.TryGetValue(key, out val))
                {
                    return true;
                }
            }
            
            if (_fallbackMap != null)
            {
                object fallbackEntry;
                if (_fallbackMap.TryGetValue(key, out fallbackEntry))
                {
                    val = (TVal) fallbackEntry;
                    return true;
                }
            }

            val = default(TVal);
            return false;
        }

        public void Update(IBinaryStream stream, Marshaller marshaller)
        {
            Debug.Assert(stream != null);
            Debug.Assert(marshaller != null);

            var reader = marshaller.StartUnmarshal(stream);

            var key = reader.ReadObject<object>();
            var hasVal = reader.ReadBoolean();
            var val = hasVal ? reader.ReadObject<object>() : null;
            var typeMatch = key is TK && (!hasVal || val is TV);

            var map = _map;
            if (map != null && typeMatch)
            {
                if (hasVal)
                {
                    map[(TK) key] = (TV) val;
                }
                else
                {
                    TV unused;
                    map.TryRemove((TK) key, out unused);
                }
            }

            if (!typeMatch)
            {
                // Type mismatch: must switch to fallback map and update it.
                _map = null;
                _fallbackMap = _fallbackMap ?? new ConcurrentDictionary<object, object>();
            }
            else if (_fallbackMap == null)
            {
                // Type match and no fallback map: exit.
                return;
            }
            
            if (hasVal)
            {
                _fallbackMap[key] = val;
            }
            else
            {
                object unused;
                _fallbackMap.TryRemove(key, out unused);
            }
        }

        public void Evict(PlatformMemoryStream stream, Marshaller marshaller)
        {
            // Eviction callbacks from Java work for 2 out of 3 cases:
            // + Client node (all keys)
            // + Server node (non-primary keys)
            // - Server node (primary keys) - because there is no need to store primary keys in near cache

            Debug.Assert(stream != null);
            Debug.Assert(marshaller != null);
            
            var reader = marshaller.StartUnmarshal(stream);
            var key = reader.ReadObject<object>();

            var map = _map;
            if (map != null && key is TK)
            {
                TV unused;
                map.TryRemove((TK) key, out unused);
            }

            if (_fallbackMap != null)
            {
                object unused;
                _fallbackMap.TryRemove(key, out unused);
            }
        }

        public void Clear()
        {
            if (_fallbackMap != null)
            {
                _fallbackMap.Clear();
            }
            else
            {
                var map = _map;
                if (map != null)
                {
                    map.Clear();
                }
            }
        }
    }
}
