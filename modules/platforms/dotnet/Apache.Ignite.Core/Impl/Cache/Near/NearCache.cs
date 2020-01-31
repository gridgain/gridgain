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
    using System;
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
        
        /** Fallback init lock. */
        private readonly object _fallbackMapLock = new object();

        /// <summary>
        /// Initializes a new instance of the <see cref="NearCache{TK, TV}"/> class. 
        /// </summary>
        public NearCache()
        {
            // TODO: Enable callbacks in Java.
            // Callbacks should be disabled by default for all caches to avoid unnecessary overhead.
        }

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

        public TVal GetOrAdd<TKey, TVal>(TKey key, Func<TKey, TVal> valueFactory)
        {
            // ReSharper disable once SuspiciousTypeConversion.Global (reviewed)
            var map = _map as ConcurrentDictionary<TKey, TVal>;
            if (map != null)
            {
                return map.GetOrAdd(key, valueFactory);
            }
            
            EnsureFallbackMap();
            
            return (TVal) _fallbackMap.GetOrAdd(key, k => valueFactory((TKey) k));
        }

        public TVal GetOrAdd<TKey, TVal>(TKey key, TVal val)
        {
            // ReSharper disable once SuspiciousTypeConversion.Global (reviewed)
            var map = _map as ConcurrentDictionary<TKey, TVal>;
            if (map != null)
            {
                return map.GetOrAdd(key, val);
            }
            
            EnsureFallbackMap();
            
            return (TVal) _fallbackMap.GetOrAdd(key, val);
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
                EnsureFallbackMap();
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
            // Primary keys on server nodes are never evicted from .NET Near Cache. This ensures best performance
            // for co-located operations, scan query filters, and so on.

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

        /** <inheritdoc /> */
        public int GetSize()
        {
            var map = _map;
            if (map != null)
            {
                return map.Count;
            }

            if (_fallbackMap != null)
            {
                return _fallbackMap.Count;
            }

            return 0;
        }

        public bool ContainsKey<TKey>(TKey key)
        {
            var map = _map as ConcurrentDictionary<TKey, TV>;
            if (map != null)
            {
                return map.ContainsKey(key);
            }

            if (_fallbackMap != null)
            {
                return _fallbackMap.ContainsKey(key);
            }

            return false;
        }

        private void EnsureFallbackMap()
        {
            if (_fallbackMap != null)
            {
                return;
            }

            lock (_fallbackMapLock)
            {
                if (_fallbackMap != null)
                {
                    return;
                }
                
                _map = null;
                _fallbackMap = new ConcurrentDictionary<object, object>();
            }
        }
    }
}
