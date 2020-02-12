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
    using Apache.Ignite.Core.Cache.Affinity;
    using Apache.Ignite.Core.Impl.Binary;
    using Apache.Ignite.Core.Impl.Binary.IO;

    /// <summary>
    /// Holds near cache data for a given cache, serves one or more <see cref="CacheImpl{TK,TV}"/> instances.
    /// </summary>
    internal sealed class NearCache<TK, TV> : INearCache
    {
        /** Indicates unknown partition. */
        private const int UnknownPartition = -1;
        
        /** Fallback init lock. */
        private readonly object _fallbackMapLock = new object();

        /** Affinity. */
        private readonly CacheAffinityImpl _affinity;

        /** Generic map, used by default, should fit most use cases. */
        private volatile ConcurrentDictionary<TK, NearCacheEntry<TV>> _map = 
            new ConcurrentDictionary<TK, NearCacheEntry<TV>>();

        /** Non-generic map. Switched to when same cache is used with different generic arguments.
         * Less efficient because of boxing and casting. */
        private volatile ConcurrentDictionary<object, NearCacheEntry<object>> _fallbackMap;

        /** Topology version func. Returns boxed <see cref="AffinityTopologyVersion"/>.
         * Boxed copy is passed directly to <see cref="NearCacheEntry{T}"/>, avoiding extra allocations.
         * This way for every unique <see cref="AffinityTopologyVersion"/> we only have one boxed copy,
         * and we can update <see cref="NearCacheEntry{T}.Version"/> atomically without locks. */
        private readonly Func<object> _affinityTopologyVersionFunc;

        /// <summary>
        /// Initializes a new instance of the <see cref="NearCache{TK, TV}"/> class. 
        /// </summary>
        public NearCache(Func<object> affinityTopologyVersionFunc, CacheAffinityImpl affinity)
        {
            // TODO: Enable callbacks in Java.
            // Callbacks should be disabled by default for all caches to avoid unnecessary overhead.

            _affinityTopologyVersionFunc = affinityTopologyVersionFunc;
            _affinity = affinity;
        }

        public bool TryGetValue<TKey, TVal>(TKey key, out TVal val)
        {
            // ReSharper disable once SuspiciousTypeConversion.Global (reviewed)
            var map = _map as ConcurrentDictionary<TKey, NearCacheEntry<TVal>>;
            if (map != null)
            {
                NearCacheEntry<TVal> entry;
                if (map.TryGetValue(key, out entry))
                {
                    if (IsValid(key, entry))
                    {
                        val = entry.Value;
                        return true;
                    }

                    // Remove invalid entry to free up memory.
                    // NOTE: We may end up removing a good entry that was inserted concurrently,
                    // but this does not violate correctness, only causes a potential near cache miss.
                    map.TryRemove(key, out entry);
                    val = default(TVal);
                    return false;
                }
            }
            
            if (_fallbackMap != null)
            {
                NearCacheEntry<object> fallbackEntry;
                if (_fallbackMap.TryGetValue(key, out fallbackEntry))
                {
                    if (IsValid(key, fallbackEntry))
                    {
                        val = (TVal) fallbackEntry.Value;
                        return true;
                    }

                    _fallbackMap.TryRemove(key, out fallbackEntry);
                }
            }

            val = default(TVal);
            return false;
        }
        
        public TVal GetOrAdd<TKey, TVal>(TKey key, Func<TKey, TVal> valueFactory)
        {
            // ReSharper disable once SuspiciousTypeConversion.Global (reviewed)
            var map = _map as ConcurrentDictionary<TKey, NearCacheEntry<TVal>>;
            if (map != null)
            {
                return map.AddOrUpdate(key, k => GetEntry(valueFactory, k),
                    (k, old) => IsValid(k, old) ? old : GetEntry(valueFactory, k)).Value;
            }
            
            EnsureFallbackMap();

            Func<object, NearCacheEntry<object>> factory = k => GetEntry(_ => (object) valueFactory((TKey) k), k);
            return (TVal) _fallbackMap.AddOrUpdate(
                key, 
                factory,
                (k, old) => IsValid(k, old) ? old : factory(k)).Value;
        }

        public TVal GetOrAdd<TKey, TVal>(TKey key, TVal val)
        {
            // ReSharper disable once SuspiciousTypeConversion.Global (reviewed)
            var map = _map as ConcurrentDictionary<TKey, NearCacheEntry<TVal>>;
            if (map != null)
            {
                // TODO: Validate on get
                return map.GetOrAdd(key, k => GetEntry(_ => val, k)).Value;
            }
            
            EnsureFallbackMap();

            return (TVal) _fallbackMap.GetOrAdd(key, k => GetEntry(_ => (object) val, k)).Value;
        }
        
        /** <inheritdoc /> */
        public int GetSize()
        {
            // TODO: Count valid entries only.
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

        /** <inheritdoc /> */
        public bool ContainsKey<TKey, TVal>(TKey key)
        {
            object _;
            return TryGetValue(key, out _);
        }

        /** <inheritdoc /> */
        public void Update(IBinaryStream stream, Marshaller marshaller)
        {
            Debug.Assert(stream != null);
            Debug.Assert(marshaller != null);

            var reader = marshaller.StartUnmarshal(stream);

            var key = reader.ReadObject<object>();
            var hasVal = reader.ReadBoolean();
            
            var val = hasVal ? reader.ReadObject<object>() : null;
            var part = hasVal ? reader.ReadInt() : 0;
            var ver = hasVal
                    ? new AffinityTopologyVersion(reader.ReadLong(), reader.ReadInt())
                    : default(AffinityTopologyVersion);
            
            var typeMatch = key is TK && (!hasVal || val is TV);

            var map = _map;
            if (map != null && typeMatch)
            {
                if (hasVal)
                {
                    // Reuse existing boxed copy when possible to reduce allocations.
                    var currentVerBoxed = _affinityTopologyVersionFunc();
                    var verBoxed = (AffinityTopologyVersion) currentVerBoxed == ver ? currentVerBoxed : ver;
                    
                    map[(TK) key] = new NearCacheEntry<TV>((TV) val, verBoxed, part);
                }
                else
                {
                    NearCacheEntry<TV> unused;
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
                _fallbackMap[key] = new NearCacheEntry<object>(val, ver, part);
            }
            else
            {
                NearCacheEntry<object> unused;
                _fallbackMap.TryRemove(key, out unused);
            }
        }

        /** <inheritdoc /> */
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

        /// <summary>
        /// Ensures that fallback map exists.
        /// </summary>
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
                _fallbackMap = new ConcurrentDictionary<object, NearCacheEntry<object>>();
            }
        }

        /// <summary>
        /// Checks whether specified cache entry is still valid, based on Affinity Topology Version.
        /// When primary node changes for a key, GridNearCacheEntry stops receiving updates for that key,
        /// because reader ("subscription") on new primary is not yet established.
        /// <para />
        /// This method is similar to GridNearCacheEntry.valid(). 
        /// </summary>
        /// <param name="key">Entry key.</param>
        /// <param name="entry">Entry to validate.</param>
        /// <typeparam name="TKey">Key type.</typeparam>
        /// <typeparam name="TVal">Value type.</typeparam>
        /// <returns>True if entry is valid and can be returned to the user; false otherwise.</returns>
        private bool IsValid<TKey, TVal>(TKey key, NearCacheEntry<TVal> entry)
        {
            // See comments on _affinityTopologyVersionFunc about boxed copy approach. 
            var currentVerBoxed = _affinityTopologyVersionFunc();
            var entryVerBoxed = entry.Version;
            
            Debug.Assert(currentVerBoxed != null);
            Debug.Assert(entryVerBoxed != null);
            
            if (ReferenceEquals(currentVerBoxed, entryVerBoxed))
            {
                // Happy path: true on stable topology.
                return true;
            }

            var entryVer = (AffinityTopologyVersion) entryVerBoxed;
            var currentVer = (AffinityTopologyVersion) currentVerBoxed;

            if (entryVer >= currentVer)
            {
                return true;
            }

            var part = entry.Partition == UnknownPartition ? GetPartition(key) : entry.Partition;
            var valid = _affinity.IsAssignmentValid(entryVer, part);

            if (valid)
            {
                // Update entry with current version and known partition to speed up future checks.
                entry.Partition = part;
                entry.Version = currentVerBoxed;
            }

            return valid;
        }

        private NearCacheEntry<TVal> GetEntry<TKey, TVal>(Func<TKey, TVal> valueFactory, TKey k)
        {
            // TODO: Make sure this is not invoked unnecessarily, when actual entry is already initialized from a callback.
            
            // Important: get the version before the value. 
            var ver = _affinityTopologyVersionFunc();
            var val = valueFactory(k);
            
            return new NearCacheEntry<TVal>(val,  ver, UnknownPartition);
        }

        private int GetPartition<TKey>(TKey k)
        {
            // TODO: Calculate locally when possible (rendezvous).
            // Is it worth the complexity? Don't think so, the case is rare, does not happen on stable topologies,
            // happens once per key.
            return _affinity.GetPartition(k);
        }
    }
}
