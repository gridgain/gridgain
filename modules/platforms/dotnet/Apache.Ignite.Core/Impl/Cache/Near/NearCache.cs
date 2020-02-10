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
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using Apache.Ignite.Core.Cache.Affinity;
    using Apache.Ignite.Core.Impl.Binary;
    using Apache.Ignite.Core.Impl.Binary.IO;

    /// <summary>
    /// Holds near cache data for a given cache, serves one or more <see cref="CacheImpl{TK,TV}"/> instances.
    /// </summary>
    internal sealed class NearCache<TK, TV> : INearCache
    {
        /** Maximum size of _partitionNodeIds map.  */
        private const int PartitionNodeIdsMaxSize = 100;
        
        /** Partition assignment map: from topology version to array of node id per partition. */
        private volatile Dictionary<AffinityTopologyVersion, Guid[]> _partitionNodeIds
            = new Dictionary<AffinityTopologyVersion, Guid[]>();

        /** Partition node ids lock. */
        private readonly object _partitionNodeIdsLock = new object();
        
        /** Fallback init lock. */
        private readonly object _fallbackMapLock = new object();

        /** Topology version func. */
        private readonly Func<AffinityTopologyVersion> _affinityTopologyVersionFunc;

        /** Affinity. */
        private readonly CacheAffinityImpl _affinity;

        /** Generic map, used by default, should fit most use cases. */
        private volatile ConcurrentDictionary<TK, NearCacheEntry<TV>> _map = 
            new ConcurrentDictionary<TK, NearCacheEntry<TV>>();

        /** Non-generic map. Switched to when same cache is used with different generic arguments.
         * Less efficient because of boxing and casting. */
        private volatile ConcurrentDictionary<object, NearCacheEntry<object>> _fallbackMap;

        /// <summary>
        /// Initializes a new instance of the <see cref="NearCache{TK, TV}"/> class. 
        /// </summary>
        public NearCache(Func<AffinityTopologyVersion> affinityTopologyVersionFunc, CacheAffinityImpl affinity)
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
                    if (IsValid(entry))
                    {
                        val = entry.Val;
                        return true;
                    }

                    // TODO: Remove from map atomically, otherwise we risk wasting memory.
                    val = default(TVal);
                    return false;
                }
            }
            
            if (_fallbackMap != null)
            {
                NearCacheEntry<object> fallbackEntry;
                if (_fallbackMap.TryGetValue(key, out fallbackEntry) && IsValid(fallbackEntry))
                {
                    val = (TVal) fallbackEntry.Val;
                    return true;
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
                return map.GetOrAdd(key, k => GetEntry(valueFactory, k)).Val;
            }
            
            EnsureFallbackMap();

            return (TVal) _fallbackMap
                .GetOrAdd(key, k => GetEntry(_ => (object) valueFactory((TKey) k), k)).Val;
        }

        public TVal GetOrAdd<TKey, TVal>(TKey key, TVal val)
        {
            // ReSharper disable once SuspiciousTypeConversion.Global (reviewed)
            var map = _map as ConcurrentDictionary<TKey, NearCacheEntry<TVal>>;
            if (map != null)
            {
                return map.GetOrAdd(key, k => GetEntry(_ => val, k)).Val;
            }
            
            EnsureFallbackMap();

            return (TVal) _fallbackMap.GetOrAdd(key, k => GetEntry(_ => (object) val, k)).Val;
        }

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
                    map[(TK) key] = new NearCacheEntry<TV>((TV)val, ver, part);
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

        public bool ContainsKey<TKey, TVal>(TKey key)
        {
            object _;
            return TryGetValue(key, out _);
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
                _fallbackMap = new ConcurrentDictionary<object, NearCacheEntry<object>>();
            }
        }
        
        
        private bool IsValid<T>(NearCacheEntry<T> entry)
        {
            var currentVersion = _affinityTopologyVersionFunc();

            if (entry.Version >= currentVersion)
            {
                return true;
            }

            var newPrimary = GetPrimaryNodeId(currentVersion, entry.Partition, true);

            return newPrimary != null && 
                   newPrimary == GetPrimaryNodeId(entry.Version, entry.Partition, false);
        }

        private Guid? GetPrimaryNodeId(AffinityTopologyVersion ver, int part, bool requestMissingAssignment)
        {
            var nodeIdPerPartition = GetNodeIdPerPartition(ver, requestMissingAssignment);
            if (nodeIdPerPartition == null)
            {
                return null;
            }

            Debug.Assert(part >= 0);
            Debug.Assert(part < nodeIdPerPartition.Length);

            return nodeIdPerPartition[part];
        }

        private Guid[] GetNodeIdPerPartition(AffinityTopologyVersion ver, bool requestMissingAssignment)
        {
            Guid[] nodeIdPerPartition;

            // ReSharper disable once InconsistentlySynchronizedField
            if (_partitionNodeIds.TryGetValue(ver, out nodeIdPerPartition))
            {
                return nodeIdPerPartition;
            }

            if (!requestMissingAssignment)
            {
                return null;
            }

            lock (_partitionNodeIdsLock)
            {
                if (_partitionNodeIds.TryGetValue(ver, out nodeIdPerPartition))
                {
                    return nodeIdPerPartition;
                }
                
                nodeIdPerPartition = _affinity.MapAllPartitionsToNodes(ver);
                
                if (nodeIdPerPartition != null)
                {
                    var partitionNodeIds = new Dictionary<AffinityTopologyVersion, Guid[]>(_partitionNodeIds);
                    partitionNodeIds[ver] = nodeIdPerPartition;

                    if (partitionNodeIds.Count > PartitionNodeIdsMaxSize)
                    {
                        var oldest = partitionNodeIds.Keys.Min();
                        partitionNodeIds.Remove(oldest);
                    }

                    _partitionNodeIds = partitionNodeIds;
                }

                return nodeIdPerPartition;
            }
        }

        private NearCacheEntry<TVal> GetEntry<TKey, TVal>(Func<TKey, TVal> valueFactory, TKey k)
        {
            // TODO: Make sure this is not invoked unnecessarily, when actual entry is already initialized from a callback.
            return new NearCacheEntry<TVal>(
                valueFactory(k),
                _affinityTopologyVersionFunc(), 
                GetPartition(k));
        }

        private int GetPartition<TKey>(TKey k)
        {
            // TODO: Calculate locally when possible (rendezvous).
            return _affinity.GetPartition(k);
        }
    }
}
