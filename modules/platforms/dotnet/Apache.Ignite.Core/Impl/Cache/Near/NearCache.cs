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

        /** Topology versions. */
        private readonly ConcurrentStack<AffinityTopologyVersion> _affinityTopologyVersions;

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
        public NearCache(ConcurrentStack<AffinityTopologyVersion> affinityTopologyVersions, CacheAffinityImpl affinity)
        {
            // TODO: Enable callbacks in Java.
            // Callbacks should be disabled by default for all caches to avoid unnecessary overhead.

            _affinityTopologyVersions = affinityTopologyVersions;
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
                return map.AddOrUpdate(key, k => GetEntry(valueFactory, k),
                    (k, old) => IsValid(old) ? old : GetEntry(valueFactory, k)).Val;
            }
            
            EnsureFallbackMap();

            Func<object, NearCacheEntry<object>> factory = k => GetEntry(_ => (object) valueFactory((TKey) k), k);
            return (TVal) _fallbackMap.AddOrUpdate(
                key, 
                factory,
                (k, old) => IsValid(old) ? old : factory(k)).Val;
        }

        public TVal GetOrAdd<TKey, TVal>(TKey key, TVal val)
        {
            // ReSharper disable once SuspiciousTypeConversion.Global (reviewed)
            var map = _map as ConcurrentDictionary<TKey, NearCacheEntry<TVal>>;
            if (map != null)
            {
                // TODO: Validate on get
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
        

        /// <summary>
        /// Checks whether specified cache entry is still valid, based on Affinity Topology Version.
        /// When primary node changes for a key, GridNearCacheEntry stops receiving updates for that key,
        /// because reader ("subscription") on new primary is not yet established.
        /// <para />
        /// This method is similar to GridNearCacheEntry.valid(). 
        /// </summary>
        /// <param name="entry">Entry to validate.</param>
        /// <param name="version">Topology version to check against.</param>
        /// <typeparam name="T">Entry type.</typeparam>
        /// <returns>True if entry is valid and can be returned to the user; false otherwise.</returns>
        private bool IsValid<T>(NearCacheEntry<T> entry, AffinityTopologyVersion? version = null)
        {
            // TODO: Compare perf with a call to Java version of this.
            // Is the complexity and memory usage worth it?
            var ver = version ?? GetCurrentTopologyVersion();

            if (entry.Version >= ver)
            {
                return true;
            }

            // Check that primary has never changed since entry has been cached.
            // Primary change requires invalidation (see GridNearCacheEntry.valid()).
            // TODO: Verify change to and from backup node. Hopefully it does not matter for us.
            var oldPrimary = GetPrimaryNodeId(entry.Version, entry.Partition);

            foreach (var newVer in _affinityTopologyVersions)
            {
                if (newVer <= entry.Version)
                {
                    continue;
                }
                
                if (GetPrimaryNodeId(newVer, entry.Partition) != oldPrimary)
                {
                    return false;
                }
            }

            return true;
        }

        private Guid? GetPrimaryNodeId(AffinityTopologyVersion ver, int part)
        {
            var nodeIdPerPartition = GetNodeIdPerPartition(ver);
            if (nodeIdPerPartition == null)
            {
                return null;
            }

            Debug.Assert(part >= 0);
            Debug.Assert(part < nodeIdPerPartition.Length);

            return nodeIdPerPartition[part];
        }

        private Guid[] GetNodeIdPerPartition(AffinityTopologyVersion ver)
        {
            Guid[] nodeIdPerPartition;

            // ReSharper disable once InconsistentlySynchronizedField
            var partitionNodeIds = _partitionNodeIds;
            if (partitionNodeIds.TryGetValue(ver, out nodeIdPerPartition))
            {
                return nodeIdPerPartition;
            }

            // TODO: Cache min version when history limit is reached.
            if (partitionNodeIds.Count == PartitionNodeIdsMaxSize && ver < partitionNodeIds.Keys.Min())
            {
                // Version is too old, don't request.
                return null;
            }

            lock (_partitionNodeIdsLock)
            {
                // TODO: Use plain array for _partitionNodeIds AND _affinityTopologyVersions
                // So they have same index for same version
                // Since we only append (under lock), and never use `foreach` (only `for`), we should be fine.
                // Purge old items by setting them to null
                if (_partitionNodeIds.TryGetValue(ver, out nodeIdPerPartition))
                {
                    return nodeIdPerPartition;
                }
                
                nodeIdPerPartition = _affinity.MapAllPartitionsToNodes(ver);
                
                if (nodeIdPerPartition != null)
                {
                    partitionNodeIds = new Dictionary<AffinityTopologyVersion, Guid[]>(_partitionNodeIds);
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
                GetCurrentTopologyVersion(), 
                GetPartition(k));
        }

        private int GetPartition<TKey>(TKey k)
        {
            // TODO: Calculate locally when possible (rendezvous).
            return _affinity.GetPartition(k);
        }

        private AffinityTopologyVersion GetCurrentTopologyVersion()
        {
            AffinityTopologyVersion ver;
            return _affinityTopologyVersions.TryPeek(out ver) ? ver : default(AffinityTopologyVersion);
        }
    }
}
