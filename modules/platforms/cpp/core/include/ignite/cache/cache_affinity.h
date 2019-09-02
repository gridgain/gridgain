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

 /**
  * @file
  * Declares ignite::cache::CacheAffinity class.
  */

#ifndef _IGNITE_CACHE_AFFINITY
#define _IGNITE_CACHE_AFFINITY

#include <ignite/cluster/cluster_group.h>

#include <ignite/impl/cache/cache_affinity_impl.h>

namespace ignite
{
    namespace cache
    {
        /**
         * Provides affinity information to detect which node is primary and which nodes are backups
         * for a partitioned or replicated cache.
         * You can get an instance of this interface by calling Ignite.GetAffinity(cacheName) method.
         */
        class IGNITE_IMPORT_EXPORT CacheAffinity
        {
        public:
            /**
             * Constructor.
             *
             * @param impl Pointer to cache affinity implementation.
             */
            CacheAffinity(impl::cache::SP_CacheAffinityImpl impl) :
                impl(impl)
            {
                // No-op.
            }

            /**
             * Get number of partitions in cache according to configured affinity function.
             *
             * @return Number of partitions.
             */
            int GetPartitions()
            {
                return impl.Get()->GetPartitions();
            }

            /**
             * Get partition id for the given key.
             *
             * @tparam K Key type.
             *
             * @param key Key to get partition id for.
             * @return Partition id.
             */
            template <typename K>
            int GetPartition(K key)
            {
                return impl.Get()->GetPartition(key);
            }

            /**
             * Return true if given node is the primary node for given key.
             *
             * @tparam K Key type.
             *
             * @param node Cluster node.
             * @param key Key to check.
             * @return True if given node is primary node for given key.
             */
            template<typename K>
            bool IsPrimary(cluster::ClusterNode node, K key)
            {
                return impl.Get()->IsPrimary(node, key);
            }

            /**
             * Return true if local node is one of the backup nodes for given key.
             *
             * @tparam K Key type.
             *
             * @param node Cluster node.
             * @param key Key to check.
             * @return True if local node is one of the backup nodes for given key.
             */
            template <typename K>
            bool IsBackup(cluster::ClusterNode node, K key)
            {
                return impl.Get()->IsBackup(node, key);
            }

            /**
             * Returns true if local node is primary or one of the backup nodes.
             * This method is essentially equivalent to calling
             * "isPrimary(ClusterNode, Object) || isBackup(ClusterNode, Object))",
             * however it is more efficient as it makes both checks at once.
             *
             * @tparam K Key type.
             *
             * @param node Cluster node.
             * @param key Key to check.
             * @return True if local node is primary or one of the backup nodes.
             */
            template <typename K>
            bool IsPrimaryOrBackup(cluster::ClusterNode node, K key)
            {
                return impl.Get()->IsPrimaryOrBackup(node, key);
            }

            /**
             * Get partition ids for which the given cluster node has primary ownership.
             *
             * @param node Cluster node.
             * @return Container of partition ids for which the given cluster node has primary ownership.
             */
            std::vector<int> GetPrimaryPartitions(cluster::ClusterNode node)
            {
                return impl.Get()->GetPrimaryPartitions(node);
            }

            /**
             * Get partition ids for which given cluster node has backup ownership.
             *
             * @param node Cluster node.
             * @return Container of partition ids for which given cluster node has backup ownership.
             */
            std::vector<int> GetBackupPartitions(cluster::ClusterNode node)
            {
                return impl.Get()->GetBackupPartitions(node);
            }

            /**
             * Get partition ids for which given cluster node has any ownership (either primary or backup).
             *
             * @param node Cluster node.
             * @return Container of partition ids for which given cluster node has any ownership (either primary or backup).
             */
            std::vector<int> GetAllPartitions(cluster::ClusterNode node)
            {
                return impl.Get()->GetAllPartitions(node);
            }

            /**
             * Map passed in key to a key which will be used for node affinity.
             *
             * @tparam TK Key to map type.
             * @tparam TR Key to be used for node-to-affinity mapping type.
             *
             * @param key Key to map.
             * @return Key to be used for node-to-affinity mapping (may be the same key as passed in).
             */
            template <typename TK, typename TR>
            TR GetAffinityKey(TK key)
            {
                return impl.Get()->GetAffinityKey<TK, TR>(key);
            }

            /**
             * This method provides ability to detect which keys are mapped to which nodes.
             * Use it to determine which nodes are storing which keys prior to sending
             * jobs that access these keys.
             *
             * @tparam TK Key to map type.
             *
             * @param keys Keys to map to nodes.
             * @return Map of nodes to keys or empty map if there are no alive nodes for this cache.
             */
            template<typename TK>
            std::map<cluster::ClusterNode, std::list<TK> > MapKeysToNodes(std::list<TK> keys)
            {
                return impl.Get()->MapKeysToNodes(keys);
            }

            /**
             * This method provides ability to detect to which primary node the given key is mapped.
             * Use it to determine which nodes are storing which keys prior to sending
             * jobs that access these keys.

             * @tparam TK Key to map type.
             *
             * @param key Key to map to node.
             * @return Primary node for the key.
             */
            template <typename TK>
            cluster::ClusterNode MapKeyToNode(TK key)
            {
                return impl.Get()->MapKeyToNode(key);
            }

            /**
             * Get primary and backup nodes for the key.
             * Note that primary node is always first in the returned collection.
             *
             * @tparam TK Key to map type.
             *
             * @param key Key to map to nodes.
             * @return Collection of cluster nodes.
             */
            template <typename TK>
            std::list<cluster::ClusterNode> MapKeyToPrimaryAndBackups(TK key)
            {
                return impl.Get()->MapKeyToPrimaryAndBackups(key);
            }

            /**
             * Get primary node for the given partition.
             *
             * @param part Partition id.
             * @return Primary node for the given partition.
             */
            cluster::ClusterNode MapPartitionToNode(int part)
            {
                return impl.Get()->MapPartitionToNode(part);
            }

            /**
             * Get primary nodes for the given partitions.
             *
             * @param parts Partition ids.
             * @return Mapping of given partitions to their primary nodes.
             */
            std::map<int, cluster::ClusterNode> MapPartitionsToNodes(std::vector<int> parts)
            {
                return impl.Get()->MapPartitionsToNodes(parts);
            }

            /**
             * Get primary and backup nodes for partition.
             * Note that primary node is always first in the returned collection.
             *
             * @param part Partition to get affinity nodes for.
             * @return Collection of primary and backup nodes for partition with primary node always first.
             */
            std::list<cluster::ClusterNode> MapPartitionToPrimaryAndBackups(int part)
            {
                return impl.Get()->MapPartitionToPrimaryAndBackups(part);
            }

        private:
            impl::cache::SP_CacheAffinityImpl impl;
        };
    }
}

#endif //_IGNITE_CACHE_AFFINITY