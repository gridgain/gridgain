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

#ifndef _IGNITE_CACHE_AFFINITY_IMPL
#define _IGNITE_CACHE_AFFINITY_IMPL

#include <ignite/common/concurrent.h>
#include <ignite/jni/java.h>

#include <ignite/impl/interop/interop_target.h>
#include <ignite/cluster/cluster_node.h>

namespace ignite
{
    namespace impl
    {
        namespace cache
        {
            struct Command
            {
                enum Type
                {
                    AFFINITY_KEY = 1,

                    ALL_PARTITIONS = 2,

                    BACKUP_PARTITIONS = 3,

                    IS_BACKUP = 4,

                    IS_PRIMARY = 5,

                    IS_PRIMARY_OR_BACKUP = 6,

                    MAP_KEY_TO_NODE = 7,

                    MAP_KEY_TO_PRIMARY_AND_BACKUPS = 8,

                    MAP_KEYS_TO_NODES = 9,

                    MAP_PARTITION_TO_NODE = 10,

                    MAP_PARTITION_TO_PRIMARY_AND_BACKUPS = 11,

                    MAP_PARTITIONS_TO_NODES = 12,

                    PARTITION = 13,

                    PRIMARY_PARTITIONS = 14,

                    PARTITIONS = 15,

                };
            };

            /* Forward declaration. */
            class CacheAffinityImpl;

            /* Shared pointer. */
            typedef common::concurrent::SharedPointer<CacheAffinityImpl> SP_CacheAffinityImpl;

            /**
             * Cache affinity implementation.
             */
            class IGNITE_FRIEND_EXPORT CacheAffinityImpl : private interop::InteropTarget
            {
                typedef common::concurrent::SharedPointer<IgniteEnvironment> SP_IgniteEnvironment;
            public:
                /**
                 * Constructor used to create new instance.
                 *
                 * @param env Environment.
                 * @param javaRef Reference to java object.
                 */
                CacheAffinityImpl(SP_IgniteEnvironment env, jobject javaRef);

                /**
                 * Get number of partitions in cache according to configured affinity function.
                 *
                 * @return Number of partitions.
                 */
                int GetPartitions();

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
                    IgniteError err;
                    In1Operation<K> inOp(key);

                    int ret = static_cast<int>(InStreamOutLong(Command::PARTITION, inOp, err));
                    IgniteError::ThrowIfNeeded(err);

                    return ret;
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
                template <typename K>
                bool IsPrimary(ignite::cluster::ClusterNode node, K key)
                {
                    std::cout << "MYLOGTAG:" << "IsPrimary(node = " << node.GetId() << " key = " << (key) << ")" << std::endl;

                    IgniteError err;

                    In2Operation<Guid, K> inOp(node.GetId(), key);

                    bool ret = OutOpDEBUG(Command::IS_PRIMARY, inOp, err);
/*
                    common::concurrent::SharedPointer<interop::InteropMemory> memIn = GetEnvironment().AllocateMemory();
                    interop::InteropOutputStream out(memIn.Get());
                    binary::BinaryWriterImpl writer(&out, GetEnvironment().GetTypeManager());

                    // writer.WriteInt64(node.GetId().GetMostSignificantBits());
                    // writer.WriteInt64(node.GetId().GetLeastSignificantBits());
                    writer.WriteObject(node.GetId());
                    writer.WriteObject<K>(key);

                    out.Synchronize();

                    bool ret = InStreamOutLongDEBUG(Command::IS_PRIMARY,
                        *memIn.Get(), err);
*/
                    IgniteError::ThrowIfNeeded(err);

                    std::cout << "MYLOGTAG:" << "IsPrimary(node = " << node.GetId() << " key = " << (key) << ")" "ret = " << ret << std::endl;

                    return ret;
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
                bool IsBackup(ignite::cluster::ClusterNode node, K key)
                {
                    IgniteError err;

                    In2Operation<Guid, K> inOp(node.GetId(), key);

                    bool ret = OutOp(Command::IS_BACKUP, inOp, err);

/*
                    common::concurrent::SharedPointer<interop::InteropMemory> memIn = GetEnvironment().AllocateMemory();
                    interop::InteropOutputStream out(memIn.Get());
                    binary::BinaryWriterImpl writer(&out, GetEnvironment().GetTypeManager());

                    writer.WriteObject(node.GetId());
                    writer.WriteObject<K>(key);

                    out.Synchronize();

                    bool ret = InStreamOutLongDEBUG(Command::IS_BACKUP,
                        *memIn.Get(), err);
*/
                    IgniteError::ThrowIfNeeded(err);

                    return ret;
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
                bool IsPrimaryOrBackup(ignite::cluster::ClusterNode node, K key)
                {
                    IgniteError err;

                    In2Operation<Guid, K> inOp(node.GetId(), key);

                    bool ret = OutOp(Command::IS_PRIMARY_OR_BACKUP, inOp, err);

/*
                    common::concurrent::SharedPointer<interop::InteropMemory> memIn = GetEnvironment().AllocateMemory();
                    interop::InteropOutputStream out(memIn.Get());
                    binary::BinaryWriterImpl writer(&out, GetEnvironment().GetTypeManager());

                    writer.WriteObject(node.GetId());
                    writer.WriteObject<K>(key);

                    out.Synchronize();

                    bool ret = InStreamOutLongDEBUG(Command::IS_PRIMARY_OR_BACKUP,
                        *memIn.Get(), err);
*/
                    IgniteError::ThrowIfNeeded(err);

                    return ret;
                }

                /**
                 * Get partition ids for which the given cluster node has primary ownership.
                 *
                 * @param node Cluster node.
                 * @return Container of partition ids for which the given cluster node has primary ownership.
                 */
                std::vector<int> GetPrimaryPartitions(ignite::cluster::ClusterNode node);

                /**
                 * Get partition ids for which given cluster node has backup ownership.
                 *
                 * @param node Cluster node.
                 * @return Container of partition ids for which given cluster node has backup ownership.
                 */
                std::vector<int> GetBackupPartitions(ignite::cluster::ClusterNode node);

                /**
                 * Get partition ids for which given cluster node has any ownership (either primary or backup).
                 *
                 * @param node Cluster node.
                 * @return Container of partition ids for which given cluster node has any ownership (either primary or backup).
                 */
                std::vector<int> GetAllPartitions(ignite::cluster::ClusterNode node);

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
                    TR ret;
                    In1Operation<TK> inOp(key);
                    Out1Operation<TR> outOp(ret);

                    IgniteError err;
                    InteropTarget::OutInOp(Command::AFFINITY_KEY, inOp, outOp, err);
                    IgniteError::ThrowIfNeeded(err);

                    return ret;
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
                std::map<ignite::cluster::ClusterNode, std::list<TK> > MapKeysToNodes(std::list<TK> keys)
                {
                    common::concurrent::SharedPointer<interop::InteropMemory> memIn = GetEnvironment().AllocateMemory();
                    common::concurrent::SharedPointer<interop::InteropMemory> memOut = GetEnvironment().AllocateMemory();
                    interop::InteropOutputStream out(memIn.Get());
                    binary::BinaryWriterImpl writer(&out, GetEnvironment().GetTypeManager());

                    writer.WriteInt32(static_cast<int32_t>(keys.size()));
                    for (typename std::list<TK>::iterator it = keys.begin(); it != keys.end(); ++it)
                        writer.WriteObject<TK>(*it);

                    out.Synchronize();

                    IgniteError err;
                    InStreamOutStream(Command::MAP_KEYS_TO_NODES, *memIn.Get(), *memOut.Get(), err);
                    IgniteError::ThrowIfNeeded(err);

                    interop::InteropInputStream inStream(memOut.Get());
                    binary::BinaryReaderImpl reader(&inStream);

                    std::map<ignite::cluster::ClusterNode, std::list<TK> > ret;

                    int32_t cnt = reader.ReadInt32();
                    for (int32_t i = 0; i < cnt; i++)
                    {
                        std::list<TK> val;

                        ignite::cluster::ClusterNode key(GetEnvironment().GetNode(reader.ReadGuid()));
                        reader.ReadCollection<TK>(std::back_inserter(val));

                        ret.insert(std::pair<ignite::cluster::ClusterNode, std::list<TK> >(key, val));
                    }

                    return ret;
                }

                /**
                 * This method provides ability to detect to which primary node the given key is mapped.
                 * Use it to determine which nodes are storing which keys prior to sending
                 * jobs that access these keys.
                 *
                 * @tparam TK Key to map type.
                 *
                 * @param key Key to map to node.
                 * @return Primary node for the key.
                 */
                template <typename TK>
                ignite::cluster::ClusterNode MapKeyToNode(TK key)
                {
                    std::cout << "MYLOGTAG:" << "MapKeyToNode(key = " << key << ")" << std::endl;

                    Guid nodeId;
                    In1Operation<TK> inOp(key);
                    Out1Operation<Guid> outOp(nodeId);

                    IgniteError err;
                    InteropTarget::OutInOp(Command::MAP_KEY_TO_NODE, inOp, outOp, err);
                    IgniteError::ThrowIfNeeded(err);

                    std::cout << "MYLOGTAG:" << "MapKeyToNode:nodeId = " << nodeId << std::endl;

                    common::concurrent::SharedPointer < ignite::impl::cluster::ClusterNodeImpl > pnode = GetEnvironment().GetNode(nodeId);
                    if (!pnode.IsValid())
                        std::cout << "MYLOGTAG:" << "MapKeyToNode:pnode INVALID" << std::endl;

                    ignite::cluster::ClusterNode node(pnode);

                    return node;
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
                std::list<ignite::cluster::ClusterNode> MapKeyToPrimaryAndBackups(TK key)
                {
                    common::concurrent::SharedPointer<interop::InteropMemory> memIn = GetEnvironment().AllocateMemory();
                    common::concurrent::SharedPointer<interop::InteropMemory> memOut = GetEnvironment().AllocateMemory();
                    interop::InteropOutputStream out(memIn.Get());
                    binary::BinaryWriterImpl writer(&out, GetEnvironment().GetTypeManager());

                    writer.WriteObject(key);

                    out.Synchronize();

                    std::cout << "MYLOGTAG:" << "MapKeyToPrimaryAndBackups( key = " << key << ")" << std::endl;

                    IgniteError err;
                    InStreamOutStream(Command::MAP_KEY_TO_PRIMARY_AND_BACKUPS, *memIn.Get(), *memOut.Get(), err);
                    IgniteError::ThrowIfNeeded(err);

                    interop::InteropInputStream inStream(memOut.Get());
                    binary::BinaryReaderImpl reader(&inStream);

                    std::list<ignite::cluster::ClusterNode> ret;
                    int32_t cnt = reader.ReadInt32();
                    std::cout << "MYLOGTAG:" << "MapKeyToPrimaryAndBackups:cnt = " << cnt << std::endl;
                    for (int32_t i = 0; i < cnt; i++) {
                        common::concurrent::SharedPointer < ignite::impl::cluster::ClusterNodeImpl > pnode = GetEnvironment().GetNode(reader.ReadGuid());
                        if (!pnode.IsValid())
                            std::cout << "MYLOGTAG:" << "MapKeyToPrimaryAndBackups:pnode INVALID" << std::endl;

                        ignite::cluster::ClusterNode node(pnode);
                        ret.push_back(node);
                    }

                    return ret;
                }

                /**
                 * Get primary node for the given partition.
                 *
                 * @param part Partition id.
                 * @return Primary node for the given partition.
                 */
                ignite::cluster::ClusterNode MapPartitionToNode(int part);

                /**
                 * Get primary nodes for the given partitions.
                 *
                 * @param parts Partition ids.
                 * @return Mapping of given partitions to their primary nodes.
                 */
                std::map<int, ignite::cluster::ClusterNode> MapPartitionsToNodes(std::vector<int> parts);

                /**
                 * Get primary and backup nodes for partition.
                 * Note that primary node is always first in the returned collection.
                 *
                 * @param part Partition to get affinity nodes for.
                 * @return Collection of primary and backup nodes for partition with primary node always first.
                 */
                std::list<ignite::cluster::ClusterNode> MapPartitionToPrimaryAndBackups(int part);

            private:
                /**
                 * Get partition ids for which given cluster node has different ownership.
                 *
                 * @param opType Operation type.
                 * @param node Cluster node.
                 * @return Container of partition ids.
                 */
                std::vector<int> GetPartitions(int32_t opType, ignite::cluster::ClusterNode node);
            };
        }
    }
}

#endif // _IGNITE_CACHE_AFFINITY_IMPL