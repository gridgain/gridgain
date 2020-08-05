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

package org.apache.ignite.internal.processors.cache.dr;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.GridCacheManager;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.dr.GridDrType;
import org.jetbrains.annotations.Nullable;

/**
 * Replication manager class which processes cache replication events.
 */
public interface GridCacheReplicationManager extends GridCacheManager {
    /**
     * @return Data center ID.
     */
    public byte dataCenterId();

    /**
     * Performs replication.
     *
     * @param key Key.
     * @param val Value.
     * @param ttl TTL.
     * @param expireTime Expire time.
     * @param ver Version.
     * @param drType Replication type.
     * @param topVer Topology version.
     * @throws IgniteCheckedException If failed.
     */
    public void replicate(KeyCacheObject key,
        @Nullable CacheObject val,
        long ttl,
        long expireTime,
        GridCacheVersion ver,
        GridDrType drType,
        AffinityTopologyVersion topVer)throws IgniteCheckedException;

    /**
     * Process partitions exchange event.
     *
     * @param topVer Topology version.
     * @param left {@code True} if exchange has been caused by node leave.
     * @throws IgniteCheckedException If failed.
     */
    public void onExchange(AffinityTopologyVersion topVer, boolean left) throws IgniteCheckedException;

    /**
     * @return {@code True} is DR is enabled.
     */
    public boolean enabled();

    /**
     * @return {@code True} if receives DR data.
     */
    public boolean receiveEnabled();

    /**
     * In case some partition is evicted, we remove entries of this partition from backup queue.
     *
     * @param part Partition.
     */
    public void partitionEvicted(int part);

    /**
     * Callback for received entries from receiver hub.
     *
     * @param entriesCnt Number of received entries.
     */
    public void onReceiveCacheEntriesReceived(int entriesCnt);

    /**
     * Callback for manual conflict resolution.
     *
     * @param useNew Use new.
     * @param useOld Use old.
     * @param merge Merge.
     */
    public void onReceiveCacheConflictResolved(boolean useNew, boolean useOld, boolean merge);

    /**
     * Resets metrics for current cache.
     */
    public void resetMetrics();
}