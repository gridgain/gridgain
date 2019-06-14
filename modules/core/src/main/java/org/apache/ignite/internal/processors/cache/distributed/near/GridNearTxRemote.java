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

package org.apache.ignite.internal.processors.cache.distributed.near;

import java.io.Externalizable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheEntryRemovedException;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.distributed.GridDistributedTxRemoteAdapter;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxEntry;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxRemoteSingleStateImpl;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxRemoteStateImpl;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.tostring.GridToStringBuilder;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.jetbrains.annotations.Nullable;

/**
 * Transaction created by system implicitly on remote nodes.
 */
public class GridNearTxRemote extends GridDistributedTxRemoteAdapter {
    /** */
    private static final long serialVersionUID = 0L;

    /** Near node ID. */
    private UUID nearNodeId;

    /** Near transaction ID. */
    private GridCacheVersion nearXidVer;

    /**
     * Empty constructor required for {@link Externalizable}.
     */
    public GridNearTxRemote() {
        // No-op.
    }

    /**
     * This constructor is meant for optimistic transactions.
     *  @param ctx Cache registry.
     * @param topVer Transaction topology version.
     * @param nodeId Node ID.
     * @param nearNodeId Near node ID.
     * @param xidVer XID version.
     * @param commitVer Commit version.
     * @param sys System flag.
     * @param plc IO policy.
     * @param concurrency Concurrency level (should be pessimistic).
     * @param isolation Transaction isolation.
     * @param invalidate Invalidate flag.
     * @param timeout Timeout.
     * @param txSize Expected transaction size.
     * @param subjId Subject ID.
     * @param taskNameHash Task name hash code.
     * @param txLbl Transaction label.
     */
    public GridNearTxRemote(
        GridCacheSharedContext ctx,
        AffinityTopologyVersion topVer,
        UUID nodeId,
        UUID nearNodeId,
        GridCacheVersion xidVer,
        GridCacheVersion nearXidVer,
        GridCacheVersion commitVer,
        boolean sys,
        byte plc,
        TransactionConcurrency concurrency,
        TransactionIsolation isolation,
        boolean invalidate,
        long timeout,
        int txSize,
        @Nullable UUID subjId,
        int taskNameHash,
        boolean single,
        @Nullable String txLbl) {
        super(
            ctx,
            nodeId,
            xidVer,
            commitVer,
            sys,
            plc,
            concurrency,
            isolation,
            invalidate,
            timeout,
            txSize,
            subjId,
            taskNameHash,
            txLbl
        );

        assert nearNodeId != null;

        this.nearNodeId = nearNodeId;
        this.nearXidVer = nearXidVer;

        txState = single ?
            new IgniteTxRemoteSingleStateImpl() :
            new IgniteTxRemoteStateImpl(Collections.emptyMap(), U.newLinkedHashMap(txSize));

        assert topVer != null && topVer.topologyVersion() > 0 : topVer;

        topologyVersion(topVer);
    }

    /** {@inheritDoc} */
    @Override public boolean remote() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean near() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public UUID eventNodeId() {
        return nearNodeId;
    }

    /**
     * @return Near transaction ID.
     */
    @Override public GridCacheVersion nearXidVersion() {
        return nearXidVer;
    }

    /** {@inheritDoc} */
    @Override public void setPartitionUpdateCounters(long[] cntrs) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void addActiveCache(GridCacheContext cacheCtx, boolean recovery) {
        throw new UnsupportedOperationException("Near tx doesn't track active caches.");
    }

    /**
     * @return Near node ID.
     */
    public UUID nearNodeId() {
        return nearNodeId;
    }

    /** {@inheritDoc} */
    @Override public Collection<UUID> masterNodeIds() {
        Collection<UUID> res = new ArrayList<>(2);

        res.add(nodeId);
        res.add(nearNodeId);

        return res;
    }

    /**
     * @param entry Entry to enlist.
     * @throws IgniteCheckedException If failed.
     * @return {@code True} if entry was enlisted.
     */
    public boolean addEntry(IgniteTxEntry entry, ClassLoader ldr) throws IgniteCheckedException {
        entry.unmarshal(cctx, true, ldr);

        checkInternal(entry.txKey());

        GridCacheContext cacheCtx = entry.context();

        if (!cacheCtx.isNear())
            cacheCtx = cacheCtx.dht().near().context();

        GridNearCacheEntry cached = cacheCtx.near().peekExx(entry.key());

        if (cached == null) {
            return false;
        }
        else {
            try {
                cached.unswap();

                CacheObject val = cached.peek();

                if (val == null && cached.evictInternal(xidVer, null, false)) {
                    return false;
                }
                else {
                    // Initialize cache entry.
                    entry.cached(cached);

                    txState.addWriteEntry(entry.txKey(), entry);

                    addExplicit(entry);

                    return true;
                }
            }
            catch (GridCacheEntryRemovedException ignore) {
                if (log.isDebugEnabled())
                    log.debug("Got removed entry when adding to remote transaction (will ignore): " + cached);

                return false;
            }
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return GridToStringBuilder.toString(GridNearTxRemote.class, this, "super", super.toString());
    }
}
