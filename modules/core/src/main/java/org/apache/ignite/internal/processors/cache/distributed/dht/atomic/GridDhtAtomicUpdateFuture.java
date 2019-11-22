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

package org.apache.ignite.internal.processors.cache.distributed.dht.atomic;

import java.util.List;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicReferenceArray;
import javax.cache.processor.EntryProcessor;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.processors.affinity.AffinityAssignment;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheOperation;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtCacheEntry;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * DHT atomic cache backup update future.
 */
class GridDhtAtomicUpdateFuture extends GridDhtAtomicAbstractUpdateFuture {
    /** */
    private int updateCntr;

    /** */
    private final AtomicReferenceArray<GridCacheVersion> stripeVers;

    /**
     * @param cctx Cache context.
     * @param writeVer Write version.
     * @param updateReq Update request.
     */
    GridDhtAtomicUpdateFuture(
        GridCacheContext cctx,
        GridCacheVersion writeVer,
        GridNearAtomicAbstractUpdateRequest updateReq
    ) {
        super(cctx, writeVer, updateReq);

        stripeVers = new AtomicReferenceArray<>(cctx.kernalContext().getStripedExecutorService().stripes());

        mappings = U.newHashMap(updateReq.size());
    }

    /** {@inheritDoc} */
    @Override protected boolean sendAllToDht() {
        return updateCntr == updateReq.size();
    }

    /** {@inheritDoc} */
    @Override protected void addDhtKey(KeyCacheObject key, List<ClusterNode> dhtNodes) {
        assert updateCntr < updateReq.size();

        updateCntr++;
    }

    /** {@inheritDoc} */
    @Override protected void addNearKey(KeyCacheObject key, GridDhtCacheEntry.ReaderId[] readers) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override void version(GridCacheVersion ver, int stripe) {
        stripeVers.set(stripe, ver);
    }

    /** */
    GridCacheVersion[] versions() {
        GridCacheVersion[] ret = new GridCacheVersion[stripeVers.length()];

        for (int i = 0; i < stripeVers.length(); i++)
            ret[i] = stripeVers.get(i);

        return ret;
    }

    /** {@inheritDoc} */
    @Override protected GridDhtAtomicAbstractUpdateRequest createRequest(
        UUID nodeId,
        long futId,
        GridCacheVersion writeVer,
        CacheWriteSynchronizationMode syncMode,
        @NotNull AffinityTopologyVersion topVer,
        long ttl,
        long conflictExpireTime,
        @Nullable GridCacheVersion conflictVer
    ) {
        return new GridDhtAtomicUpdateRequest(
            cctx.cacheId(),
            nodeId,
            futId,
            writeVer,
            syncMode,
            topVer,
            updateReq.subjectId(),
            updateReq.taskNameHash(),
            null,
            cctx.deploymentEnabled(),
            updateReq.keepBinary(),
            updateReq.skipStore(),
            false);
    }

    /** {@inheritDoc} */
    @Override synchronized void addWriteEntry(AffinityAssignment affAssignment, GridDhtCacheEntry entry,
        @Nullable CacheObject val, EntryProcessor<Object, Object, Object> entryProcessor, long ttl,
        long conflictExpireTime, @Nullable GridCacheVersion conflictVer, boolean addPrevVal,
        @Nullable CacheObject prevVal, long updateCntr, GridCacheOperation cacheOp) {
        super.addWriteEntry(affAssignment, entry, val, entryProcessor, ttl, conflictExpireTime, conflictVer, addPrevVal, prevVal, updateCntr, cacheOp);
    }

    private Queue<Runnable> delayedEntries; // = new ConcurrentLinkedQueue<>();

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDhtAtomicUpdateFuture.class, this, "super", super.toString());
    }

    @Override protected void flushDelayed() {
        for (Runnable entry : delayedEntries)
            entry.run();

        delayedEntries = null;
    }

    /** {@inheritDoc} */
    @Override public void delayedWriteEntry(AffinityAssignment assignment, KeyCacheObject k, CacheObject newVal,
        EntryProcessor<Object, Object, Object> o, long newTtl, long conflictExpireTime,
        @Nullable GridCacheVersion newConflictVer, boolean sndPrevVal, CacheObject prevVal, long updCntr,
        GridCacheOperation op) {
        delayedEntries.add(new Runnable() {
            @Override public void run() {
                addWriteEntry2(
                    assignment,
                    k,
                    newVal,
                    o,
                    newTtl,
                    conflictExpireTime,
                    newConflictVer,
                    sndPrevVal,
                    prevVal,
                    updCntr,
                    op);
            }
        });
    }
}
