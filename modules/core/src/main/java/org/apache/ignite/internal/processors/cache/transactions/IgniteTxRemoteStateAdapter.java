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

package org.apache.ignite.internal.processors.cache.transactions;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTopologyFuture;
import org.apache.ignite.internal.processors.cache.mvcc.MvccCachingManager;
import org.apache.ignite.internal.util.GridIntList;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_ASYNC;

/**
 *
 */
public abstract class IgniteTxRemoteStateAdapter implements IgniteTxRemoteState {
    /** Active cache IDs. */
    private GridIntList activeCacheIds = new GridIntList();

    /** Cache ids used for mvcc caching. See {@link MvccCachingManager}. */
    private GridIntList mvccCachingCacheIds = new GridIntList();

    /** */
    protected boolean mvccEnabled;

    /** {@inheritDoc} */
    @Override public boolean implicitSingle() {
        return false;
    }

    /** {@inheritDoc} */
    @Nullable @Override public Integer firstCacheId() {
        return activeCacheIds.isEmpty() ? null : activeCacheIds.get(0);
    }

    /** {@inheritDoc} */
    @Nullable @Override public GridIntList cacheIds() {
        return activeCacheIds;
    }

    /** {@inheritDoc} */
    @Override public IgniteCheckedException validateTopology(
        GridCacheSharedContext cctx,
        boolean read,
        GridDhtTopologyFuture topFut
    ) {
        assert false;

        return null;
    }

    /** {@inheritDoc} */
    @Override public CacheWriteSynchronizationMode syncMode(GridCacheSharedContext cctx) {
        assert false;

        return FULL_ASYNC;
    }

    /** {@inheritDoc} */
    @Override public void addActiveCache(GridCacheContext cctx, boolean recovery, IgniteTxAdapter tx)
        throws IgniteCheckedException {
        assert !tx.local();

        int cacheId = cctx.cacheId();

        boolean mvccTx = tx.mvccSnapshot() != null;

        assert activeCacheIds.isEmpty() || mvccEnabled == mvccTx;

        mvccEnabled = mvccTx;

        // Check if we can enlist new cache to transaction.
        if (!activeCacheIds.contains(cacheId)) {
            activeCacheIds.add(cacheId);

            if (cctx.mvccEnabled() && (cctx.hasContinuousQueryListeners(tx) || cctx.isDrEnabled()))
                mvccCachingCacheIds.add(cacheId);
        }
    }

    /** {@inheritDoc} */
    @Override public GridDhtTopologyFuture topologyReadLock(GridCacheSharedContext cctx, GridFutureAdapter<?> fut) {
        assert false;

        return null;
    }

    /** {@inheritDoc} */
    @Override public void topologyReadUnlock(GridCacheSharedContext cctx) {
        assert false;
    }

    /** {@inheritDoc} */
    @Override public boolean storeWriteThrough(GridCacheSharedContext cctx) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean hasInterceptor(GridCacheSharedContext cctx) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public void onTxEnd(GridCacheSharedContext cctx, IgniteInternalTx tx, boolean commit) {
        assert false;
    }

    /** {@inheritDoc} */
    @Override public boolean mvccEnabled() {
        return mvccEnabled;
    }

    /** {@inheritDoc} */
    @Override public boolean useMvccCaching(int cacheId) {
        return mvccCachingCacheIds.contains(cacheId);
    }
}
