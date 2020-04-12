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
package org.apache.ignite.internal.processors.cache.persistence;

import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.pagemem.PageIdAllocator;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.pagemem.store.IgnitePageStoreManager;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.IgniteCacheOffheapManager;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtInvalidPartitionException;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.pendingtask.DurableBackgroundTask;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.PageMemoryEx;
import org.apache.ignite.internal.processors.cache.tree.PendingEntriesTree;
import org.apache.ignite.internal.processors.cache.tree.PendingRow;
import org.apache.ignite.internal.util.lang.GridCursor;
import org.apache.ignite.internal.util.typedef.internal.CU;

/**
 *
 */
public class RemoveCacheDataDurableBackgroundTask implements DurableBackgroundTask {
    private final int cacheId;

    private final int grpId;

    private boolean sharedGroup;

    private CacheConfiguration cacheCfg;

    /** */
    private String id;

    public RemoveCacheDataDurableBackgroundTask(CacheConfiguration cacheCfg, boolean sharedGroup) {
        this.cacheId = CU.cacheId(cacheCfg.getName());

        assert cacheId != CU.UNDEFINED_CACHE_ID;

        this.grpId = CU.cacheGroupId(cacheCfg.getName(), cacheCfg.getGroupName());
        this.sharedGroup = sharedGroup;
        this.cacheCfg = cacheCfg;
        this.id = UUID.randomUUID().toString();
    }

    /** {@inheritDoc} */
    @Override public String shortName() {
        return "REMOVE_CACHE_DATA-" + cacheCfg.getName() + "-" + id;
    }

    /** {@inheritDoc} */
    @Override public void execute(GridKernalContext ctx) {
        try {
            if (sharedGroup)
                removeCacheFromSharedGroup(ctx);
            else
                removeCacheGroup(ctx);
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }

    private void removeCacheFromSharedGroup(GridKernalContext ctx) throws IgniteCheckedException {
        CacheGroupContext grpCtx = ctx.cache().cacheGroup(grpId);

        if (grpCtx == null)
            throw new IgniteException("Group with id=" + grpId + " could not be found.");

        ctx.cache().onCacheDataRemoveStarted(cacheCfg.getName());

        ctx.cache().context().database().checkpointReadLock();

        try {
            removeFromCacheDataStores(grpCtx);

            ctx.cache().onCacheDataRemoveFinished(cacheCfg.getName());
        } finally {
            ctx.cache().context().database().checkpointReadUnlock();
        }
    }

    private void clearPendingEntries(PendingEntriesTree pendingEntries) throws IgniteCheckedException {
        if (pendingEntries != null) {
            PendingRow row = new PendingRow(cacheId);

            GridCursor<PendingRow> cursor = pendingEntries.find(row, row, PendingEntriesTree.WITHOUT_KEY);

            while (cursor.next()) {
                boolean res = pendingEntries.removex(cursor.get());

                assert res;
            }
        }

    }

    private void removeFromCacheDataStores(CacheGroupContext grpCtx) {
        Exception ex = null;

        for (int i = 0; i < cacheCfg.getAffinity().partitions(); i++) {
            GridDhtLocalPartition partition = null;

            try {
                partition = grpCtx.topology().localPartition(i);
            } catch (GridDhtInvalidPartitionException e) {
                /* No op. */
            }

            // This means that partition is not local.
            if (partition == null)
                continue;

            try {
                IgniteCacheOffheapManager.CacheDataStore dataStore = grpCtx.offheap().dataStore(partition);

                dataStore.clear(cacheId);

                clearPendingEntries(dataStore.pendingTree());
            } catch (IgniteCheckedException e) {
                if (ex == null)
                    ex = e;
                else
                    ex.addSuppressed(e);
            }
        }

        if (ex != null)
            throw new IgniteException("Errors occured while destroying cache data store", ex);
    }

    private void removeCacheGroup(GridKernalContext ctx) throws IgniteCheckedException {
        IgnitePageStoreManager pageStore = ctx.cache().context().pageStore();

        // Page store is null when persistence is not enabled.
        if (pageStore == null)
            return;

        DataRegion dataRegion = ctx.cache().context().database().dataRegion(cacheCfg.getDataRegionName());

        // Invalidating page memory for cache group, if region was found.
        if (dataRegion != null) {
            PageMemory pageMem = dataRegion.pageMemory();

            if (pageMem instanceof PageMemoryEx) {
                PageMemoryEx pageMemEx = (PageMemoryEx) pageMem;

                for (int partId = 0; partId < cacheCfg.getAffinity().partitions(); partId++)
                    pageMemEx.invalidate(grpId, partId);

                pageMemEx.invalidate(grpId, PageIdAllocator.INDEX_PARTITION);
            }
        }

        pageStore.cleanupPersistentSpace(cacheCfg);
    }
}
