/*
 * Copyright 2021 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.internal.processors.cache;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.function.ToIntFunction;
import javax.cache.Cache;
import javax.cache.processor.EntryProcessor;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.failure.FailureType;
import org.apache.ignite.internal.NodeStoppingException;
import org.apache.ignite.internal.metric.IoStatisticsHolder;
import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.pagemem.wal.record.delta.DataPageMvccMarkUpdatedRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.DataPageMvccUpdateNewTxStateHintRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.DataPageMvccUpdateTxStateHintRecord;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.distributed.dht.colocated.GridDhtDetachedCacheEntry;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.CachePartitionPartialCountersMap;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsExchangeFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.IgniteDhtDemandedPartitionsMap;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.IgniteHistoricalIterator;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.IgniteRebalanceIteratorImpl;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtInvalidPartitionException;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.mvcc.MvccSnapshot;
import org.apache.ignite.internal.processors.cache.mvcc.MvccSnapshotWithoutTxs;
import org.apache.ignite.internal.processors.cache.mvcc.MvccUtils;
import org.apache.ignite.internal.processors.cache.mvcc.MvccVersion;
import org.apache.ignite.internal.processors.cache.mvcc.txlog.TxState;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRowAdapter;
import org.apache.ignite.internal.processors.cache.persistence.CacheSearchRow;
import org.apache.ignite.internal.processors.cache.persistence.RootPage;
import org.apache.ignite.internal.processors.cache.persistence.RowStore;
import org.apache.ignite.internal.processors.cache.persistence.freelist.SimpleDataRow;
import org.apache.ignite.internal.processors.cache.persistence.partstate.GroupPartitionId;
import org.apache.ignite.internal.processors.cache.persistence.partstorage.PartitionMetaStorage;
import org.apache.ignite.internal.processors.cache.persistence.tree.BPlusTree;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.DataPageIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.reuse.ReuseList;
import org.apache.ignite.internal.processors.cache.persistence.tree.util.PageHandler;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryManager;
import org.apache.ignite.internal.processors.cache.tree.CacheDataRowStore;
import org.apache.ignite.internal.processors.cache.tree.CacheDataTree;
import org.apache.ignite.internal.processors.cache.tree.DataRow;
import org.apache.ignite.internal.processors.cache.tree.PendingEntriesTree;
import org.apache.ignite.internal.processors.cache.tree.PendingRow;
import org.apache.ignite.internal.processors.cache.tree.RowLinkIO;
import org.apache.ignite.internal.processors.cache.tree.SearchRow;
import org.apache.ignite.internal.processors.cache.tree.mvcc.data.MvccDataRow;
import org.apache.ignite.internal.processors.cache.tree.mvcc.data.MvccUpdateDataRow;
import org.apache.ignite.internal.processors.cache.tree.mvcc.data.MvccUpdateResult;
import org.apache.ignite.internal.processors.cache.tree.mvcc.data.ResultType;
import org.apache.ignite.internal.processors.cache.tree.mvcc.search.MvccDataPageClosure;
import org.apache.ignite.internal.processors.cache.tree.mvcc.search.MvccFirstRowTreeClosure;
import org.apache.ignite.internal.processors.cache.tree.mvcc.search.MvccLinkAwareSearchRow;
import org.apache.ignite.internal.processors.cache.tree.mvcc.search.MvccMaxSearchRow;
import org.apache.ignite.internal.processors.cache.tree.mvcc.search.MvccMinSearchRow;
import org.apache.ignite.internal.processors.cache.tree.mvcc.search.MvccSnapshotSearchRow;
import org.apache.ignite.internal.processors.cache.tree.mvcc.search.MvccTreeClosure;
import org.apache.ignite.internal.processors.cache.tree.updatelog.PartitionLogTree;
import org.apache.ignite.internal.processors.cache.tree.updatelog.UpdateLogRow;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.query.GridQueryRowCacheCleaner;
import org.apache.ignite.internal.transactions.IgniteTxUnexpectedStateCheckedException;
import org.apache.ignite.internal.util.GridAtomicLong;
import org.apache.ignite.internal.util.GridCloseableIteratorAdapter;
import org.apache.ignite.internal.util.GridEmptyCloseableIterator;
import org.apache.ignite.internal.util.GridLongList;
import org.apache.ignite.internal.util.GridSpinBusyLock;
import org.apache.ignite.internal.util.GridStripedLock;
import org.apache.ignite.internal.util.IgniteTree;
import org.apache.ignite.internal.util.collection.ImmutableIntSet;
import org.apache.ignite.internal.util.collection.IntMap;
import org.apache.ignite.internal.util.collection.IntRWHashMap;
import org.apache.ignite.internal.util.collection.IntSet;
import org.apache.ignite.internal.util.lang.GridCloseableIterator;
import org.apache.ignite.internal.util.lang.GridCursor;
import org.apache.ignite.internal.util.lang.GridIterator;
import org.apache.ignite.internal.util.lang.IgniteClosure2X;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgnitePredicate;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static java.lang.Boolean.TRUE;
import static org.apache.ignite.IgniteSystemProperties.getBoolean;
import static org.apache.ignite.internal.pagemem.PageIdAllocator.FLAG_IDX;
import static org.apache.ignite.internal.pagemem.PageIdAllocator.INDEX_PARTITION;
import static org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState.OWNING;
import static org.apache.ignite.internal.processors.cache.mvcc.MvccUtils.INITIAL_VERSION;
import static org.apache.ignite.internal.processors.cache.mvcc.MvccUtils.MVCC_COUNTER_NA;
import static org.apache.ignite.internal.processors.cache.mvcc.MvccUtils.MVCC_CRD_COUNTER_NA;
import static org.apache.ignite.internal.processors.cache.mvcc.MvccUtils.MVCC_HINTS_BIT_OFF;
import static org.apache.ignite.internal.processors.cache.mvcc.MvccUtils.MVCC_KEY_ABSENT_BEFORE_OFF;
import static org.apache.ignite.internal.processors.cache.mvcc.MvccUtils.MVCC_OP_COUNTER_MASK;
import static org.apache.ignite.internal.processors.cache.mvcc.MvccUtils.MVCC_OP_COUNTER_NA;
import static org.apache.ignite.internal.processors.cache.mvcc.MvccUtils.compare;
import static org.apache.ignite.internal.processors.cache.mvcc.MvccUtils.compareNewVersion;
import static org.apache.ignite.internal.processors.cache.mvcc.MvccUtils.isVisible;
import static org.apache.ignite.internal.processors.cache.mvcc.MvccUtils.mvccVersionIsValid;
import static org.apache.ignite.internal.processors.cache.mvcc.MvccUtils.state;
import static org.apache.ignite.internal.processors.cache.mvcc.MvccUtils.unexpectedStateException;
import static org.apache.ignite.internal.processors.cache.persistence.CacheDataRowAdapter.RowData.FULL_WITH_HINTS;
import static org.apache.ignite.internal.processors.cache.persistence.tree.io.DataPageIO.MVCC_INFO_SIZE;
import static org.apache.ignite.internal.util.IgniteTree.OperationType.NOOP;
import static org.apache.ignite.internal.util.IgniteTree.OperationType.PUT;
import static org.apache.ignite.internal.util.lang.GridCursor.EMPTY_CURSOR;

/**
 *
 */
public class IgniteCacheOffheapManagerImpl implements IgniteCacheOffheapManager {
    /** Batch size for cache removals during destroy. */
    private static final int BATCH_SIZE = 1000;

    /** */
    protected GridCacheSharedContext ctx;

    /** */
    protected CacheGroupContext grp;

    /** */
    protected IgniteLogger log;

    /** Cache data store for <tt>LOCAL</tt> caches only. */
    private CacheDataStore locCacheDataStore;

    /** */
    private volatile PendingEntriesTree pendingEntries;

    /** */
    private final GridAtomicLong globalRmvId = new GridAtomicLong(U.currentTimeMillis() * 1000_000);

    /** */
    protected final GridSpinBusyLock busyLock = new GridSpinBusyLock();

    /** */
    protected GridStripedLock partStoreLock = new GridStripedLock(Runtime.getRuntime().availableProcessors());

    /** {@inheritDoc} */
    @Override public GridAtomicLong globalRemoveId() {
        return globalRmvId;
    }

    /** {@inheritDoc} */
    @Override public void start(GridCacheSharedContext ctx, CacheGroupContext grp) throws IgniteCheckedException {
        this.ctx = ctx;
        this.grp = grp;
        this.log = ctx.logger(getClass());

        if (grp.affinityNode()) {
            ctx.database().checkpointReadLock();

            try {
                initDataStructures();

                if (grp.isLocal())
                    locCacheDataStore = createCacheDataStore(0);
            }
            finally {
                ctx.database().checkpointReadUnlock();
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void onCacheStarted(GridCacheContext cctx) throws IgniteCheckedException {
        initPendingTree(cctx);
    }

    /**
     * @param cctx Cache context.
     * @throws IgniteCheckedException If failed.
     */
    protected void initPendingTree(GridCacheContext<?, ?> cctx) throws IgniteCheckedException {
        assert !cctx.group().persistenceEnabled();

        if (cctx.affinityNode() && pendingEntries == null) {
            String pendingEntriesTreeName = cctx.name() + "##PendingEntries";

            long rootPage = allocateForTree();

            pendingEntries = new PendingEntriesTree(
                grp,
                pendingEntriesTreeName,
                grp.dataRegion().pageMemory(),
                rootPage,
                grp.reuseList(),
                true,
                ctx.diagnostic().pageLockTracker(),
                FLAG_IDX
            );
        }
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    protected void initDataStructures() throws IgniteCheckedException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void stopCache(int cacheId, boolean destroy) {
        if (destroy && grp.affinityNode())
            removeCacheData(cacheId);
    }

    /** {@inheritDoc} */
    @Override public void stop() {
        try {
            for (CacheDataStore store : cacheDataStores())
                destroyCacheDataStore(store);

            if (pendingEntries != null)
                pendingEntries.destroy();
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e.getMessage(), e);
        }
    }

    /** {@inheritDoc} */
    @Override public long restoreStateOfPartition(int p, @Nullable Integer recoveryState) throws IgniteCheckedException {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public void restorePartitionStates(Map<GroupPartitionId, Integer> partRecoveryStates) throws IgniteCheckedException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void confirmPartitionStatesRestored() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void onKernalStop() {
        busyLock.block();
    }

    /**
     * @param cacheId Cache ID.
     */
    private void removeCacheData(int cacheId) {
        assert grp.affinityNode();

        try {
            if (grp.sharedGroup()) {
                assert cacheId != CU.UNDEFINED_CACHE_ID;

                for (CacheDataStore store : cacheDataStores())
                    store.clear(cacheId);

                // Clear non-persistent pending tree if needed.
                if (pendingEntries != null) {
                    PendingRow row = new PendingRow(cacheId);

                    GridCursor<PendingRow> cursor = pendingEntries.find(row, row, PendingEntriesTree.WITHOUT_KEY);

                    while (cursor.next()) {
                        boolean res = pendingEntries.removex(cursor.get());

                        assert res;
                    }
                }
            }
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e.getMessage(), e);
        }
    }

    /**
     * @param cctx Cache context.
     * @param key Key.
     * @return Data store.
     */
    @Nullable private CacheDataStore dataStore(GridCacheContext<?, ?> cctx, KeyCacheObject key) {
        if (grp.isLocal())
            return locCacheDataStore;

        return dataStore(cctx.affinity().partition(key), false);
    }

    /**
     * @param part Partition.
     * @return Data store for given entry.
     */
    @Override public CacheDataStore dataStore(@Nullable GridDhtLocalPartition part) {
        if (grp.isLocal())
            return locCacheDataStore;

        assert part != null;

        return part.dataStore();
    }

    /**
     * @param partId Partition.
     * @return Data store for given entry.
     */
    @Nullable public CacheDataStore dataStore(int partId, boolean includeRenting) {
        if (grp.isLocal())
            return locCacheDataStore;

        GridDhtLocalPartition part = grp.topology().localPartition(partId, AffinityTopologyVersion.NONE, false, includeRenting);

        return part == null ? null : part.dataStore();
    }

    /** {@inheritDoc} */
    @Override public long cacheEntriesCount(int cacheId) {
        long size = 0;

        for (CacheDataStore store : cacheDataStores())
            size += store.cacheSize(cacheId);

        return size;
    }

    /** {@inheritDoc} */
    @Override public long tombstonesCount() {
        if (!grp.supportsTombstone())
            return 0;
        else {
            long size = 0;

            for (CacheDataStore store : cacheDataStores())
                size += store.tombstonesCount();

            return size;
        }
    }

    /** {@inheritDoc} */
    @Override public void preloadPartition(int p) throws IgniteCheckedException {
        throw new IgniteCheckedException("Operation only applicable to caches with enabled persistence");
    }

    /** {@inheritDoc} */
    @Override public void removePendingRow(PendingRow row) throws IgniteCheckedException {
        CacheDataStore store = dataStore(row.key.partition(), true);

        if (store != null)
            store.pendingTree().remove(row);
    }

    /** {@inheritDoc} */
    @Override public long cacheEntriesCount(
        int cacheId,
        boolean primary,
        boolean backup,
        AffinityTopologyVersion topVer
    ) throws IgniteCheckedException {
        if (grp.isLocal())
            if (primary)
                return cacheEntriesCount(cacheId, 0);
            else
                return 0L;
        else {
            long cnt = 0;

            Iterator<CacheDataStore> it = cacheData(primary, backup, topVer);

            while (it.hasNext())
                cnt += it.next().cacheSize(cacheId);

            return cnt;
        }
    }

    /** {@inheritDoc} */
    @Override public long cacheEntriesCount(int cacheId, int part) {
        CacheDataStore store = dataStore(part, true);

        return store == null ? 0 : store.cacheSize(cacheId);
    }

    /** {@inheritDoc} */
    @Override public Iterable<CacheDataStore> cacheDataStores() {
        return cacheDataStores(F.alwaysTrue());
    }

    /**
     * @param filter Filtering predicate.
     * @return Iterable over all existing cache data stores except which one is marked as <tt>destroyed</tt>.
     */
    private Iterable<CacheDataStore> cacheDataStores(
        IgnitePredicate<GridDhtLocalPartition> filter
    ) {
        return grp.isLocal() ? Collections.singletonList(locCacheDataStore) :
            F.iterator(grp.topology().currentLocalPartitions(), GridDhtLocalPartition::dataStore, true,
                filter, p -> !p.dataStore().destroyed());
    }

    /**
     * @param primary Primary data flag.
     * @param backup Primary data flag.
     * @param topVer Topology version.
     * @return Data stores iterator.
     */
    private Iterator<CacheDataStore> cacheData(boolean primary, boolean backup, AffinityTopologyVersion topVer) {
        assert primary || backup;

        if (grp.isLocal())
            return singletonIterator(locCacheDataStore);

        IgnitePredicate<GridDhtLocalPartition> filter;

        if (primary && backup)
            filter = F.alwaysTrue();
        else {
            IntSet parts = ImmutableIntSet.wrap(primary ? grp.affinity().primaryPartitions(ctx.localNodeId(), topVer) :
                grp.affinity().backupPartitions(ctx.localNodeId(), topVer));

            filter = part -> parts.contains(part.id());
        }

        return cacheDataStores(filter).iterator();
    }

    /** {@inheritDoc} */
    @Override public void invoke(
        GridCacheContext cctx,
        KeyCacheObject key,
        GridDhtLocalPartition part,
        OffheapInvokeClosure c
    ) throws IgniteCheckedException {
        dataStore(part).invoke(cctx, key, c);
    }

    /** {@inheritDoc} */
    @Override public void update(
        GridCacheContext cctx,
        KeyCacheObject key,
        CacheObject val,
        GridCacheVersion ver,
        long expireTime,
        GridDhtLocalPartition part,
        @Nullable CacheDataRow oldRow
    ) throws IgniteCheckedException {
        assert expireTime >= 0;

        dataStore(part).update(cctx, key, val, ver, expireTime, oldRow);
    }

    /** {@inheritDoc} */
    @Override public boolean mvccInitialValue(
        GridCacheMapEntry entry,
        CacheObject val,
        GridCacheVersion ver,
        long expireTime,
        MvccVersion mvccVer,
        MvccVersion newMvccVer) throws IgniteCheckedException {
        return dataStore(entry.localPartition()).mvccInitialValue(
            entry.context(),
            entry.key(),
            val,
            ver,
            expireTime,
            mvccVer,
            newMvccVer);
    }

    /** {@inheritDoc} */
    @Override public boolean mvccApplyHistoryIfAbsent(GridCacheMapEntry entry, List<GridCacheMvccEntryInfo> hist)
        throws IgniteCheckedException {
        return dataStore(entry.localPartition()).mvccApplyHistoryIfAbsent(entry.cctx, entry.key(), hist);
    }

    /** {@inheritDoc} */
    @Override public boolean mvccUpdateRowWithPreloadInfo(
        GridCacheMapEntry entry,
        @Nullable CacheObject val,
        GridCacheVersion ver,
        long expireTime,
        MvccVersion mvccVer,
        MvccVersion newMvccVer,
        byte mvccTxState,
        byte newMvccTxState
    ) throws IgniteCheckedException {
        assert entry.lockedByCurrentThread();

        return dataStore(entry.localPartition()).mvccUpdateRowWithPreloadInfo(
            entry.context(),
            entry.key(),
            val,
            ver,
            expireTime,
            mvccVer,
            newMvccVer,
            mvccTxState,
            newMvccTxState
        );
    }

    /** {@inheritDoc} */
    @Override public MvccUpdateResult mvccUpdate(
        GridCacheMapEntry entry,
        CacheObject val,
        GridCacheVersion ver,
        long expireTime,
        MvccSnapshot mvccSnapshot,
        boolean primary,
        boolean needHistory,
        boolean noCreate,
        boolean needOldVal,
        @Nullable CacheEntryPredicate filter,
        boolean retVal,
        boolean keepBinary,
        EntryProcessor entryProc,
        Object[] invokeArgs) throws IgniteCheckedException {
        if (entry.detached() || entry.isNear())
            return null;

        assert entry.lockedByCurrentThread();

        return dataStore(entry.localPartition()).mvccUpdate(entry.context(),
            entry.key(),
            val,
            ver,
            expireTime,
            mvccSnapshot,
            filter,
            entryProc,
            invokeArgs,
            primary,
            needHistory,
            noCreate,
            needOldVal,
            retVal,
            keepBinary);
    }

    /** {@inheritDoc} */
    @Override public MvccUpdateResult mvccRemove(
        GridCacheMapEntry entry,
        MvccSnapshot mvccSnapshot,
        boolean primary,
        boolean needHistory,
        boolean needOldVal,
        @Nullable CacheEntryPredicate filter,
        boolean retVal) throws IgniteCheckedException {
        if (entry.detached() || entry.isNear())
            return null;

        assert entry.lockedByCurrentThread();

        return dataStore(entry.localPartition()).mvccRemove(entry.context(),
            entry.key(),
            mvccSnapshot,
            filter,
            primary,
            needHistory,
            needOldVal,
            retVal);
    }

    /** {@inheritDoc} */
    @Override public void mvccRemoveAll(GridCacheMapEntry entry) throws IgniteCheckedException {
        if (entry.detached() || entry.isNear())
            return;

        dataStore(entry.localPartition()).mvccRemoveAll(entry.context(), entry.key());
    }

    /** {@inheritDoc} */
    @Nullable @Override public MvccUpdateResult mvccLock(GridCacheMapEntry entry,
        MvccSnapshot mvccSnapshot) throws IgniteCheckedException {
        if (entry.detached() || entry.isNear())
            return null;

        assert entry.lockedByCurrentThread();

        return dataStore(entry.localPartition()).mvccLock(entry.context(), entry.key(), mvccSnapshot);
    }

    /** {@inheritDoc} */
    @Override public void mvccApplyUpdate(
        GridCacheContext cctx,
        KeyCacheObject key,
        CacheObject val,
        GridCacheVersion ver,
        long expireTime,
        GridDhtLocalPartition part,
        MvccVersion mvccVer) throws IgniteCheckedException {

        dataStore(part).mvccApplyUpdate(cctx,
            key,
            val,
            ver,
            expireTime,
            mvccVer);
    }

    /** {@inheritDoc} */
    @Override public void remove(
        GridCacheContext cctx,
        KeyCacheObject key,
        int partId,
        GridDhtLocalPartition part
    ) throws IgniteCheckedException {
        // part can be null for local cache.
        assert !cctx.isNear();

        dataStore(part).remove(cctx, key, partId);
    }

    /** {@inheritDoc} */
    @Override public void removeWithTombstone(
        GridCacheContext cctx,
        KeyCacheObject key,
        GridCacheVersion ver,
        GridDhtLocalPartition part
    ) throws IgniteCheckedException {
        assert part != null; // Tombstones are not written for local cache.
        assert !cctx.isNear();
        assert !cctx.isLocal();

        dataStore(part).removeWithTombstone(cctx, key, ver, part);
    }

    /** {@inheritDoc} */
    @Override @Nullable public CacheDataRow read(GridCacheMapEntry entry)
        throws IgniteCheckedException {
        KeyCacheObject key = entry.key();

        assert grp.isLocal() || entry.localPartition() != null : entry;

        return dataStore(entry.localPartition()).find(entry.context(), key);
    }

    /** {@inheritDoc} */
    @Nullable @Override public CacheDataRow read(GridCacheContext cctx, KeyCacheObject key)
        throws IgniteCheckedException {
        CacheDataStore dataStore = dataStore(cctx, key);

        CacheDataRow row = dataStore != null ? dataStore.find(cctx, key) : null;

        if (row != null && row.tombstone())
            row = null;

        assert row == null || row.value() != null : row;

        return row;
    }

    /** {@inheritDoc} */
    @Nullable @Override public CacheDataRow mvccRead(GridCacheContext cctx, KeyCacheObject key, MvccSnapshot mvccSnapshot)
        throws IgniteCheckedException {
        assert mvccSnapshot != null;

        CacheDataStore dataStore = dataStore(cctx, key);

        CacheDataRow row = dataStore != null ? dataStore.mvccFind(cctx, key, mvccSnapshot) : null;

        assert row == null || row.value() != null : row;

        return row;
    }

    /** {@inheritDoc} */
    @Override public List<IgniteBiTuple<Object, MvccVersion>> mvccAllVersions(GridCacheContext cctx, KeyCacheObject key)
        throws IgniteCheckedException {
        CacheDataStore dataStore = dataStore(cctx, key);

        return dataStore != null ? dataStore.mvccFindAllVersions(cctx, key) :
            Collections.emptyList();
    }

    /** {@inheritDoc} */
    @Override public GridCursor<CacheDataRow> mvccAllVersionsCursor(GridCacheContext cctx,
        KeyCacheObject key, CacheDataRowAdapter.RowData x) throws IgniteCheckedException {
        CacheDataStore dataStore = dataStore(cctx, key);

        return dataStore != null ? dataStore.mvccAllVersionsCursor(cctx, key, x) : EMPTY_CURSOR;
    }

    /**
     * Clears offheap entries.
     *
     * @param readers {@code True} to clear readers.
     */
    @Override public void clearCache(GridCacheContext cctx, boolean readers) {
        GridCacheVersion obsoleteVer = cctx.versions().startVersion();

        // Local clearing should skip existing tombstones to avoid unexpected tombstone removal.
        try (GridCloseableIterator<CacheDataRow> it = grp.isLocal() ?
            iterator(cctx.cacheId(), cacheDataStores().iterator(), null, null, DATA) :
            evictionSafeIterator(cctx.cacheId(), cacheDataStores().iterator(), DATA)) {
            while (it.hasNext()) {
                cctx.shared().database().checkpointReadLock();

                try {
                    CacheDataRow row = it.next();

                    KeyCacheObject key = row.key();

                    try {
                        GridCacheEntryEx entry = cctx.cache().entryEx(key);

                        // Need unswapped entry for clear method to check if it's a tombstone.
                        entry.unswap(row);

                        entry.clear(obsoleteVer, readers);
                    }
                    catch (GridDhtInvalidPartitionException | GridCacheEntryRemovedException ignore) {
                        // Ignore.
                    }
                    catch (IgniteCheckedException e) {
                        U.error(log, "Failed to clear cache entry: " + key, e);
                    }
                }
                finally {
                    cctx.shared().database().checkpointReadUnlock();
                }
            }
        }
        catch (IgniteCheckedException e) {
            U.error(log, "Failed to close iterator", e);
        }
    }

    /** {@inheritDoc} */
    @Override public int onUndeploy(ClassLoader ldr) {
        // TODO: GG-11141.
        return 0;
    }

    /** {@inheritDoc} */
    @Override public long offHeapAllocatedSize() {
        // TODO GG-10884.
        return 0;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public <K, V> GridCloseableIterator<Cache.Entry<K, V>> cacheEntriesIterator(
        GridCacheContext cctx,
        boolean primary,
        boolean backup,
        AffinityTopologyVersion topVer,
        boolean keepBinary,
        @Nullable MvccSnapshot mvccSnapshot,
        Boolean dataPageScanEnabled
    ) {
        Iterator<CacheDataRow> it = cacheIterator(cctx.cacheId(), primary, backup,
            topVer, mvccSnapshot, dataPageScanEnabled);

        return new GridCloseableIteratorAdapter<Cache.Entry<K, V>>() {
            /** */
            private CacheEntryImplEx next;

            @Override protected Cache.Entry<K, V> onNext() {
                CacheEntryImplEx ret = next;

                next = null;

                return ret;
            }

            @Override protected boolean onHasNext() {
                if (next != null)
                    return true;

                CacheDataRow nextRow = null;

                if (it.hasNext())
                    nextRow = it.next();

                if (nextRow != null) {
                    KeyCacheObject key = nextRow.key();
                    CacheObject val = nextRow.value();

                    Object key0 = cctx.unwrapBinaryIfNeeded(key, keepBinary, false, null);
                    Object val0 = cctx.unwrapBinaryIfNeeded(val, keepBinary, false, null);

                    next = new CacheEntryImplEx(key0, val0, nextRow.version());

                    return true;
                }

                return false;
            }
        };
    }

    /** {@inheritDoc} */
    @Override public GridCloseableIterator<KeyCacheObject> cacheKeysIterator(int cacheId, int part)
        throws IgniteCheckedException {
        CacheDataStore data = dataStore(part, true);

        if (data == null)
            return new GridEmptyCloseableIterator<>();

        GridCursor<? extends CacheDataRow> cur =
            data.cursor(cacheId, null, null, CacheDataRowAdapter.RowData.KEY_ONLY);

        return new GridCloseableIteratorAdapter<KeyCacheObject>() {
            /** */
            private KeyCacheObject next;

            @Override protected KeyCacheObject onNext() {
                KeyCacheObject res = next;

                next = null;

                return res;
            }

            @Override protected boolean onHasNext() throws IgniteCheckedException {
                if (next != null)
                    return true;

                if (cur.next()) {
                    CacheDataRow row = cur.get();

                    next = row.key();
                }

                return next != null;
            }
        };
    }

    /** {@inheritDoc} */
    @Override public GridIterator<CacheDataRow> cacheIterator(
        int cacheId,
        boolean primary,
        boolean backups,
        AffinityTopologyVersion topVer,
        @Nullable MvccSnapshot mvccSnapshot,
        Boolean dataPageScanEnabled
    ) {
        return iterator(cacheId, cacheData(primary, backups, topVer), mvccSnapshot, dataPageScanEnabled, DATA);
    }

    /** {@inheritDoc} */
    @Override public GridIterator<CacheDataRow> cachePartitionIterator(int cacheId, int part,
        @Nullable MvccSnapshot mvccSnapshot, Boolean dataPageScanEnabled) {
        CacheDataStore data = dataStore(part, true);

        if (data == null)
            return new GridEmptyCloseableIterator<>();

        return iterator(cacheId, singletonIterator(data), mvccSnapshot, dataPageScanEnabled, DATA);
    }

    /** {@inheritDoc} */
    @Override public GridIterator<CacheDataRow> partitionIterator(int part, int flags) {
        CacheDataStore data = dataStore(part, true);

        if (data == null)
            return new GridEmptyCloseableIterator<>();

        return iterator(CU.UNDEFINED_CACHE_ID, singletonIterator(data), null, null, flags);
    }

    /**
     *
     * @param cacheId Cache ID.
     * @param dataIt Data store iterator.
     * @param mvccSnapshot Mvcc snapshot.
     * @param dataPageScanEnabled Flag to enable data page scan.
     * @param flags Scan flags.
     * @return Rows iterator
     */
    private GridCloseableIterator<CacheDataRow> iterator(int cacheId,
        Iterator<CacheDataStore> dataIt,
        MvccSnapshot mvccSnapshot,
        Boolean dataPageScanEnabled,
        int flags
    ) {
        return new GridCloseableIteratorAdapter<CacheDataRow>() {
            /** */
            private GridCursor<? extends CacheDataRow> cur;

            /** */
            private int curPart;

            /** */
            private CacheDataRow next;

            @Override protected CacheDataRow onNext() {
                CacheDataRow res = next;

                next = null;

                return res;
            }

            @Override protected boolean onHasNext() throws IgniteCheckedException {
                if (next != null)
                    return true;

                while (true) {
                    try {
                        if (cur == null) {
                            if (dataIt.hasNext()) {
                                CacheDataStore ds = dataIt.next();

                                curPart = ds.partId();

                                // Data page scan is disabled by default for scan queries.
                                // TODO https://ggsystems.atlassian.net/browse/GG-20800
                                CacheDataTree.setDataPageScanEnabled(false);

                                try {
                                    if (mvccSnapshot == null)
                                        cur = cacheId == CU.UNDEFINED_CACHE_ID ? ds.cursor(flags) : ds.cursor(cacheId, flags);
                                    else {
                                        cur = cacheId == CU.UNDEFINED_CACHE_ID ?
                                            ds.cursor(mvccSnapshot) : ds.cursor(cacheId, mvccSnapshot);
                                    }
                                }
                                finally {
                                    CacheDataTree.setDataPageScanEnabled(false);
                                }
                            }
                            else
                                break;
                        }

                        if (cur.next()) {
                            next = cur.get();
                            next.key().partition(curPart);

                            break;
                        }
                        else
                            cur = null;
                    }
                    catch (IgniteCheckedException ex) {
                        throw new IgniteCheckedException("Failed to get next data row due to underlying cursor " +
                            "invalidation", ex);
                    }
                }

                return next != null;
            }
        };
    }

    /**
     * @param cacheId Cache ID.
     * @param dataIt Data store iterator.
     * @param flags Scan flags.
     * @return Rows iterator
     */
    private GridCloseableIterator<CacheDataRow> evictionSafeIterator(
        int cacheId,
        Iterator<CacheDataStore> dataIt,
        int flags
    ) {
        return new GridCloseableIteratorAdapter<CacheDataRow>() {
            /** */
            private GridCursor<? extends CacheDataRow> cur;

            /** */
            private GridDhtLocalPartition curPart;

            /** */
            private CacheDataRow next;

            @Override protected CacheDataRow onNext() {
                CacheDataRow res = next;

                next = null;

                return res;
            }

            @Override protected boolean onHasNext() throws IgniteCheckedException {
                if (next != null)
                    return true;

                while (true) {
                    if (cur == null) {
                        if (dataIt.hasNext()) {
                            CacheDataStore ds = dataIt.next();

                            if (!reservePartition(ds.partId()))
                                continue;

                            cur = cacheId == CU.UNDEFINED_CACHE_ID ? ds.cursor(flags) : ds.cursor(cacheId, flags);
                        }
                        else
                            break;
                    }

                    if (cur.next()) {
                        next = cur.get();
                        next.key().partition(curPart.id());

                        break;
                    }
                    else {
                        cur = null;

                        releaseCurrentPartition();
                    }
                }

                return next != null;
            }

            /** */
            private void releaseCurrentPartition() {
                GridDhtLocalPartition p = curPart;

                assert p != null;

                curPart = null;

                p.release();
            }

            /**
             * @param partId Partition number.
             * @return {@code True} if partition was reserved.
             */
            private boolean reservePartition(int partId) {
                GridDhtLocalPartition p = grp.topology().localPartition(partId);

                if (p != null && p.reserve()) {
                    curPart = p;

                    return true;
                }

                return false;
            }

            /** {@inheritDoc} */
            @Override protected void onClose() throws IgniteCheckedException {
                if (curPart != null)
                    releaseCurrentPartition();
            }
        };
    }

    /**
     * @param item Item.
     * @return Single item iterator.
     * @param <T> Type of item.
     */
    private <T> Iterator<T> singletonIterator(T item) {
        return new Iterator<T>() {
            /** */
            private boolean hasNext = true;

            /** {@inheritDoc} */
            @Override public boolean hasNext() {
                return hasNext;
            }

            /** {@inheritDoc} */
            @Override public T next() {
                if (hasNext) {
                    hasNext = false;

                    return item;
                }

                throw new NoSuchElementException();
            }

            /** {@inheritDoc} */
            @Override public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

    /**
     * @return Page ID.
     * @throws IgniteCheckedException If failed.
     */
    private long allocateForTree() throws IgniteCheckedException {
        ReuseList reuseList = grp.reuseList();

        long pageId;

        if (reuseList == null || (pageId = reuseList.takeRecycledPage()) == 0L)
            pageId = grp.dataRegion().pageMemory().allocatePage(grp.groupId(), INDEX_PARTITION, FLAG_IDX);

        return pageId;
    }

    /** {@inheritDoc} */
    @Override public RootPage rootPageForIndex(int cacheId, String idxName, int segment) throws IgniteCheckedException {
        long pageId = allocateForTree();

        return new RootPage(new FullPageId(pageId, grp.groupId()), true);
    }

    /** {@inheritDoc} */
    @Override public @Nullable RootPage findRootPageForIndex(int cacheId, String idxName, int segment) throws IgniteCheckedException {
        return null; // No-op.
    }

    /** {@inheritDoc} */
    @Override public @Nullable RootPage dropRootPageForIndex(
        int cacheId,
        String idxName,
        int segment
    ) throws IgniteCheckedException {
        return null; // No-op.
    }

    /** {@inheritDoc} */
    @Override public @Nullable RootPage renameRootPageForIndex(
        int cacheId,
        String oldIdxName,
        String newIdxName,
        int segment
    ) throws IgniteCheckedException {
        return null; // No-op.
    }

    /** {@inheritDoc} */
    @Override public ReuseList reuseListForIndex(String idxName) {
        return grp.reuseList();
    }

    /** {@inheritDoc} */
    @Override public GridCloseableIterator<CacheDataRow> reservedIterator(int part, AffinityTopologyVersion topVer) {
        GridDhtLocalPartition loc = grp.topology().localPartition(part, topVer, false);

        if (loc == null || !loc.reserve())
            return null;

        // It is necessary to check state after reservation to avoid race conditions.
        if (loc.state() != OWNING) {
            loc.release();

            return null;
        }

        CacheDataStore data = dataStore(loc);

        return new GridCloseableIteratorAdapter<CacheDataRow>() {
            /** */
            private CacheDataRow next;

            /** */
            private GridCursor<? extends CacheDataRow> cur;

            @Override protected CacheDataRow onNext() {
                CacheDataRow res = next;

                next = null;

                return res;
            }

            @Override protected boolean onHasNext() throws IgniteCheckedException {
                if (cur == null)
                    cur = grp.mvccEnabled() ? data.cursor(FULL_WITH_HINTS) :
                        data.cursor(IgniteCacheOffheapManager.DATA_AND_TOMBSTONES);

                if (next != null)
                    return true;

                if (cur.next())
                    next = cur.get();

                boolean hasNext = next != null;

                if (!hasNext)
                    cur = null;

                return hasNext;
            }

            @Override protected void onClose() {
                assert loc != null && loc.state() == OWNING && loc.reservations() > 0
                    : "Partition should be in OWNING state and has at least 1 reservation: " + loc;

                loc.release();

                cur = null;
            }
        };
    }

    /** {@inheritDoc} */
    @Override public IgniteRebalanceIterator rebalanceIterator(IgniteDhtDemandedPartitionsMap parts,
        AffinityTopologyVersion topVer)
        throws IgniteCheckedException {

        TreeMap<Integer, GridCloseableIterator<CacheDataRow>> iterators = new TreeMap<>();

        Set<Integer> missing = new HashSet<>();

        for (Integer p : parts.fullSet()) {
            GridCloseableIterator<CacheDataRow> partIter = reservedIterator(p, topVer);

            if (partIter == null) {
                missing.add(p);

                continue;
            }

            iterators.put(p, partIter);
        }

        IgniteHistoricalIterator historicalIterator = historicalIterator(parts.historicalMap(), missing);

        IgniteRebalanceIterator iter = new IgniteRebalanceIteratorImpl(iterators, historicalIterator);

        for (Integer p : missing)
            iter.setPartitionMissing(p);

        return iter;
    }

    /**
     * @param partCntrs Partition counters map.
     * @param missing Set of partitions need to populate if partition is missing or failed to reserve.
     * @return Historical iterator.
     * @throws IgniteCheckedException If failed.
     */
    @Nullable protected IgniteHistoricalIterator historicalIterator(CachePartitionPartialCountersMap partCntrs, Set<Integer> missing)
        throws IgniteCheckedException {
        return null;
    }

    /** {@inheritDoc} */
    @Override public final CacheDataStore createCacheDataStore(int p) throws IgniteCheckedException {
        CacheDataStore dataStore;

        partStoreLock.lock(p);

        try {
            dataStore = createCacheDataStore0(p);
        }
        finally {
            partStoreLock.unlock(p);
        }

        return dataStore;
    }

    /**
     * @param p Partition.
     * @return Cache data store.
     * @throws IgniteCheckedException If failed.
     */
    protected CacheDataStore createCacheDataStore0(int p) throws IgniteCheckedException {
        long rootPage = allocateForTree();

        CacheDataRowStore rowStore = new CacheDataRowStore(grp, grp.freeList(), p);

        String dataTreeName = grp.cacheOrGroupName() + "-" + treeName(p);

        CacheDataTree dataTree = new CacheDataTree(
            grp,
            dataTreeName,
            grp.reuseList(),
            rowStore,
            rootPage,
            true,
            ctx.diagnostic().pageLockTracker(),
            FLAG_IDX
        );

        String logTreeName = BPlusTree.treeName(grp.cacheOrGroupName() + "-p-" + p, "CacheData");

        PartitionLogTree logTree = new PartitionLogTree(
            grp,
            p,
            logTreeName,
            grp.dataRegion().pageMemory(),
            allocateForTree(),
            grp.reuseList(),
            true,
            ctx.diagnostic().pageLockTracker(),
            FLAG_IDX,
            log
        );

        return new CacheDataStoreImpl(p, rowStore, dataTree, logTree, () -> pendingEntries, grp, busyLock, log, null);
    }

    /** {@inheritDoc} */
    @Override public void destroyCacheDataStore(CacheDataStore store) throws IgniteCheckedException {
        int p = store.partId();

        partStoreLock.lock(p);

        try {
            if (store.destroyed())
                return; // Already destroyed.

            destroyCacheDataStore0(store);
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
        finally {
            partStoreLock.unlock(p);
        }
    }

    /**
     * @param store Cache data store.
     * @throws IgniteCheckedException If failed.
     */
    protected void destroyCacheDataStore0(CacheDataStore store) throws IgniteCheckedException {
        store.destroy();
    }

    /**
     * @param p Partition.
     * @return Tree name for given partition.
     */
    protected final String treeName(int p) {
        return BPlusTree.treeName("p-" + p, "CacheData");
    }

    /** {@inheritDoc} */
    @Override public boolean expireRows(
        IgniteClosure2X<GridCacheEntryEx, Long, Boolean> c,
        int amount,
        long now
    ) {
        if (!busyLock.enterBusy())
            return false;

        try {
            return ctx.evict().expire(false, c, amount, now);
        }
        finally {
            busyLock.leaveBusy();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean expireTombstones(
        IgniteClosure2X<GridCacheEntryEx, Long, Boolean> c,
        int amount,
        long now
    ) {
        long tsCnt = tombstonesCount(), tsLimit = ctx.ttl().tombstonesLimit();

        // Even if tombstones count is zero, we have some entries in the queue and they must be processed.

        GridDhtPartitionsExchangeFuture fut = ctx.exchange().lastTopologyFuture();

        // We need to keep tombstones during rebalancing because they are used to handle put-remove conflicts.
        if (tsCnt <= tsLimit &&
            (!ctx.exchange().lastFinishedFuture().rebalanced() ||
                !(fut.isDone() && fut.rebalanced()) ||
                ctx.ttl().tombstoneCleanupSuspended()))
            return false;

        if (tsCnt > tsLimit) { // Force removal of tombstones beyond the limit.
            amount = (int) (tsCnt - tsLimit);

            now = Long.MAX_VALUE;
        }

        // Tombstones for volatile cache group are always cleared.
        if (!busyLock.enterBusy())
            return false;

        try {
            return ctx.evict().expire(true, c, amount, now);
        }
        finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * @param tombstone {@code True} to process tomstones.
     * @param amount Limit of processed entries by single call, {@code -1} for no limit.
     * @param upper Upper bound.
     * @param c Closure.
     * @return A number of scanned entries.
     * @throws IgniteCheckedException If failed.
     */
    @Override public int fillQueue(
        boolean tombstone,
        int amount,
        long upper,
        ToIntFunction<PendingRow> c
    ) throws IgniteCheckedException {
        if (!busyLock.enterBusy())
            return 0;

        long upper0 = upper;

        // Adjust upper bound if tombstone limit is exceeded.
        if (tombstone) {
            long tsCnt = tombstonesCount(), tsLimit = ctx.ttl().tombstonesLimit();

            if (tsCnt > tsLimit) {
                amount = (int) (tsCnt - tsLimit);

                upper0 = Long.MAX_VALUE;
            }
        }

        int cnt = 0;

        try {
            for (GridCacheContext ctx : grp.caches()) {
                if (!ctx.started())
                    continue;

                cnt += fillQueueInternal(pendingEntries, ctx, grp.sharedGroup() ? ctx.cacheId() : CU.UNDEFINED_CACHE_ID,
                    tombstone, amount - cnt, upper0, c);

                if (amount != -1 && cnt >= amount)
                    break;
            }
        }
        finally {
            busyLock.leaveBusy();
        }

        return cnt;
    }

    /**
     * @param pendingEntries Pending entries tree.
     * @param ctx Cache context.
     * @param cacheId Cache id for tree scan.
     * @param tombstone Tombstone.
     * @param amount The amount to fill.
     * @param upper Upper limit.
     * @param c Fill closure.
     */
    protected int fillQueueInternal(
        PendingEntriesTree pendingEntries,
        GridCacheContext ctx,
        int cacheId,
        boolean tombstone,
        int amount,
        long upper,
        ToIntFunction<PendingRow> c
    ) throws IgniteCheckedException {
        if (pendingEntries == null)
            return 0;

        GridCursor<PendingRow> cur = pendingEntries.find(new PendingRow(cacheId, tombstone, 0, 0),
                new PendingRow(cacheId, tombstone, upper, 0));

        if (!cur.next())
            return 0;

        int scanned = 0;

        do {
            if (amount != -1 && scanned >= amount)
                break;

            PendingRow row = cur.get();

            if (row.cacheId == CU.UNDEFINED_CACHE_ID)
                row.cacheId = ctx.cacheId();

            row.deploymentId = ctx.dynamicDeploymentId();

            if (row.key.partition() == -1)
                row.key.partition(ctx.affinity().partition(row.key));

            assert row.key != null && row.link != 0 && row.expireTime != 0 : row;

            if (c.applyAsInt(row) != 0)
                break;

            scanned++;
        }
        while (cur.next());

        return scanned;
    }

    /** {@inheritDoc} */
    @Override public long expiredSize() throws IgniteCheckedException {
        return pendingEntries != null ? pendingEntries.size() : 0;
    }

    /**
     *
     */
    public static class CacheDataStoreImpl implements CacheDataStore {
        /** */
        private final boolean IS_INCREMENTAL_DR_ENABLED = getBoolean("GG_INCREMENTAL_STATE_TRANSFER", true);

        /** */
        private final int partId;

        /** */
        private final CacheDataRowStore rowStore;

        /** */
        private final CacheDataTree dataTree;

        /** */
        private final PartitionLogTree logTree;

        /** */
        private final Supplier<PendingEntriesTree> pendingEntries;

        /** */
        private final CacheGroupContext grp;

        /** */
        private final GridSpinBusyLock busyLock;

        /** Update counter. */
        protected final PartitionUpdateCounter pCntr;

        /** Partition size. */
        private final AtomicLong storageSize = new AtomicLong();

        /** */
        private final IntMap<AtomicLong> cacheSizes = new IntRWHashMap<>();

        /** Mvcc remove handler. */
        private final PageHandler<MvccUpdateDataRow, Boolean> mvccUpdateMarker;

        /** Mvcc update tx state hint handler. */
        private final PageHandler<Void, Boolean> mvccUpdateTxStateHint;

        /** */
        private final PageHandler<MvccDataRow, Boolean> mvccApplyChanges;

        /** */
        private final IgniteLogger log;

        /** */
        private final Boolean failNodeOnPartitionInconsistency = Boolean.getBoolean(
            IgniteSystemProperties.IGNITE_FAIL_NODE_ON_UNRECOVERABLE_PARTITION_INCONSISTENCY
        );

        /** */
        private final int updateValSizeThreshold;

        /** Tombstones counter. */
        private final AtomicLong tombstonesCnt = new AtomicLong();

        /** */
        private volatile GridQueryRowCacheCleaner rowCacheCleaner;

        /**
         * @param partId Partition number.
         * @param rowStore Row store.
         * @param dataTree Data tree.
         * @param logTree Partition log tree.
         */
        public CacheDataStoreImpl(
                int partId,
                CacheDataRowStore rowStore,
                CacheDataTree dataTree,
                PartitionLogTree logTree,
                Supplier<PendingEntriesTree> pendingEntries,
                CacheGroupContext grp,
                GridSpinBusyLock busyLock,
                IgniteLogger log,
                @Nullable Supplier<GridQueryRowCacheCleaner> cleaner
            ) {
            this.partId = partId;
            this.rowStore = rowStore;
            this.dataTree = dataTree;
            this.logTree = logTree;
            this.pendingEntries = pendingEntries;
            this.grp = grp;
            this.busyLock = busyLock;
            this.log = log;

            PartitionUpdateCounter delegate = grp.mvccEnabled() ? new PartitionUpdateCounterMvccImpl(grp) :
                !grp.persistenceEnabled() || grp.hasAtomicCaches() ? new PartitionUpdateCounterVolatileImpl(grp) :
                            new PartitionUpdateCounterTrackingImpl(grp);

            pCntr = grp.shared().logger(PartitionUpdateCounterDebugWrapper.class).isDebugEnabled() ?
                new PartitionUpdateCounterDebugWrapper(partId, delegate) : new PartitionUpdateCounterErrorWrapper(partId, delegate);

            updateValSizeThreshold = grp.shared().database().pageSize() / 2;

            mvccUpdateMarker = new MvccMarkUpdatedHandler(grp);
            mvccUpdateTxStateHint = new MvccUpdateTxStateHintHandler(grp);
            mvccApplyChanges = new MvccApplyChangesHandler(grp);

            if (cleaner == null)
                rowStore.setRowCacheCleaner(() -> rowCacheCleaner);
            else
                rowStore.setRowCacheCleaner(cleaner);
        }

        /** {@inheritDoc} */
        @Override public CacheDataTree tree() {
            return dataTree;
        }

        /**
         * @param cacheId Cache ID.
         */
        void incrementSize(int cacheId) {
            updateSize(cacheId, 1);
        }

        /**
         * @param cacheId Cache ID.
         */
        void decrementSize(int cacheId) {
            updateSize(cacheId, -1);
        }

        /** {@inheritDoc} */
        @Override public boolean init() {
            return false;
        }

        /** {@inheritDoc} */
        @Override public int partId() {
            return partId;
        }

        /** {@inheritDoc} */
        @Override public long cacheSize(int cacheId) {
            if (grp.sharedGroup()) {
                AtomicLong size = cacheSizes.get(cacheId);

                return size != null ? (int)size.get() : 0;
            }

            return storageSize.get();
        }

        /** {@inheritDoc} */
        @Override public Map<Integer, Long> cacheSizes() {
            if (!grp.sharedGroup())
                return null;

            Map<Integer, Long> res = new HashMap<>();

            cacheSizes.forEach((key, val) -> res.put(key, val.longValue()));

            return res;
        }

        /** {@inheritDoc} */
        @Override public long fullSize() {
            return storageSize.get();
        }

        /**
         * @return {@code True} if there are no items in the store.
         */
        @Override public boolean isEmpty() {
            try {
                /*
                 * TODO https://issues.apache.org/jira/browse/IGNITE-10082
                 * Using of counters is cheaper than tree operations. Return size checking after the ticked is resolved.
                 */
                return grp.mvccEnabled() ? dataTree.isEmpty() : storageSize.get() == 0;
            }
            catch (IgniteCheckedException e) {
                U.error(grp.shared().logger(IgniteCacheOffheapManagerImpl.class), "Failed to perform operation.", e);

                return false;
            }
        }

        /** {@inheritDoc} */
        @Override public void updateSize(int cacheId, long delta) {
            storageSize.addAndGet(delta);

            if (grp.sharedGroup()) {
                AtomicLong size = cacheSizes.get(cacheId);

                if (size == null) {
                    AtomicLong old = cacheSizes.putIfAbsent(cacheId, size = new AtomicLong());

                    if (old != null)
                        size = old;
                }

                size.addAndGet(delta);
            }
        }

        /** {@inheritDoc} */
        @Override public long nextUpdateCounter() {
            return pCntr.next();
        }

        /** {@inheritDoc} */
        @Override public long initialUpdateCounter() {
            return pCntr.initial();
        }

        /** {@inheritDoc}
         * @param start Start.
         * @param delta Delta.
         */
        @Override public void updateInitialCounter(long start, long delta) {
            pCntr.updateInitial(start, delta);
        }

        /** {@inheritDoc} */
        @Override public long getAndIncrementUpdateCounter(long delta) {
            return pCntr.reserve(delta);
        }

        /** {@inheritDoc} */
        @Override public long updateCounter() {
            return pCntr.get();
        }

        /** {@inheritDoc} */
        @Override public long reservedCounter() {
            return pCntr.reserved();
        }

        /** {@inheritDoc} */
        @Override public PartitionUpdateCounter partUpdateCounter() {
            return pCntr;
        }

        /** {@inheritDoc} */
        @Override public long reserve(long delta) {
            return pCntr.reserve(delta);
        }

        /** {@inheritDoc} */
        @Override public void updateCounter(long val) {
            try {
                pCntr.update(val);
            }
            catch (IgniteCheckedException e) {
                U.error(log, "Failed to update partition counter. " +
                    "Most probably a node with most actual data is out of topology or data streamer is used " +
                    "in preload mode (allowOverride=false) concurrently with cache transactions [grpName=" +
                    grp.cacheOrGroupName() + ", partId=" + partId + ']', e);

                if (failNodeOnPartitionInconsistency)
                    grp.shared().kernalContext().failure().process(new FailureContext(FailureType.CRITICAL_ERROR, e));
            }
        }

        /** {@inheritDoc} */
        @Override public boolean updateCounter(long start, long delta) {
            return pCntr.update(start, delta);
        }

        /** {@inheritDoc} */
        @Override public GridLongList finalizeUpdateCounters() {
            return pCntr.finalizeUpdateCounters();
        }

        /**
         * @param cctx Cache context.
         * @param oldRow Old row.
         * @param dataRow New row.
         * @return {@code True} if it is possible to update old row data.
         * @throws IgniteCheckedException If failed.
         */
        private boolean canUpdateOldRow(GridCacheContext cctx, @Nullable CacheDataRow oldRow, DataRow dataRow)
            throws IgniteCheckedException {
            if (oldRow == null || cctx.queries().enabled() || grp.mvccEnabled()
                || cctx.cacheObjectContext().compressionStrategy() != null)
                return false;

            if (oldRow.expireTime() != dataRow.expireTime())
                return false;

            oldRow.key().prepareForCache(cctx.cacheObjectContext(), false);
            oldRow.value().prepareForCache(cctx.cacheObjectContext(), false);

            int oldLen = oldRow.size();

            // Use grp.sharedGroup() flag since it is possible cacheId is not yet set here.
            if (!grp.storeCacheIdInDataPage() && grp.sharedGroup() && oldRow.cacheId() != CU.UNDEFINED_CACHE_ID)
                oldLen -= 4;

            if (oldLen > updateValSizeThreshold)
                return false;

            dataRow.key().prepareForCache(cctx.cacheObjectContext(), false);
            dataRow.value().prepareForCache(cctx.cacheObjectContext(), false);

            int newLen = dataRow.size();

            return oldLen == newLen;
        }

        /** {@inheritDoc} */
        @Override public void invoke(GridCacheContext cctx, KeyCacheObject key, OffheapInvokeClosure c)
            throws IgniteCheckedException {
            if (!busyLock.enterBusy())
                throw new NodeStoppingException("Operation has been cancelled (node is stopping).");

            int cacheId = grp.sharedGroup() ? cctx.cacheId() : CU.UNDEFINED_CACHE_ID;

            try {
                invoke0(cctx, new SearchRow(cacheId, key), c);
            }
            finally {
                busyLock.leaveBusy();
            }
        }

        /**
         * @param cctx Cache context.
         * @param row Search row.
         * @param c Closure.
         * @throws IgniteCheckedException If failed.
         */
        private void invoke0(GridCacheContext cctx, CacheSearchRow row, OffheapInvokeClosure c)
            throws IgniteCheckedException {
            assert cctx.shared().database().checkpointLockIsHeldByThread();

            dataTree.invoke(row, CacheDataRowAdapter.RowData.NO_KEY, c);

            CacheDataRow newRow = c.newRow();

            switch (c.operationType()) {
                case PUT: {
                    CacheDataRow oldRow = c.oldRow();

                    assert newRow != null : c;

                    // Row was logically removed by update closure.
                    if (c.newRow().tombstone())
                        finishRemove(cctx, row.key(), oldRow, newRow);
                    else
                        finishUpdate(cctx, newRow, oldRow);

                    break;
                }

                case REMOVE: {
                    // Closures should not produce physical removes, probably should add assertion instead.
                    CacheDataRow oldRow = c.oldRow();

                    finishRemove(cctx, row.key(), oldRow, null);

                    break;
                }

                case IN_PLACE:
                    CacheDataRow oldRow = c.oldRow();

                    assert oldRow != null;
                    assert newRow != null;

                    if (newRow.tombstone() && !oldRow.tombstone()) {
                        updatePendingEntries(cctx, c.newRow(), null);

                        tombstoneCreated();

                        decrementSize(cctx.cacheId());
                    }
                    else if (oldRow.tombstone() && !newRow.tombstone()) {
                        clearPendingEntries(cctx, c.oldRow());

                        tombstoneRemoved();

                        incrementSize(cctx.cacheId());
                    }

                    if (isIncrementalDrEnabled(cctx)) {
                        if (oldRow.version().updateCounter() != 0)
                            removeFromLog(new UpdateLogRow(cctx.cacheId(), oldRow.version().updateCounter(), oldRow.link()));

                        if (newRow.version().updateCounter() != 0)
                            addUpdateToLog(new UpdateLogRow(cctx.cacheId(), newRow.version().updateCounter(), newRow.link()));
                    }

                    break;

                case NOOP:
                    break;

                default:
                    assert false : c.operationType();
            }
        }

        /**
         * Check replication conditions.
         *
         * @param ver Version to check.
         * @return {@code true} if row need to be replicated, {@code false} otherwise.
         */
        public static boolean replicationRequire(GridCacheVersion ver) {
            final GridCacheVersion conflictVer = ver.conflictVersion();

            if (ver == conflictVer)
                return true;

            final byte dc0 = ver.dataCenterId();
            final byte dc1 = conflictVer.dataCenterId();

            return dc0 != dc1;
        }

        /**
         * @param cctx Cache context.
         * @return {@code true} if IncrementalDR enabled for cache, {@code false} otherwise.
         */
        private boolean isIncrementalDrEnabled(GridCacheContext cctx) {
            return IS_INCREMENTAL_DR_ENABLED && cctx.isDrEnabled();
        }

        /** {@inheritDoc} */
        @Override public CacheDataRow createRow(
            GridCacheContext cctx,
            KeyCacheObject key,
            CacheObject val,
            GridCacheVersion ver,
            long expireTime,
            @Nullable CacheDataRow oldRow) throws IgniteCheckedException {
            int cacheId = grp.storeCacheIdInDataPage() ? cctx.cacheId() : CU.UNDEFINED_CACHE_ID;

            // Set real stored cacheId to properly calculate row size.
            if (oldRow != null)
                oldRow.cacheId(cacheId);

            DataRow dataRow = makeDataRow(key, val, ver, expireTime, cacheId);

            if (canUpdateOldRow(cctx, oldRow, dataRow) && rowStore.updateRow(oldRow.link(), dataRow, grp.statisticsHolderData()))
                dataRow.link(oldRow.link());
            else {
                CacheObjectContext coCtx = cctx.cacheObjectContext();

                key = key.prepareForCache(coCtx, coCtx.compressKeys());

                if (val != null)
                    val = val.prepareForCache(coCtx, true);

                dataRow = makeDataRow(key, val, ver, expireTime, cacheId);

                rowStore.addRow(dataRow, grp.statisticsHolderData());
            }

            assert dataRow.link() != 0 : dataRow;

            if (grp.sharedGroup()) {
                if (dataRow.cacheId() == CU.UNDEFINED_CACHE_ID)
                    dataRow.cacheId(cctx.cacheId());

                if (oldRow != null && oldRow.cacheId() == CU.UNDEFINED_CACHE_ID)
                    oldRow.cacheId(cctx.cacheId());
            }

            return dataRow;
        }

        /**
         * @param key Cache key.
         * @param val Cache value.
         * @param ver Version.
         * @param expireTime Expired time.
         * @param cacheId Cache id.
         * @return Made data row.
         */
        @NotNull private DataRow makeDataRow(KeyCacheObject key, CacheObject val, GridCacheVersion ver, long expireTime,
            int cacheId) {
            if (key.partition() == -1)
                key.partition(partId);

            return new DataRow(key, val, ver, partId, expireTime, cacheId);
        }

        /** {@inheritDoc} */
        @Override public boolean mvccInitialValue(
            GridCacheContext cctx,
            KeyCacheObject key,
            @Nullable CacheObject val,
            GridCacheVersion ver,
            long expireTime,
            MvccVersion mvccVer,
            MvccVersion newMvccVer)
            throws IgniteCheckedException
        {
            assert mvccVer != null || newMvccVer == null : newMvccVer;

            if (!busyLock.enterBusy())
                throw new NodeStoppingException("Operation has been cancelled (node is stopping).");

            try {
                CacheObjectContext coCtx = cctx.cacheObjectContext();

                key = key.prepareForCache(coCtx, cctx.cacheObjectContext().compressKeys());

                // null is passed for loaded from store.
                if (mvccVer == null) {
                    mvccVer = INITIAL_VERSION;

                    // Clean all versions of row
                    mvccRemoveAll(cctx, key);
                }

                if (val != null) {
                    val = val.prepareForCache(coCtx, true);

                    MvccDataRow updateRow = new MvccDataRow(
                        key,
                        val,
                        ver,
                        partId,
                        expireTime,
                        cctx.cacheId(),
                        mvccVer,
                        newMvccVer);

                    assert cctx.shared().database().checkpointLockIsHeldByThread();

                    if (!grp.storeCacheIdInDataPage() && updateRow.cacheId() != CU.UNDEFINED_CACHE_ID) {
                        updateRow.cacheId(CU.UNDEFINED_CACHE_ID);

                        rowStore.addRow(updateRow, grp.statisticsHolderData());

                        updateRow.cacheId(cctx.cacheId());
                    }
                    else
                        rowStore.addRow(updateRow, grp.statisticsHolderData());

                    dataTree.putx(updateRow);

                    incrementSize(cctx.cacheId());

                    if (cctx.queries().enabled())
                        cctx.queries().store(updateRow, null, true);

                    return true;
                }
            }
            finally {
                busyLock.leaveBusy();
            }

            return false;
        }

        /** {@inheritDoc} */
        @Override public boolean mvccApplyHistoryIfAbsent(
            GridCacheContext cctx,
            KeyCacheObject key,
            List<GridCacheMvccEntryInfo> hist) throws IgniteCheckedException {
            assert !F.isEmpty(hist);

            if (!busyLock.enterBusy())
                throw new NodeStoppingException("Operation has been cancelled (node is stopping).");

            try {
                CacheObjectContext coCtx = cctx.cacheObjectContext();

                key = key.prepareForCache(coCtx, coCtx.compressKeys());

                assert cctx.shared().database().checkpointLockIsHeldByThread();

                int cacheId = grp.sharedGroup() ? cctx.cacheId() : CU.UNDEFINED_CACHE_ID;

                // No cursor needed here  as we won't iterate over whole page items, but just check if page has smth or not.
                CheckHistoryExistsClosure clo = new CheckHistoryExistsClosure();

                dataTree.iterate(
                    new MvccMaxSearchRow(cacheId, key),
                    new MvccMinSearchRow(cacheId, key),
                    clo
                );

                if (clo.found())
                    return false;

                for (GridCacheMvccEntryInfo info : hist) {
                    CacheObject val = info.value();

                    if (val != null)
                        val = val.prepareForCache(coCtx, true);

                    MvccDataRow row = new MvccDataRow(key,
                        val,
                        info.version(),
                        partId,
                        info.ttl(),
                        cacheId,
                        info.mvccVersion(),
                        info.newMvccVersion());

                    row.mvccTxState(info.mvccTxState());
                    row.newMvccTxState(info.newMvccTxState());

                    assert info.newMvccTxState() == TxState.NA || info.newMvccCoordinatorVersion() > MVCC_CRD_COUNTER_NA;
                    assert MvccUtils.mvccVersionIsValid(info.mvccCoordinatorVersion(), info.mvccCounter(), info.mvccOperationCounter());

                    if (!grp.storeCacheIdInDataPage() && cacheId != CU.UNDEFINED_CACHE_ID)
                        row.cacheId(CU.UNDEFINED_CACHE_ID);

                    rowStore.addRow(row, grp.statisticsHolderData());

                    row.cacheId(cacheId);

                    boolean hasOld = dataTree.putx(row);

                    assert !hasOld : row;

                    finishUpdate(cctx, row, null);
                }

                return true;
            }
            finally {
                busyLock.leaveBusy();
            }
        }

        /** {@inheritDoc} */
        @Override public boolean mvccUpdateRowWithPreloadInfo(
            GridCacheContext cctx,
            KeyCacheObject key,
            @Nullable CacheObject val,
            GridCacheVersion ver,
            long expireTime,
            MvccVersion mvccVer,
            MvccVersion newMvccVer,
            byte mvccTxState,
            byte newMvccTxState) throws IgniteCheckedException {
            if (!busyLock.enterBusy())
                throw new NodeStoppingException("Operation has been cancelled (node is stopping).");

            try {
                CacheObjectContext coCtx = cctx.cacheObjectContext();

                key.prepareForCache(coCtx, coCtx.compressKeys());

                if (val != null)
                    val.prepareForCache(coCtx, true);

                assert cctx.shared().database().checkpointLockIsHeldByThread();

                MvccUpdateRowWithPreloadInfoClosure clo = new MvccUpdateRowWithPreloadInfoClosure(cctx,
                    key,
                    val,
                    ver,
                    expireTime,
                    mvccVer,
                    newMvccVer,
                    mvccTxState,
                    newMvccTxState);

                assert newMvccTxState == TxState.NA || newMvccVer.coordinatorVersion() != 0;
                assert MvccUtils.mvccVersionIsValid(mvccVer.coordinatorVersion(), mvccVer.counter(), mvccVer.operationCounter());

                invoke0(cctx, clo, clo);

                return clo.operationType() == PUT;
            }
            finally {
                busyLock.leaveBusy();
            }
        }

        /** {@inheritDoc} */
        @Override public MvccUpdateResult mvccUpdate(
            GridCacheContext cctx,
            KeyCacheObject key,
            CacheObject val,
            GridCacheVersion ver,
            long expireTime,
            MvccSnapshot mvccSnapshot,
            @Nullable CacheEntryPredicate filter,
            EntryProcessor entryProc,
            Object[] invokeArgs,
            boolean primary,
            boolean needHistory,
            boolean noCreate,
            boolean needOldVal,
            boolean retVal,
            boolean keepBinary) throws IgniteCheckedException {
            assert mvccSnapshot != null;
            assert primary || !needHistory;

            if (!busyLock.enterBusy())
                throw new NodeStoppingException("Operation has been cancelled (node is stopping).");

            try {
                int cacheId = grp.sharedGroup() ? cctx.cacheId() : CU.UNDEFINED_CACHE_ID;

                CacheObjectContext coCtx = cctx.cacheObjectContext();

                key = key.prepareForCache(coCtx, coCtx.compressKeys());

                if (val != null)
                    val = val.prepareForCache(coCtx, true);

                 MvccUpdateDataRow updateRow = new MvccUpdateDataRow(
                    cctx,
                    key,
                    val,
                    ver,
                    partId,
                    expireTime,
                    mvccSnapshot,
                    null,
                    filter,
                    primary,
                    false,
                    needHistory,
                    // we follow fast update visit flow here if row cannot be created by current operation
                    noCreate,
                    needOldVal,
                    retVal || entryProc != null);

                assert cctx.shared().database().checkpointLockIsHeldByThread();

                dataTree.visit(new MvccMaxSearchRow(cacheId, key), new MvccMinSearchRow(cacheId, key), updateRow);

                ResultType res = updateRow.resultType();

                if (res == ResultType.LOCKED // cannot update locked
                    || res == ResultType.VERSION_MISMATCH) // cannot update on write conflict
                    return updateRow;
                else if (res == ResultType.VERSION_FOUND || // exceptional case
                        res == ResultType.FILTERED || // Operation should be skipped.
                        (res == ResultType.PREV_NULL && noCreate)  // No op.
                    ) {
                    // Do nothing, except cleaning up not needed versions
                    cleanup0(cctx, updateRow.cleanupRows());

                    return updateRow;
                }

                CacheDataRow oldRow = null;

                if (res == ResultType.PREV_NOT_NULL) {
                    oldRow = updateRow.oldRow();

                    assert oldRow != null && oldRow.link() != 0 : oldRow;

                    oldRow.key(key);
                }
                else
                    assert res == ResultType.PREV_NULL;

                if (entryProc != null) {
                    entryProc = EntryProcessorResourceInjectorProxy.wrap(cctx.kernalContext(), entryProc);

                    CacheInvokeEntry.Operation op = applyEntryProcessor(cctx, key, ver, entryProc, invokeArgs,
                        updateRow, oldRow, keepBinary);

                    if (op == CacheInvokeEntry.Operation.NONE) {
                        if (res == ResultType.PREV_NOT_NULL)
                            updateRow.value(oldRow.value()); // Restore prev. value.

                        updateRow.resultType(ResultType.FILTERED);

                        cleanup0(cctx, updateRow.cleanupRows());

                        return updateRow;
                    }

                    // Mark old version as removed.
                    if (res == ResultType.PREV_NOT_NULL) {
                        rowStore.updateDataRow(oldRow.link(), mvccUpdateMarker, updateRow, grp.statisticsHolderData());

                        if (op == CacheInvokeEntry.Operation.REMOVE) {
                            updateRow.resultType(ResultType.REMOVED_NOT_NULL);

                            cleanup0(cctx, updateRow.cleanupRows());

                            clearPendingEntries(cctx, oldRow);

                            return updateRow; // Won't create new version on remove.
                        }
                    }
                    else
                        assert op != CacheInvokeEntry.Operation.REMOVE;
                }
                else if (oldRow != null)
                    rowStore.updateDataRow(oldRow.link(), mvccUpdateMarker, updateRow, grp.statisticsHolderData());

                if (!grp.storeCacheIdInDataPage() && updateRow.cacheId() != CU.UNDEFINED_CACHE_ID) {
                    updateRow.cacheId(CU.UNDEFINED_CACHE_ID);

                    rowStore.addRow(updateRow, grp.statisticsHolderData());

                    updateRow.cacheId(cctx.cacheId());
                }
                else
                    rowStore.addRow(updateRow, grp.statisticsHolderData());

                if (needHistory) {
                    assert updateRow.link() != 0;

                    updateRow.history().add(0, new MvccLinkAwareSearchRow(cacheId,
                        key,
                        updateRow.mvccCoordinatorVersion(),
                        updateRow.mvccCounter(),
                        updateRow.mvccOperationCounter(),
                        updateRow.link()));
                }

                boolean old = dataTree.putx(updateRow);

                assert !old;

                GridCacheQueryManager qryMgr = cctx.queries();

                if (qryMgr.enabled())
                    qryMgr.store(updateRow, null, true);

                updatePendingEntries(cctx, updateRow, oldRow);

                cleanup0(cctx, updateRow.cleanupRows());

                return updateRow;
            }
            finally {
                busyLock.leaveBusy();
            }
        }

        /**
         *
         * @param cctx Cache context.
         * @param key entry key.
         * @param ver Entry version.
         * @param entryProc Entry processor.
         * @param invokeArgs Entry processor invoke arguments.
         * @param updateRow Row for update.
         * @param oldRow Old row.
         * @param keepBinary Keep binary flag.
         * @return Entry processor operation.
         */
        @SuppressWarnings("unchecked")
        private CacheInvokeEntry.Operation applyEntryProcessor(GridCacheContext cctx,
            KeyCacheObject key,
            GridCacheVersion ver,
            EntryProcessor entryProc,
            Object[] invokeArgs,
            MvccUpdateDataRow updateRow,
            CacheDataRow oldRow,
            boolean keepBinary) {
            Object procRes = null;
            Exception err = null;

            CacheObject oldVal = oldRow == null ? null : oldRow.value();

            CacheInvokeEntry invokeEntry = new CacheInvokeEntry<>(key, oldVal, ver, keepBinary,
                new GridDhtDetachedCacheEntry(cctx, key));

            try {
                procRes = entryProc.process(invokeEntry, invokeArgs);

                if (invokeEntry.modified() && invokeEntry.op() != CacheInvokeEntry.Operation.REMOVE) {
                    Object val = invokeEntry.getValue(true);

                    CacheObject val0 = cctx.toCacheObject(val);

                    val0 = val0.prepareForCache(cctx.cacheObjectContext(), true);

                    updateRow.value(val0);
                }
            }
            catch (Exception e) {
                log.error("Exception was thrown during entry processing.", e);

                err = e;
            }

            CacheInvokeResult invokeRes = err == null ? CacheInvokeResult.fromResult(procRes) :
                CacheInvokeResult.fromError(err);

            updateRow.invokeResult(invokeRes);

            return invokeEntry.op();
        }

        /** {@inheritDoc} */
        @Override public MvccUpdateResult mvccRemove(GridCacheContext cctx,
            KeyCacheObject key,
            MvccSnapshot mvccSnapshot,
            @Nullable CacheEntryPredicate filter,
            boolean primary,
            boolean needHistory,
            boolean needOldVal,
            boolean retVal) throws IgniteCheckedException {
            assert mvccSnapshot != null;
            assert primary || mvccSnapshot.activeTransactions().size() == 0 : mvccSnapshot;
            assert primary || !needHistory;

            if (!busyLock.enterBusy())
                throw new NodeStoppingException("Operation has been cancelled (node is stopping).");

            try {
                int cacheId = grp.sharedGroup() ? cctx.cacheId() : CU.UNDEFINED_CACHE_ID;

                CacheObjectContext coCtx = cctx.cacheObjectContext();

                key = key.prepareForCache(coCtx, false);

                MvccUpdateDataRow updateRow = new MvccUpdateDataRow(
                    cctx,
                    key,
                    null,
                    null,
                    partId,
                    0,
                    mvccSnapshot,
                    null,
                    filter,
                    primary,
                    false,
                    needHistory,
                    true,
                    needOldVal,
                    retVal);

                assert cctx.shared().database().checkpointLockIsHeldByThread();

                dataTree.visit(new MvccMaxSearchRow(cacheId, key), new MvccMinSearchRow(cacheId, key), updateRow);

                ResultType res = updateRow.resultType();

                if (res == ResultType.LOCKED // cannot update locked
                    || res == ResultType.VERSION_MISMATCH) // cannot update on write conflict
                    return updateRow;
                else if (res == ResultType.VERSION_FOUND || res == ResultType.FILTERED) {
                    // Do nothing, except cleaning up not needed versions
                    cleanup0(cctx, updateRow.cleanupRows());

                    return updateRow;
                }
                else if (res == ResultType.PREV_NOT_NULL) {
                    CacheDataRow oldRow = updateRow.oldRow();

                    assert oldRow != null && oldRow.link() != 0 : oldRow;

                    rowStore.updateDataRow(oldRow.link(), mvccUpdateMarker, updateRow, grp.statisticsHolderData());

                    clearPendingEntries(cctx, oldRow);
                }

                cleanup0(cctx, updateRow.cleanupRows());

                return updateRow;
            }
            finally {
                busyLock.leaveBusy();
            }
        }

        /** {@inheritDoc} */
        @Override public MvccUpdateResult mvccLock(GridCacheContext cctx, KeyCacheObject key,
            MvccSnapshot mvccSnapshot) throws IgniteCheckedException {
            assert mvccSnapshot != null;

            if (!busyLock.enterBusy())
                throw new NodeStoppingException("Operation has been cancelled (node is stopping).");

            try {
                int cacheId = grp.sharedGroup() ? cctx.cacheId() : CU.UNDEFINED_CACHE_ID;

                CacheObjectContext coCtx = cctx.cacheObjectContext();

                key = key.prepareForCache(coCtx, false);

                MvccUpdateDataRow updateRow = new MvccUpdateDataRow(
                    cctx,
                    key,
                    null,
                    null,
                    partId,
                    0,
                    mvccSnapshot,
                    null,
                    null,
                    true,
                    true,
                    false,
                    false,
                    false,
                    false);

                assert cctx.shared().database().checkpointLockIsHeldByThread();

                dataTree.visit(new MvccMaxSearchRow(cacheId, key), new MvccMinSearchRow(cacheId, key), updateRow);

                ResultType res = updateRow.resultType();

                // cannot update locked, cannot update on write conflict
                if (res == ResultType.LOCKED || res == ResultType.VERSION_MISMATCH)
                    return updateRow;

                // Do nothing, except cleaning up not needed versions
                cleanup0(cctx, updateRow.cleanupRows());

                return updateRow;
            }
            finally {
                busyLock.leaveBusy();
            }
        }

        /** {@inheritDoc} */
        @Override public void mvccRemoveAll(GridCacheContext cctx, KeyCacheObject key) throws IgniteCheckedException {
            if (!busyLock.enterBusy())
                throw new NodeStoppingException("Operation has been cancelled (node is stopping).");

            try {
                key = key.prepareForCache(cctx.cacheObjectContext(), false);

                int cacheId = grp.sharedGroup() ? cctx.cacheId() : CU.UNDEFINED_CACHE_ID;

                boolean cleanup = cctx.queries().enabled();

                assert cctx.shared().database().checkpointLockIsHeldByThread();

                GridCursor<CacheDataRow> cur = dataTree.find(
                    new MvccMaxSearchRow(cacheId, key),
                    new MvccMinSearchRow(cacheId, key),
                    cleanup ? CacheDataRowAdapter.RowData.NO_KEY : CacheDataRowAdapter.RowData.LINK_ONLY
                );

                boolean first = true;

                while (cur.next()) {
                    CacheDataRow row = cur.get();

                    row.key(key);

                    assert row.link() != 0 : row;

                    boolean rmvd = dataTree.removex(row);

                    assert rmvd : row;

                    if (cleanup) {
                        if (cctx.queries().enabled())
                            cctx.queries().remove(key, row);

                        if (first)
                            clearPendingEntries(cctx, row);
                    }

                    rowStore.removeRow(row.link(), grp.statisticsHolderData());

                    if (first)
                        first = false;
                }

                // first == true means there were no row versions
                if (!first)
                    decrementSize(cctx.cacheId());

            }
            finally {
                busyLock.leaveBusy();
            }
        }

        /** {@inheritDoc} */
        @Override public int cleanup(GridCacheContext cctx, @Nullable List<MvccLinkAwareSearchRow> cleanupRows)
            throws IgniteCheckedException {
            if (F.isEmpty(cleanupRows))
                return 0;

            if (!busyLock.enterBusy())
                throw new NodeStoppingException("Operation has been cancelled (node is stopping).");

            try {
                return cleanup0(cctx, cleanupRows);
            }
            finally {
                busyLock.leaveBusy();
            }
        }

        /**
         * @param cctx Cache context.
         * @param cleanupRows Rows to cleanup.
         * @throws IgniteCheckedException If failed.
         * @return Cleaned rows count.
         */
        private int cleanup0(GridCacheContext cctx, @Nullable List<MvccLinkAwareSearchRow> cleanupRows)
             throws IgniteCheckedException {
             if (F.isEmpty(cleanupRows))
                 return 0;

            int res = 0;

            GridCacheQueryManager qryMgr = cctx.queries();

            for (int i = 0; i < cleanupRows.size(); i++) {
                MvccLinkAwareSearchRow cleanupRow = cleanupRows.get(i);

                assert cleanupRow.link() != 0 : cleanupRow;

                assert cctx.shared().database().checkpointLockIsHeldByThread();

                CacheDataRow oldRow = dataTree.remove(cleanupRow);

                if (oldRow != null) { // oldRow == null means it was cleaned by another cleanup process.
                    assert oldRow.mvccCounter() == cleanupRow.mvccCounter();

                    if (qryMgr.enabled())
                        qryMgr.remove(oldRow.key(), oldRow);

                    clearPendingEntries(cctx, oldRow);

                    rowStore.removeRow(cleanupRow.link(), grp.statisticsHolderData());

                    res++;
                }
            }

            return res;
        }

        /** {@inheritDoc} */
        @Override public void updateTxState(GridCacheContext cctx, CacheSearchRow row)
            throws IgniteCheckedException {
            assert grp.mvccEnabled();
            assert mvccVersionIsValid(row.mvccCoordinatorVersion(), row.mvccCounter(), row.mvccOperationCounter()) : row;

            // Need an extra lookup because the row may be already cleaned by another thread.
            CacheDataRow row0 = dataTree.findOne(row, CacheDataRowAdapter.RowData.LINK_ONLY);

            if (row0 != null)
                rowStore.updateDataRow(row0.link(), mvccUpdateTxStateHint, null, grp.statisticsHolderData());
        }

        /** {@inheritDoc} */
        @Override public void update(GridCacheContext cctx,
            KeyCacheObject key,
            CacheObject val,
            GridCacheVersion ver,
            long expireTime,
            @Nullable CacheDataRow oldRow
        ) throws IgniteCheckedException {
            assert oldRow == null || oldRow.link() != 0L : oldRow;

            if (!busyLock.enterBusy())
                throw new NodeStoppingException("Operation has been cancelled (node is stopping).");

            try {
                int cacheId = grp.storeCacheIdInDataPage() ? cctx.cacheId() : CU.UNDEFINED_CACHE_ID;

                assert oldRow == null || oldRow.cacheId() == cacheId : oldRow;

                CacheObjectContext coCtx = cctx.cacheObjectContext();

                key = key.prepareForCache(coCtx, coCtx.compressKeys());

                if (val != null)
                    val = val.prepareForCache(coCtx, true);

                DataRow dataRow = makeDataRow(key, val, ver, expireTime, cacheId);

                CacheDataRow old;

                assert cctx.shared().database().checkpointLockIsHeldByThread();

                if (canUpdateOldRow(cctx, oldRow, dataRow) && rowStore.updateRow(oldRow.link(), dataRow, grp.statisticsHolderData())) {
                    old = oldRow;

                    dataRow.link(oldRow.link());
                }
                else {
                    rowStore.addRow(dataRow, grp.statisticsHolderData());

                    assert dataRow.link() != 0 : dataRow;

                    if (grp.sharedGroup() && dataRow.cacheId() == CU.UNDEFINED_CACHE_ID)
                        dataRow.cacheId(cctx.cacheId());

                    if (oldRow != null) {
                        old = oldRow;

                        dataTree.putx(dataRow);
                    }
                    else
                        old = dataTree.put(dataRow);
                }

                finishUpdate(cctx, dataRow, old);
            }
            finally {
                busyLock.leaveBusy();
            }
        }

        /** {@inheritDoc} */
        @Override public void mvccApplyUpdate(GridCacheContext cctx,
            KeyCacheObject key,
            CacheObject val,
            GridCacheVersion ver,
            long expireTime,
            MvccVersion mvccVer
        ) throws IgniteCheckedException {
            if (!busyLock.enterBusy())
                throw new NodeStoppingException("Operation has been cancelled (node is stopping).");

            try {
                int cacheId = grp.sharedGroup() ? cctx.cacheId() : CU.UNDEFINED_CACHE_ID;

                CacheObjectContext coCtx = cctx.cacheObjectContext();

                key = key.prepareForCache(coCtx, coCtx.compressKeys());

                if (val != null)
                    val = val.prepareForCache(coCtx, true);

                MvccSnapshotWithoutTxs mvccSnapshot = new MvccSnapshotWithoutTxs(mvccVer.coordinatorVersion(),
                    mvccVer.counter(), mvccVer.operationCounter(), MvccUtils.MVCC_COUNTER_NA);

                MvccUpdateDataRow updateRow = new MvccUpdateDataRow(
                    cctx,
                    key,
                    val,
                    ver,
                    partId,
                    0L,
                    mvccSnapshot,
                    null,
                    null,
                    false,
                    false,
                    false,
                    false,
                    false,
                    false);

                assert cctx.shared().database().checkpointLockIsHeldByThread();

                dataTree.visit(new MvccMaxSearchRow(cacheId, key), new MvccMinSearchRow(cacheId, key), updateRow);

                ResultType res = updateRow.resultType();

                assert res == ResultType.PREV_NULL || res == ResultType.PREV_NOT_NULL : res;

                if (res == ResultType.PREV_NOT_NULL) {
                    CacheDataRow oldRow = updateRow.oldRow();

                    assert oldRow != null && oldRow.link() != 0 : oldRow;

                    rowStore.updateDataRow(oldRow.link(), mvccUpdateMarker, updateRow, grp.statisticsHolderData());
                }

                if (val != null) {
                    if (!grp.storeCacheIdInDataPage() && updateRow.cacheId() != CU.UNDEFINED_CACHE_ID) {
                        updateRow.cacheId(CU.UNDEFINED_CACHE_ID);

                        rowStore.addRow(updateRow, grp.statisticsHolderData());

                        updateRow.cacheId(cctx.cacheId());
                    }
                    else
                        rowStore.addRow(updateRow, grp.statisticsHolderData());

                    boolean old = dataTree.putx(updateRow);

                    assert !old;

                    GridCacheQueryManager qryMgr = cctx.queries();

                    if (qryMgr.enabled())
                        qryMgr.store(updateRow, null, true);

                    cleanup0(cctx, updateRow.cleanupRows());
                }
            }
            finally {
                busyLock.leaveBusy();
            }
        }

        /**
         * @param cctx Cache context.
         * @param newRow New row.
         * @param oldRow Old row if available.
         * @throws IgniteCheckedException If failed.
         */
        private void finishUpdate(
            GridCacheContext cctx,
            CacheDataRow newRow,
            @Nullable CacheDataRow oldRow
        ) throws IgniteCheckedException {
            boolean oldTombstone = oldRow != null && oldRow.tombstone();
            boolean hasOldVal = oldRow != null && !oldRow.tombstone();

            if (!hasOldVal)
                incrementSize(cctx.cacheId());

            GridCacheQueryManager qryMgr = cctx.queries();

            if (qryMgr.enabled())
                qryMgr.store(newRow, hasOldVal ? oldRow : null, true);

            updatePendingEntries(cctx, newRow, hasOldVal ? oldRow : null);

            if (oldTombstone) {
                tombstoneRemoved();

                clearPendingEntries(cctx, oldRow);
            }

            if (isIncrementalDrEnabled(cctx)) {
                if (oldRow != null && oldRow.version().updateCounter() != 0)
                    removeFromLog(new UpdateLogRow(cctx.cacheId(), oldRow.version().updateCounter(), oldRow.link()));

                // Ignore entry initial value.
                if (newRow.version().updateCounter() != 0 && replicationRequire(newRow.version()))
                    addUpdateToLog(new UpdateLogRow(cctx.cacheId(), newRow.version().updateCounter(), newRow.link()));
            }

            if (oldRow != null) {
                assert oldRow.link() != 0 : oldRow;

                if (newRow.link() != oldRow.link())
                    rowStore.removeRow(oldRow.link(), grp.statisticsHolderData());
            }
        }

        /**
         * @param cctx Cache context.
         * @param newRow New row.
         * @param oldRow Old row.
         * @throws IgniteCheckedException If failed.
         */
        private void updatePendingEntries(GridCacheContext cctx, CacheDataRow newRow, @Nullable CacheDataRow oldRow)
            throws IgniteCheckedException
        {
            long expireTime = newRow.expireTime();

            int cacheId = grp.sharedGroup() ? cctx.cacheId() : CU.UNDEFINED_CACHE_ID;

            if (oldRow != null) {
                assert oldRow.link() != 0 : oldRow;

                if (pendingTree() != null && oldRow.expireTime() != 0)
                    pendingTree().removex(new PendingRow(cacheId, oldRow.tombstone(), oldRow.expireTime(), oldRow.link()));
            }

            if (pendingTree() != null && expireTime != 0) {
                pendingTree().putx(new PendingRow(cacheId, newRow.tombstone(), expireTime, newRow.link()));

                if (!cctx.ttl().hasPendingEntries(newRow.tombstone()))
                    cctx.ttl().setHasPendingEntries(newRow.tombstone());
            }
        }

        /** {@inheritDoc} */
        @Override public void remove(GridCacheContext cctx, KeyCacheObject key, int partId) throws IgniteCheckedException {
            if (!busyLock.enterBusy())
                throw new NodeStoppingException("Operation has been cancelled (node is stopping).");

            try {
                int cacheId = grp.sharedGroup() ? cctx.cacheId() : CU.UNDEFINED_CACHE_ID;

                assert cctx.shared().database().checkpointLockIsHeldByThread();

                CacheDataRow oldRow = dataTree.remove(new SearchRow(cacheId, key));

                if (oldRow != null)
                    finishRemove(cctx, key, oldRow, null);
            }
            finally {
                busyLock.leaveBusy();
            }
        }

        /** */
        private class RemoveWithTombstone implements IgniteCacheOffheapManager.OffheapInvokeClosure {
            /** */
            private final GridCacheContext cctx;

            /** */
            private final KeyCacheObject key;

            /** */
            private final GridCacheVersion ver;

            /** */
            private CacheDataRow oldRow;

            /** */
            private CacheDataRow newRow;

            /** */
            private IgniteTree.OperationType operationType;

            /**
             * @param cctx Context.
             * @param key Key.
             * @param ver Version.
             */
            RemoveWithTombstone(GridCacheContext cctx, KeyCacheObject key, GridCacheVersion ver) {
                this.cctx = cctx;
                this.key = key;
                this.ver = ver;
            }

            /** {@inheritDoc} */
            @Override public CacheDataRow oldRow() {
                return oldRow;
            }

            /** {@inheritDoc} */
            @Override public void call(@Nullable CacheDataRow oldRow) throws IgniteCheckedException {
                if (oldRow != null)
                    oldRow.key(key);

                this.oldRow = oldRow;

                // Always write tombstone (overwrite with new version if it existed before)
                newRow = createRow(cctx, key, TombstoneCacheObject.INSTANCE, ver,
                    cctx.shared().ttl().tombstoneExpireTime(), oldRow);

                operationType = oldRow != null && oldRow.link() == newRow.link() ?
                    IgniteTree.OperationType.IN_PLACE : IgniteTree.OperationType.PUT;
            }

            /** {@inheritDoc} */
            @Override public CacheDataRow newRow() {
                return newRow;
            }

            /** {@inheritDoc} */
            @Override public IgniteTree.OperationType operationType() {
                return operationType;
            }
        }

        /** {@inheritDoc} */
        @Override public void removeWithTombstone(
            GridCacheContext cctx,
            KeyCacheObject key,
            GridCacheVersion ver,
            GridDhtLocalPartition part
        ) throws IgniteCheckedException {
            if (!busyLock.enterBusy())
                throw new NodeStoppingException("Operation has been cancelled (node is stopping).");

            try {
                assert cctx.shared().database().checkpointLockIsHeldByThread();

                int cacheId = grp.sharedGroup() ? cctx.cacheId() : CU.UNDEFINED_CACHE_ID;

                RemoveWithTombstone c = new RemoveWithTombstone(cctx, key, ver);

                dataTree.invoke(new SearchRow(cacheId, key), CacheDataRowAdapter.RowData.NO_KEY, c);

                // assert c.operationType() == PUT || c.operationType() == IN_PLACE : c.operationType();

                if (c.operationType() != NOOP)
                    finishRemove(cctx, key, c.oldRow, c.newRow);
            }
            finally {
                busyLock.leaveBusy();
            }
        }

        /**
         * @param cctx Cache context.
         * @param key Key.
         * @param oldRow Removed row. Can be null for logical remove (ts write as first entry op).
         * @param tombstoneRow Tombstone row (if tombstone was created for remove). Not null means logical removal.
         * @throws IgniteCheckedException If failed.
         */
        private void finishRemove(
            GridCacheContext cctx,
            KeyCacheObject key,
            @Nullable CacheDataRow oldRow,
            @Nullable CacheDataRow tombstoneRow
        ) throws IgniteCheckedException {
            boolean oldTombstone = oldRow != null && oldRow.tombstone();
            boolean oldVal = oldRow != null && !oldRow.tombstone();

            if (oldVal) {
                clearPendingEntries(cctx, oldRow);

                decrementSize(cctx.cacheId());
            }

            GridCacheQueryManager qryMgr = cctx.queries();

            if (qryMgr.enabled())
                qryMgr.remove(key, oldVal ? oldRow : null);

            if (oldTombstone) {
                if (tombstoneRow == null) {
                    tombstoneRemoved();

                    clearPendingEntries(cctx, oldRow);

                    // On tombstone removal should move tombstone clear counter forward to avoid data desync.
                    grp.topology().localPartition(oldRow.partition()).dataStore().partUpdateCounter().
                        updateTombstoneClearCounter(oldRow.version().updateCounter());
                }
                else // Reindex tombstones to avoid a situation when PendingRow link points to invalid row.
                    updatePendingEntries(cctx, tombstoneRow, oldRow);
            }
            else if (tombstoneRow != null) {
                tombstoneCreated();

                updatePendingEntries(cctx, tombstoneRow, null);
            }

            if (isIncrementalDrEnabled(cctx)) {
                if (tombstoneRow != null && tombstoneRow.version().updateCounter() != 0 &&
                    replicationRequire(tombstoneRow.version()))
                    addUpdateToLog(new UpdateLogRow(cctx.cacheId(), tombstoneRow.version().updateCounter(), tombstoneRow.link()));

                if (oldRow != null && oldRow.version().updateCounter() != 0) {
                    if (oldTombstone && tombstoneRow == null)
                        cctx.dr().onTombstoneCleaned(partId, oldRow.version().updateCounter());

                    removeFromLog(new UpdateLogRow(cctx.cacheId(), oldRow.version().updateCounter(), oldRow.link()));
                }
            }

            if (oldRow != null && (tombstoneRow == null || tombstoneRow.link() != oldRow.link()))
                rowStore.removeRow(oldRow.link(), grp.statisticsHolderData());
        }

        /**
         * @param cctx Cache context.
         * @param oldRow Old row.
         * @throws IgniteCheckedException If failed.
         */
        private void clearPendingEntries(GridCacheContext cctx, CacheDataRow oldRow)
            throws IgniteCheckedException {
            int cacheId = grp.sharedGroup() ? cctx.cacheId() : CU.UNDEFINED_CACHE_ID;

            assert oldRow.link() != 0 : oldRow;
            assert cacheId == CU.UNDEFINED_CACHE_ID || oldRow.cacheId() == cacheId :
                "Incorrect cache ID [expected=" + cacheId + ", actual=" + oldRow.cacheId() + "].";

            if (pendingTree() != null && oldRow.expireTime() != 0)
                pendingTree().removex(new PendingRow(cacheId, oldRow.tombstone(), oldRow.expireTime(), oldRow.link()));
        }

        /** {@inheritDoc} */
        @Override public CacheDataRow find(GridCacheContext cctx, KeyCacheObject key) throws IgniteCheckedException {
            key = key.prepareForCache(cctx.cacheObjectContext(), false);

            int cacheId = grp.sharedGroup() ? cctx.cacheId() : CU.UNDEFINED_CACHE_ID;

            CacheDataRow row;

            if (grp.mvccEnabled()) {
                MvccFirstRowTreeClosure clo = new MvccFirstRowTreeClosure(cctx);

                dataTree.iterate(
                    new MvccMaxSearchRow(cacheId, key),
                    new MvccMinSearchRow(cacheId, key),
                    clo
                );

                row = clo.row();
            }
            else
                row = dataTree.findOne(new SearchRow(cacheId, key), CacheDataRowAdapter.RowData.NO_KEY);

            afterRowFound(row, key);

            return row;
        }

        /** {@inheritDoc} */
        @Override public List<IgniteBiTuple<Object, MvccVersion>> mvccFindAllVersions(
            GridCacheContext cctx,
            KeyCacheObject key)
            throws IgniteCheckedException
        {
            assert grp.mvccEnabled();

            // Note: this method is intended for testing only.

            key = key.prepareForCache(cctx.cacheObjectContext(), false);

            int cacheId = grp.sharedGroup() ? cctx.cacheId() : CU.UNDEFINED_CACHE_ID;

            GridCursor<CacheDataRow> cur = dataTree.find(
                new MvccMaxSearchRow(cacheId, key),
                new MvccMinSearchRow(cacheId, key)
            );

            List<IgniteBiTuple<Object, MvccVersion>> res = new ArrayList<>();

            long crd = MVCC_CRD_COUNTER_NA;
            long cntr = MVCC_COUNTER_NA;
            int opCntr = MVCC_OP_COUNTER_NA;

            while (cur.next()) {
                CacheDataRow row = cur.get();

                if (compareNewVersion(row, crd, cntr, opCntr) != 0) // deleted row
                    res.add(F.t(null, row.newMvccVersion()));

                res.add(F.t(row.value(), row.mvccVersion()));

                crd = row.mvccCoordinatorVersion();
                cntr = row.mvccCounter();
                opCntr = row.mvccOperationCounter();
            }

            return res;
        }

        /** {@inheritDoc} */
        @Override public GridCursor<CacheDataRow> mvccAllVersionsCursor(
            GridCacheContext cctx, KeyCacheObject key, CacheDataRowAdapter.RowData x
        ) throws IgniteCheckedException {
            int cacheId = cctx.cacheId();

            GridCursor<CacheDataRow> cursor = dataTree.find(
                new MvccMaxSearchRow(cacheId, key),
                new MvccMinSearchRow(cacheId, key),
                x);

            return cursor;
        }

        /** {@inheritDoc} */
        @Override public CacheDataRow mvccFind(GridCacheContext cctx,
            KeyCacheObject key,
            MvccSnapshot snapshot) throws IgniteCheckedException {
            key = key.prepareForCache(cctx.cacheObjectContext(), false);

            int cacheId = grp.sharedGroup() ? cctx.cacheId() : CU.UNDEFINED_CACHE_ID;

            MvccSnapshotSearchRow clo = new MvccSnapshotSearchRow(cctx, key, snapshot);

            dataTree.iterate(
                clo,
                new MvccMinSearchRow(cacheId, key),
                clo
            );

            CacheDataRow row = clo.row();

            afterRowFound(row, key);

            return row;
        }

        /**
         * @param row Row.
         * @param key Key.
         * @throws IgniteCheckedException If failed.
         */
        private void afterRowFound(@Nullable CacheDataRow row, KeyCacheObject key) throws IgniteCheckedException {
            if (row != null) {
                row.key(key);

                grp.dataRegion().evictionTracker().touchPage(row.link());
            }
        }

        /** {@inheritDoc} */
        @Override public GridCursor<? extends CacheDataRow> cursor(int flags) throws IgniteCheckedException {
            GridCursor<? extends CacheDataRow> cur = dataTree.find(null, null, null);

            return flags == DATA ? cursorSkipTombstone(cur) : flags == DATA_AND_TOMBSTONES ? cur : cursorSkipData(cur);
        }

        /** {@inheritDoc} */
        @Override public GridCursor<? extends CacheDataRow> cursor(CacheDataRowAdapter.RowData x) throws IgniteCheckedException {
            return dataTree.find(null, null, x);
        }

        /** {@inheritDoc} */
        @Override public GridCursor<? extends CacheDataRow> cursor(MvccSnapshot mvccSnapshot)
            throws IgniteCheckedException {
            assert grp.mvccEnabled();

            return dataTree.find(null, null,
                new MvccFirstVisibleRowTreeClosure(grp.singleCacheContext(), mvccSnapshot), null);
        }

        /** {@inheritDoc} */
        @Override public GridCursor<? extends CacheDataRow> cursor(int cacheId, int flags) throws IgniteCheckedException {
            return cursor(cacheId, null, null, null, null, flags);
        }

        /** {@inheritDoc} */
        @Override public GridCursor<? extends CacheDataRow> cursor(int cacheId,
            MvccSnapshot mvccSnapshot) throws IgniteCheckedException {
            return cursor(cacheId, null, null, null, mvccSnapshot, DATA);
        }

        /** {@inheritDoc} */
        @Override public GridCursor<? extends CacheDataRow> cursor(int cacheId, KeyCacheObject lower,
            KeyCacheObject upper) throws IgniteCheckedException {
            return cursor(cacheId, lower, upper, null);
        }

        /** {@inheritDoc} */
        @Override public GridCursor<? extends CacheDataRow> cursor(int cacheId, KeyCacheObject lower,
            KeyCacheObject upper, CacheDataRowAdapter.RowData x) throws IgniteCheckedException {
            return cursor(cacheId, lower, upper, null, null, DATA);
        }

        /** {@inheritDoc} */
        @Override public GridCursor<? extends CacheDataRow> cursor(int cacheId,
            KeyCacheObject lower,
            KeyCacheObject upper,
            CacheDataRowAdapter.RowData x,
            MvccSnapshot snapshot,
            int flags
        ) throws IgniteCheckedException {
            SearchRow lowerRow;
            SearchRow upperRow;

            if (grp.sharedGroup()) {
                assert cacheId != CU.UNDEFINED_CACHE_ID;

                lowerRow = lower != null ? new SearchRow(cacheId, lower) : new SearchRow(cacheId);
                upperRow = upper != null ? new SearchRow(cacheId, upper) : new SearchRow(cacheId);
            }
            else {
                lowerRow = lower != null ? new SearchRow(CU.UNDEFINED_CACHE_ID, lower) : null;
                upperRow = upper != null ? new SearchRow(CU.UNDEFINED_CACHE_ID, upper) : null;
            }

            GridCursor<? extends CacheDataRow> cursor;

            if (snapshot != null) {
                assert grp.mvccEnabled();

                GridCacheContext cctx = grp.sharedGroup() ? grp.shared().cacheContext(cacheId) : grp.singleCacheContext();

                cursor = dataTree.find(lowerRow, upperRow, new MvccFirstVisibleRowTreeClosure(cctx, snapshot), x);
            }
            else {
                cursor = dataTree.find(lowerRow, upperRow, x);

                if (flags == DATA)
                    cursor = cursorSkipTombstone(cursor);
                else if (flags == TOMBSTONES)
                    cursor = cursorSkipData(cursor);
            }

            return cursor;
        }

        /** {@inheritDoc} */
        @Override public void destroy() throws IgniteCheckedException {
            AtomicReference<IgniteCheckedException> exception = new AtomicReference<>();

            logTree.destroy();

            dataTree.destroy(row -> {
                try {
                    rowStore.removeRow(row.link(), grp.statisticsHolderData());
                }
                catch (IgniteCheckedException e) {
                    U.error(log, "Failed to remove row [link=" + row.link() + "]");

                    IgniteCheckedException ex = exception.get();

                    if (ex == null)
                        exception.set(e);
                    else
                        ex.addSuppressed(e);
                }
            }, false);

            if (exception.get() != null)
                throw new IgniteCheckedException("Failed to destroy store", exception.get());
        }

        /** {@inheritDoc} */
        @Override public void markDestroyed() {
            dataTree.markDestroyed();
        }

        /** {@inheritDoc} */
        @Override public boolean destroyed() {
            return dataTree.destroyed();
        }

        /** {@inheritDoc} */
        @Override public void clear(int cacheId) throws IgniteCheckedException {
            assert cacheId != CU.UNDEFINED_CACHE_ID;

            if (cacheSize(cacheId) == 0)
                return;

            Exception ex = null;

            // Tombstones require value, this can be optimized to avoid full row for non-tombstones.
            GridCursor<? extends CacheDataRow> cur =
                cursor(cacheId, null, null, CacheDataRowAdapter.RowData.FULL, null, DATA_AND_TOMBSTONES);

            int rmv = 0;

            while (cur.next()) {
                if (++rmv == BATCH_SIZE) {
                    grp.shared().database().checkpointReadUnlock();

                    rmv = 0;

                    grp.shared().database().checkpointReadLock();
                }

                CacheDataRow row = cur.get();

                assert row.link() != 0 : row;

                try {
                    boolean res = dataTree.removex(row);

                    assert res : row;

                    rowStore.removeRow(row.link(), grp.statisticsHolderData());

                    if (!row.tombstone())
                        decrementSize(cacheId);
                }
                catch (IgniteCheckedException e) {
                    U.error(log, "Fail remove row [link=" + row.link() + "]");

                    if (ex == null)
                        ex = e;
                    else
                        ex.addSuppressed(e);
                }
            }

            if (ex != null)
                throw new IgniteCheckedException("Fail destroy store", ex);

            // Allow checkpointer to progress if a partition contains less than BATCH_SIZE keys.
            if (rmv > 0) {
                grp.shared().database().checkpointReadUnlock();

                grp.shared().database().checkpointReadLock();
            }
        }

        /** {@inheritDoc} */
        @Override public RowStore rowStore() {
            return rowStore;
        }

        /** {@inheritDoc} */
        @Override public void setRowCacheCleaner(GridQueryRowCacheCleaner rowCacheCleaner) {
            this.rowCacheCleaner = rowCacheCleaner;
        }

        /**
         * @param size Size to init.
         * @param updCntr Update counter.
         * @param cacheSizes Cache sizes if store belongs to group containing multiple caches.
         * @param updCntrGapsData Update counters gaps raw data.
         * @param tombstonesCnt Tombstones count.
         */
        public void restoreState(
            long size,
            long updCntr,
            Map<Integer, Long> cacheSizes,
            byte[] updCntrGapsData,
            long tombstonesCnt
        ) {
            pCntr.init(updCntr, updCntrGapsData);

            storageSize.set(size);

            for (Map.Entry<Integer, Long> e : cacheSizes.entrySet())
                this.cacheSizes.put(e.getKey(), new AtomicLong(e.getValue()));

            this.tombstonesCnt.set(tombstonesCnt);
        }

        /** {@inheritDoc} */
        @Override public PartitionLogTree logTree() {
            return logTree;
        }

        /**
         * Remove row from partition log.
         *
         * @param row Log row.
         * @throws IgniteCheckedException If failed.
         */
        private void removeFromLog(UpdateLogRow row) throws IgniteCheckedException {
            assert row.updateCounter() > 0;

            logTree.remove(row);
        }

        /**
         * Add row to partition log.
         *
         * @param row Log row.
         * @throws IgniteCheckedException If failed.
         */
        private void addUpdateToLog(UpdateLogRow row) throws IgniteCheckedException {
            assert row.updateCounter() > 0;

            logTree.putx(row);
        }

        /** {@inheritDoc} */
        @Override public PendingEntriesTree pendingTree() {
            return pendingEntries.get();
        }

        /** {@inheritDoc} */
        @Override public void preload() throws IgniteCheckedException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void resetUpdateCounter() {
            pCntr.reset();
        }

        /** {@inheritDoc} */
        @Override public void resetInitialUpdateCounter() {
            pCntr.resetInitialCounter();
        }

        /** {@inheritDoc} */
        @Override public PartitionMetaStorage<SimpleDataRow> partStorage() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public long tombstonesCount() {
            return tombstonesCnt.get();
        }

        /**
         * Called when tombstone has removed from partition.
         */
        @Override public void tombstoneRemoved() {
            tombstonesCnt.decrementAndGet();
        }

        /**
         * Called when tombstone has created in partition.
         */
        @Override public void tombstoneCreated() {
            tombstonesCnt.incrementAndGet();
        }

        /**
         * @param cur Cursor.
         * @return Cursor skipping non-tombstone entries.
         */
        private GridCursor<? extends CacheDataRow> cursorSkipData(GridCursor<? extends CacheDataRow> cur) {
            if (!grp.supportsTombstone())
                return cur;

            return new GridCursor<CacheDataRow>() {
                /** */
                CacheDataRow next;

                /** {@inheritDoc} */
                @Override public boolean next() throws IgniteCheckedException {
                    while (cur.next()) {
                        CacheDataRow next = cur.get();

                        // If request cursor with RowData.TOMBSTONES, then for non-tombtones all fields are null.
                        if (next.tombstone()) {
                            this.next = next;

                            return true;
                        }
                    }

                    return false;
                }

                /** {@inheritDoc} */
                @Override public CacheDataRow get() {
                    return next;
                }

                /** {@inheritDoc} */
                @Override public void close() throws Exception {
                    cur.close();
                }
            };
        }

        /**
         * @param cur Cursor.
         * @return Cursor skipping tombstone entries.
         */
        private GridCursor<? extends CacheDataRow> cursorSkipTombstone(GridCursor<? extends CacheDataRow> cur) {
            if (!grp.supportsTombstone())
                return cur;

            return new GridCursor<CacheDataRow>() {
                /** */
                CacheDataRow next;

                /** {@inheritDoc} */
                @Override public boolean next() throws IgniteCheckedException {
                    while (cur.next()) {
                        CacheDataRow next = cur.get();

                        if (!next.tombstone()) {
                            this.next = next;

                            return true;
                        }
                    }

                    return false;
                }

                /** {@inheritDoc} */
                @Override public CacheDataRow get() {
                    return next;
                }

                /** {@inheritDoc} */
                @Override public void close() throws Exception {
                    cur.close();
                }
            };
        }

        /**
         * Isolated method to get length of IGFS block.
         *
         * @param cctx Cache context.
         * @param val Value.
         * @return Length of value.
         */
        private int valueLength(GridCacheContext cctx, @Nullable CacheObject val) {
            if (val == null)
                return 0;

            byte[] bytes = val.value(cctx.cacheObjectContext(), false);

            if (bytes != null)
                return bytes.length;
            else
                return 0;
        }

        /** */
        private final class MvccFirstVisibleRowTreeClosure implements MvccTreeClosure, MvccDataPageClosure {
            /** */
            private final GridCacheContext cctx;

            /** */
            private final MvccSnapshot snapshot;

            /**
             *
             * @param cctx Cache context.
             * @param snapshot MVCC snapshot.
             */
            MvccFirstVisibleRowTreeClosure(GridCacheContext cctx, MvccSnapshot snapshot) {
                this.cctx = cctx;
                this.snapshot = snapshot;
            }

            /** {@inheritDoc} */
            @Override public boolean apply(BPlusTree<CacheSearchRow, CacheDataRow> tree, BPlusIO<CacheSearchRow> io,
                long pageAddr, int idx) throws IgniteCheckedException {
                RowLinkIO rowIo = (RowLinkIO)io;

                long rowCrdVer = rowIo.getMvccCoordinatorVersion(pageAddr, idx);
                long rowCntr = rowIo.getMvccCounter(pageAddr, idx);
                int rowOpCntr = rowIo.getMvccOperationCounter(pageAddr, idx);

                assert mvccVersionIsValid(rowCrdVer, rowCntr, rowOpCntr);

                return isVisible(cctx, snapshot, rowCrdVer, rowCntr, rowOpCntr, rowIo.getLink(pageAddr, idx));
            }

            /** {@inheritDoc} */
            @Override public boolean applyMvcc(DataPageIO io, long dataPageAddr, int itemId, int pageSize)
                throws IgniteCheckedException {
                try {
                    return isVisible(cctx, snapshot, io, dataPageAddr, itemId, pageSize);
                }
                catch (IgniteTxUnexpectedStateCheckedException e) {
                    // TODO this catch must not be needed if we switch Vacuum to data page scan
                    // We expect the active tx state can be observed by read tx only in the cases when tx has been aborted
                    // asynchronously and node hasn't received finish message yet but coordinator has already removed it from
                    // the active txs map. Rows written by this tx are invisible to anyone and will be removed by the vacuum.
                    if (log.isDebugEnabled())
                        log.debug( "Unexpected tx state on index lookup. " + X.getFullStackTrace(e));

                    return false;
                }
            }
        }

        /**
         *
         */
        private class MvccUpdateRowWithPreloadInfoClosure extends MvccDataRow implements OffheapInvokeClosure {
            /** */
            private CacheDataRow oldRow;

            /** */
            private IgniteTree.OperationType op;

            /**
             * @param cctx Cache context.
             * @param key Key.
             * @param val Value.
             * @param ver Version.
             * @param expireTime Expire time.
             * @param mvccVer Mvcc created version.
             * @param newMvccVer Mvcc updated version.
             * @param mvccTxState Mvcc Tx state hint.
             * @param newMvccTxState New Mvcc Tx state hint.
             */
            MvccUpdateRowWithPreloadInfoClosure(GridCacheContext cctx,
                KeyCacheObject key,
                @Nullable CacheObject val,
                GridCacheVersion ver,
                long expireTime,
                MvccVersion mvccVer,
                MvccVersion newMvccVer,
                byte mvccTxState,
                byte newMvccTxState) {
                super(key,
                    val,
                    ver,
                    partId,
                    expireTime,
                    grp.sharedGroup() ? cctx.cacheId() : CU.UNDEFINED_CACHE_ID,
                    mvccVer,
                    newMvccVer);

                mvccTxState(mvccTxState);
                newMvccTxState(newMvccTxState);
            }

            /** {@inheritDoc} */
            @Nullable @Override public CacheDataRow oldRow() {
                return oldRow;
            }

            /** {@inheritDoc} */
            @Override public void call(@Nullable CacheDataRow oldRow) throws IgniteCheckedException {
                this.oldRow = oldRow;

                if (oldRow == null) {
                    op = PUT;

                    int cacheId = cacheId();

                    if (!grp.storeCacheIdInDataPage() && cacheId != CU.UNDEFINED_CACHE_ID)
                        cacheId(CU.UNDEFINED_CACHE_ID);

                    rowStore().addRow(this, grp.statisticsHolderData());

                    cacheId(cacheId);
                }
                else {
                    op = NOOP;

                    if (oldRow.mvccTxState() != mvccTxState() ||
                        oldRow.newMvccCoordinatorVersion() != newMvccCoordinatorVersion() ||
                        oldRow.newMvccCounter() != newMvccCounter() ||
                        oldRow.newMvccOperationCounter() != newMvccOperationCounter() ||
                        oldRow.newMvccTxState() != newMvccTxState()) {

                        rowStore().updateDataRow(oldRow.link(), mvccApplyChanges, this, grp.statisticsHolderData());
                    }
                }
            }

            /** {@inheritDoc} */
            @Override public CacheDataRow newRow() {
                return op == PUT ? this : null;
            }

            /** {@inheritDoc} */
            @Override public IgniteTree.OperationType operationType() {
                return op == null ? NOOP : op;
            }
        }

        /**
         *  Check if any row version exists.
         */
        private final class CheckHistoryExistsClosure implements BPlusTree.TreeRowClosure<CacheSearchRow, CacheDataRow> {
            /** */
            private boolean found;

            /** {@inheritDoc} */
            @Override public boolean apply(BPlusTree<CacheSearchRow, CacheDataRow> tree, BPlusIO<CacheSearchRow> io,
                long pageAddr, int idx) {
                found = true;

                return false;  // Stop search.
            }

            /**
             * @return {@code True} if any row was found, {@code False} otherwise.
             */
            public boolean found() {
                return found;
            }
        }
    }

    /**
     * Mvcc remove handler.
     */
    private static final class MvccMarkUpdatedHandler extends PageHandler<MvccUpdateDataRow, Boolean> {
        /** */
        private final CacheGroupContext grp;

        private MvccMarkUpdatedHandler(CacheGroupContext grp) {
            this.grp = grp;
        }

        /** {@inheritDoc} */
        @Override public Boolean run(int cacheId, long pageId, long page, long pageAddr, PageIO io, Boolean walPlc,
            MvccUpdateDataRow updateDataRow, int itemId, IoStatisticsHolder statHolder) throws IgniteCheckedException {
            assert grp.mvccEnabled();

            PageMemory pageMem = grp.dataRegion().pageMemory();
            IgniteWriteAheadLogManager wal = grp.shared().wal();

            DataPageIO iox = (DataPageIO)io;

            int off = iox.getPayloadOffset(pageAddr, itemId,
                pageMem.realPageSize(grp.groupId()), MVCC_INFO_SIZE);

            long newCrd = iox.newMvccCoordinator(pageAddr, off);
            long newCntr = iox.newMvccCounter(pageAddr, off);
            int newOpCntr = iox.rawNewMvccOperationCounter(pageAddr, off);

            assert newCrd == MVCC_CRD_COUNTER_NA || state(grp, newCrd, newCntr, newOpCntr) == TxState.ABORTED;

            int keyAbsentBeforeFlag = updateDataRow.isKeyAbsentBefore() ? (1 << MVCC_KEY_ABSENT_BEFORE_OFF) : 0;

            iox.updateNewVersion(pageAddr, off, updateDataRow.mvccCoordinatorVersion(), updateDataRow.mvccCounter(),
                updateDataRow.mvccOperationCounter() | keyAbsentBeforeFlag, TxState.NA);

            if (isWalDeltaRecordNeeded(pageMem, cacheId, pageId, page, wal, walPlc))
                wal.log(new DataPageMvccMarkUpdatedRecord(cacheId, pageId, itemId,
                    updateDataRow.mvccCoordinatorVersion(), updateDataRow.mvccCounter(), updateDataRow.mvccOperationCounter()));

            return TRUE;
        }
    }

    /**
     * Mvcc update operation counter hints handler.
     */
    private static final class MvccUpdateTxStateHintHandler extends PageHandler<Void, Boolean> {
        /** */
        private final CacheGroupContext grp;

        /** */
        private MvccUpdateTxStateHintHandler(CacheGroupContext grp) {
            this.grp = grp;
        }

        /** {@inheritDoc} */
        @Override public Boolean run(int cacheId, long pageId, long page, long pageAddr, PageIO io,
            Boolean walPlc, Void ignore, int itemId, IoStatisticsHolder statHolder) throws IgniteCheckedException {

            DataPageIO iox = (DataPageIO)io;

            PageMemory pageMem = grp.dataRegion().pageMemory();
            IgniteWriteAheadLogManager wal = grp.shared().wal();

            int off = iox.getPayloadOffset(pageAddr, itemId,
                pageMem.realPageSize(grp.groupId()), MVCC_INFO_SIZE);

            long crd = iox.mvccCoordinator(pageAddr, off);
            long cntr = iox.mvccCounter(pageAddr, off);
            int opCntr = iox.rawMvccOperationCounter(pageAddr, off);
            byte txState = (byte)(opCntr >>> MVCC_HINTS_BIT_OFF);

            if (txState == TxState.NA) {
                byte state = state(grp, crd, cntr, opCntr);

                if (state == TxState.COMMITTED || state == TxState.ABORTED) {
                    iox.rawMvccOperationCounter(pageAddr, off, opCntr | (state << MVCC_HINTS_BIT_OFF));

                    if (isWalDeltaRecordNeeded(pageMem, cacheId, pageId, page, wal, walPlc))
                        wal.log(new DataPageMvccUpdateTxStateHintRecord(cacheId, pageId, itemId, state));
                }
                else
                    throw unexpectedStateException(grp, state, crd, cntr, opCntr);
            }

            long newCrd = iox.newMvccCoordinator(pageAddr, off);
            long newCntr = iox.newMvccCounter(pageAddr, off);
            int newOpCntr = iox.rawNewMvccOperationCounter(pageAddr, off);
            byte newTxState = (byte)(newOpCntr >>> MVCC_HINTS_BIT_OFF);

            if (newCrd != MVCC_CRD_COUNTER_NA && newTxState == TxState.NA) {
                byte state = state(grp, newCrd, newCntr, newOpCntr);

                if (state == TxState.COMMITTED || state == TxState.ABORTED) {
                    iox.rawNewMvccOperationCounter(pageAddr, off, newOpCntr | (state << MVCC_HINTS_BIT_OFF));

                    if (isWalDeltaRecordNeeded(pageMem, cacheId, pageId, page, wal, walPlc))
                        wal.log(new DataPageMvccUpdateNewTxStateHintRecord(cacheId, pageId, itemId, state));
                }

                // We do not throw an exception here because new version may be updated by active Tx at this moment.
            }

            return TRUE;
        }
    }

    /**
     * Applies changes to the row.
     */
    private static final class MvccApplyChangesHandler extends PageHandler<MvccDataRow, Boolean> {
        /** */
        private final CacheGroupContext grp;

        /** */
        private MvccApplyChangesHandler(CacheGroupContext grp) {
            this.grp = grp;
        }

        /** {@inheritDoc} */
        @Override public Boolean run(int cacheId, long pageId, long page, long pageAddr, PageIO io, Boolean walPlc,
            MvccDataRow newRow, int itemId, IoStatisticsHolder statHolder) throws IgniteCheckedException {
            assert grp.mvccEnabled();

            DataPageIO iox = (DataPageIO)io;

            PageMemory pageMem = grp.dataRegion().pageMemory();
            IgniteWriteAheadLogManager wal = grp.shared().wal();

            int off = iox.getPayloadOffset(pageAddr, itemId,
                pageMem.realPageSize(grp.groupId()), MVCC_INFO_SIZE);

            long crd = iox.mvccCoordinator(pageAddr, off);
            long cntr = iox.mvccCounter(pageAddr, off);
            int opCntrAndHint = iox.rawMvccOperationCounter(pageAddr, off);
            int opCntr = opCntrAndHint & MVCC_OP_COUNTER_MASK;
            byte txState = (byte)(opCntrAndHint >>> MVCC_HINTS_BIT_OFF);

            long newCrd = iox.newMvccCoordinator(pageAddr, off);
            long newCntr = iox.newMvccCounter(pageAddr, off);
            int newOpCntrAndHint = iox.rawNewMvccOperationCounter(pageAddr, off);
            int newOpCntr = newOpCntrAndHint & MVCC_OP_COUNTER_MASK;
            byte newTxState = (byte)(newOpCntrAndHint >>> MVCC_HINTS_BIT_OFF);

            assert crd == newRow.mvccCoordinatorVersion();
            assert cntr == newRow.mvccCounter();
            assert opCntr == newRow.mvccOperationCounter();

            assert newRow.mvccTxState() != TxState.NA : newRow.mvccTxState();

            if (txState != newRow.mvccTxState() && newRow.mvccTxState() != TxState.NA) {
                assert txState == TxState.NA : txState;

                iox.rawMvccOperationCounter(pageAddr, off, opCntr | (newRow.mvccTxState() << MVCC_HINTS_BIT_OFF));

                if (isWalDeltaRecordNeeded(pageMem, cacheId, pageId, page, wal, walPlc))
                    wal.log(new DataPageMvccUpdateTxStateHintRecord(cacheId, pageId, itemId, newRow.mvccTxState()));
            }

            if (compare(newCrd,
                newCntr,
                newOpCntr,
                newRow.newMvccCoordinatorVersion(),
                newRow.newMvccCounter(),
                newRow.newMvccOperationCounter()) != 0) {

                assert newRow.newMvccTxState() == TxState.NA || newRow.newMvccCoordinatorVersion() != MVCC_CRD_COUNTER_NA;

                iox.updateNewVersion(pageAddr, off, newRow.newMvccCoordinatorVersion(), newRow.newMvccCounter(),
                    newRow.newMvccOperationCounter(), newRow.newMvccTxState());

                if (isWalDeltaRecordNeeded(pageMem, cacheId, pageId, page, wal, walPlc))
                    wal.log(new DataPageMvccMarkUpdatedRecord(cacheId, pageId, itemId,
                        newRow.newMvccCoordinatorVersion(), newRow.newMvccCounter(),
                        newRow.newMvccOperationCounter() | (newRow.newMvccTxState() << MVCC_HINTS_BIT_OFF)));
            }
            else if (newTxState != newRow.newMvccTxState() && newRow.newMvccTxState() != TxState.NA) {
                assert newTxState == TxState.NA : newTxState;

                iox.rawNewMvccOperationCounter(pageAddr, off, newOpCntr | (newRow.newMvccTxState() << MVCC_HINTS_BIT_OFF));

                if (isWalDeltaRecordNeeded(pageMem, cacheId, pageId, page, wal, walPlc))
                    wal.log(new DataPageMvccUpdateNewTxStateHintRecord(cacheId, pageId, itemId, newRow.newMvccTxState()));
            }

            return TRUE;
        }
    }
}
