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

package org.apache.ignite.internal.processors.cache;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;
import javax.cache.Cache;
import javax.cache.expiry.ExpiryPolicy;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorResult;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.SystemProperty;
import org.apache.ignite.cache.CacheInterceptor;
import org.apache.ignite.cache.eviction.EvictableEntry;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.NodeStoppingException;
import org.apache.ignite.internal.UnregisteredBinaryTypeException;
import org.apache.ignite.internal.UnregisteredClassException;
import org.apache.ignite.internal.pagemem.wal.WALPointer;
import org.apache.ignite.internal.pagemem.wal.record.DataEntry;
import org.apache.ignite.internal.pagemem.wal.record.DataRecord;
import org.apache.ignite.internal.pagemem.wal.record.MvccDataEntry;
import org.apache.ignite.internal.pagemem.wal.record.MvccDataRecord;
import org.apache.ignite.internal.pagemem.wal.record.OutOfOrderDataRecord;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheUpdateAtomicResult.UpdateOutcome;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtCacheEntry;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxLocalAdapter;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridDhtAtomicAbstractUpdateFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtInvalidPartitionException;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearCacheEntry;
import org.apache.ignite.internal.processors.cache.extras.GridCacheEntryExtras;
import org.apache.ignite.internal.processors.cache.extras.GridCacheMvccEntryExtras;
import org.apache.ignite.internal.processors.cache.extras.GridCacheObsoleteEntryExtras;
import org.apache.ignite.internal.processors.cache.extras.GridCacheTtlEntryExtras;
import org.apache.ignite.internal.processors.cache.mvcc.MvccSnapshot;
import org.apache.ignite.internal.processors.cache.mvcc.MvccUtils;
import org.apache.ignite.internal.processors.cache.mvcc.MvccVersion;
import org.apache.ignite.internal.processors.cache.mvcc.txlog.TxState;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRowAdapter;
import org.apache.ignite.internal.processors.cache.persistence.DataRegion;
import org.apache.ignite.internal.processors.cache.persistence.StorageException;
import org.apache.ignite.internal.processors.cache.query.continuous.CacheContinuousQueryListener;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxEntry;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxKey;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxLocalAdapter;
import org.apache.ignite.internal.processors.cache.transactions.TxCounters;
import org.apache.ignite.internal.processors.cache.tree.mvcc.data.MvccUpdateResult;
import org.apache.ignite.internal.processors.cache.tree.mvcc.data.ResultType;
import org.apache.ignite.internal.processors.cache.version.GridCacheLazyPlainVersionedEntry;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersionConflictContext;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersionEx;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersionedEntryEx;
import org.apache.ignite.internal.processors.dr.GridDrType;
import org.apache.ignite.internal.processors.platform.PlatformProcessor;
import org.apache.ignite.internal.processors.query.schema.SchemaIndexCacheVisitorClosure;
import org.apache.ignite.internal.transactions.IgniteTxDuplicateKeyCheckedException;
import org.apache.ignite.internal.transactions.IgniteTxSerializationCheckedException;
import org.apache.ignite.internal.util.IgniteTree;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.lang.GridClosureException;
import org.apache.ignite.internal.util.lang.GridCursor;
import org.apache.ignite.internal.util.lang.GridMetadataAwareAdapter;
import org.apache.ignite.internal.util.lang.GridTuple;
import org.apache.ignite.internal.util.lang.GridTuple3;
import org.apache.ignite.internal.util.tostring.GridToStringBuilder;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.T3;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.thread.IgniteThread;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.IgniteSystemProperties.getLong;
import static org.apache.ignite.events.EventType.EVT_CACHE_OBJECT_EXPIRED;
import static org.apache.ignite.events.EventType.EVT_CACHE_OBJECT_LOCKED;
import static org.apache.ignite.events.EventType.EVT_CACHE_OBJECT_PUT;
import static org.apache.ignite.events.EventType.EVT_CACHE_OBJECT_READ;
import static org.apache.ignite.events.EventType.EVT_CACHE_OBJECT_REMOVED;
import static org.apache.ignite.events.EventType.EVT_CACHE_OBJECT_UNLOCKED;
import static org.apache.ignite.internal.processors.cache.GridCacheOperation.CREATE;
import static org.apache.ignite.internal.processors.cache.GridCacheOperation.DELETE;
import static org.apache.ignite.internal.processors.cache.GridCacheOperation.READ;
import static org.apache.ignite.internal.processors.cache.GridCacheOperation.TRANSFORM;
import static org.apache.ignite.internal.processors.cache.GridCacheOperation.UPDATE;
import static org.apache.ignite.internal.processors.cache.GridCacheUpdateAtomicResult.UpdateOutcome.INVOKE_NO_OP;
import static org.apache.ignite.internal.processors.cache.GridCacheUpdateAtomicResult.UpdateOutcome.REMOVE_NO_VAL;
import static org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState.RENTING;
import static org.apache.ignite.internal.processors.cache.mvcc.MvccUtils.MVCC_MAX_SNAPSHOT;
import static org.apache.ignite.internal.processors.cache.mvcc.MvccUtils.compareIgnoreOpCounter;
import static org.apache.ignite.internal.processors.cache.persistence.CacheDataRowAdapter.RowData.NO_KEY;
import static org.apache.ignite.internal.processors.dr.GridDrType.DR_NONE;
import static org.apache.ignite.internal.util.tostring.GridToStringBuilder.SensitiveDataLogging.HASH;
import static org.apache.ignite.internal.util.tostring.GridToStringBuilder.SensitiveDataLogging.PLAIN;

/**
 * Adapter for cache entry.
 */
@SuppressWarnings({"TooBroadScope"})
public abstract class GridCacheMapEntry extends GridMetadataAwareAdapter implements GridCacheEntryEx {
    /** */
    public static final GridCacheAtomicVersionComparator ATOMIC_VER_COMPARATOR = new GridCacheAtomicVersionComparator();

    /** @see GridCacheMapEntry#ENTRY_LOCK_TIMEOUT */
    public static final int DFLT_LOCK_TIMEOUT_ENV = 1000;

    /** Property name for entry lock timeout in milliseconds. Default is 1000. */
    @SystemProperty(value = "Sets the entry's lock timeout in milliseconds",
        type = Long.class, defaults = "" + DFLT_LOCK_TIMEOUT_ENV)
    public static final String ENTRY_LOCK_TIMEOUT_ENV = "ENTRY_LOCK_TIMEOUT";

    /** Entry lock time awaiting. */
    private static final long ENTRY_LOCK_TIMEOUT = getLong(ENTRY_LOCK_TIMEOUT_ENV, DFLT_LOCK_TIMEOUT_ENV);

    /** */
    private static final byte IS_UNSWAPPED_MASK = 0x01;

    /** */
    private static final byte IS_EVICT_DISABLED = 0x02;

    /**
     * NOTE
     * <br/>
     * ====
     * <br/>
     * Make sure to recalculate this value any time when adding or removing fields from entry.
     * The size should be count as follows:
     * <ul>
     * <li>Primitives: byte/boolean = 1, short = 2, int/float = 4, long/double = 8</li>
     * <li>References: 8 each</li>
     * <li>Each nested object should be analyzed in the same way as above.</li>
     * </ul>
     * ====
     * <br/>
     * <ul>
     *     <li>Reference fields:<ul>
     *         <li>8 : {@link #cctx}</li>
     *         <li>8 : {@link #key}</li>
     *         <li>8 : {@link #val}</li>
     *         <li>8 : {@link #ver}</li>
     *         <li>8 : {@link #extras}</li>
     *         <li>8 : {@link #lock}</li>
     *         <li>8 : {@link #listenerLock}</li>
     *         <li>8 : {@link GridMetadataAwareAdapter#data}</li>
     *     </ul></li>
     *     <li>Primitive fields:<ul>
     *         <li>4 : {@link #hash}</li>
     *         <li>1 : {@link #flags}</li>
     *     </ul></li>
     *     <li>Extras:<ul>
     *         <li>8 : {@link GridCacheEntryExtras#ttl()}</li>
     *         <li>8 : {@link GridCacheEntryExtras#expireTime()}</li>
     *     </ul></li>
     *     <li>Version:<ul>
     *         <li>4 : {@link GridCacheVersion#topVer}</li>
     *         <li>4 : {@link GridCacheVersion#nodeOrderDrId}</li>
     *         <li>8 : {@link GridCacheVersion#order}</li>
     *     </ul></li>
     *     <li>Key:<ul>
     *         <li>8 : {@link CacheObjectAdapter#val}</li>
     *         <li>8 : {@link CacheObjectAdapter#valBytes}</li>
     *         <li>4 : {@link KeyCacheObjectImpl#part}</li>
     *     </ul></li>
     *     <li>Value:<ul>
     *         <li>8 : {@link CacheObjectAdapter#val}</li>
     *         <li>8 : {@link CacheObjectAdapter#valBytes}</li>
     *     </ul></li>
     * </ul>
     */
    private static final int SIZE_OVERHEAD = 8 * 8 /* references */ + 5 /* primitives */ + 16 /* extras */
        + 16 /* version */ + 20 /* key */ + 16 /* value */;

    /** Static logger to avoid re-creation. Made static for test purpose. */
    protected static final AtomicReference<IgniteLogger> logRef = new AtomicReference<>();

    /** Logger. */
    protected static volatile IgniteLogger log;

    /** Cache registry. */
    @GridToStringExclude
    protected final GridCacheContext<?, ?> cctx;

    /** Key. */
    @GridToStringInclude(sensitive = true)
    protected final KeyCacheObject key;

    /** Value. */
    @GridToStringInclude(sensitive = true)
    protected CacheObject val;

    /** Version. */
    @GridToStringInclude
    protected GridCacheVersion ver;

    /** Key hash code. */
    @GridToStringInclude
    private final int hash;

    /** Extras */
    @GridToStringInclude
    private GridCacheEntryExtras extras;

    /** */
    @GridToStringExclude
    private final ReentrantLock lock = new ReentrantLock();

    /** Read Lock for continuous query listener */
    @GridToStringExclude
    private final ReadWriteLock listenerLock;

    /**
     * Flags:
     * <ul>
     *     <li>Unswapped flag - mask {@link #IS_UNSWAPPED_MASK}</li>
     *     <li>Evict disabled flag - mask {@link #IS_EVICT_DISABLED}</li>
     * </ul>
     */
    @GridToStringInclude
    protected byte flags;

    /**
     * @param cctx Cache context.
     * @param key Cache key.
     */
    protected GridCacheMapEntry(
        GridCacheContext<?, ?> cctx,
        KeyCacheObject key
    ) {
        if (log == null)
            log = U.logger(cctx.kernalContext(), logRef, GridCacheMapEntry.class);

        assert key != null;

        try {
            key = key.prepareForCache(cctx.cacheObjectContext(), cctx.cacheObjectContext().compressKeys());
        }
        catch (IgniteCheckedException ex) {
            throw new IgniteException(ex);
        }

        this.key = key;
        this.hash = key.hashCode();
        this.cctx = cctx;
        this.listenerLock = cctx.group().listenerLock();

        ver = cctx.shared().versions().startVersion();
    }

    /**
     * Sets entry value. If off-heap value storage is enabled, will serialize value to off-heap.
     *
     * @param val Value to store.
     */
    protected void value(@Nullable CacheObject val) {
        assert lock.isHeldByCurrentThread();

        this.val = val;
    }

    /** {@inheritDoc} */
    @Override public int memorySize() throws IgniteCheckedException {
        byte[] kb;
        byte[] vb = null;

        int extrasSize;

        lockEntry();

        try {
            key.prepareMarshal(cctx.cacheObjectContext());

            kb = key.valueBytes(cctx.cacheObjectContext());

            if (val != null) {
                val.prepareMarshal(cctx.cacheObjectContext());

                vb = val.valueBytes(cctx.cacheObjectContext());
            }

            extrasSize = extrasSize();
        }
        finally {
            unlockEntry();
        }

        return SIZE_OVERHEAD + extrasSize + kb.length + (vb == null ? 1 : vb.length);
    }

    /** {@inheritDoc} */
    @Override public boolean isInternal() {
        return key.internal();
    }

    /** {@inheritDoc} */
    @Override public boolean isDht() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean isLocal() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean isMvcc() {
        return cctx.mvccEnabled();
    }

    /** {@inheritDoc} */
    @Override public boolean isNear() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean isReplicated() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean detached() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public <K, V> GridCacheContext<K, V> context() {
        return (GridCacheContext<K, V>)cctx;
    }

    /** {@inheritDoc} */
    @Override public boolean isNew() throws GridCacheEntryRemovedException {
        assert lock.isHeldByCurrentThread();

        checkObsolete();

        return isStartVersion();
    }

    /** {@inheritDoc} */
    @Override public boolean isNewLocked() throws GridCacheEntryRemovedException {
        lockEntry();

        try {
            checkObsolete();

            return isStartVersion();
        }
        finally {
            unlockEntry();
        }
    }

    /**
     * @return {@code True} if start version.
     */
    public boolean isStartVersion() {
        return cctx.shared().versions().isStartVersion(ver);
    }

    /** {@inheritDoc} */
    @Override public boolean valid(AffinityTopologyVersion topVer) {
        return true;
    }

    /** {@inheritDoc} */
    @Override public int partition() {
        return 0;
    }

    /**
     * @return Local partition that owns this entry.
     */
    protected GridDhtLocalPartition localPartition() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean partitionValid() {
        return true;
    }

    /** {@inheritDoc} */
    @Nullable @Override public GridCacheEntryInfo info() {
        GridCacheEntryInfo info = null;

        lockEntry();

        try {
            if (!obsolete()) {
                info = new GridCacheEntryInfo();

                info.key(key);
                info.cacheId(cctx.cacheId());

                long expireTime = expireTimeExtras();

                boolean expired = expireTime != 0 && expireTime <= U.currentTimeMillis();

                info.ttl(ttlExtras());
                info.expireTime(expireTime);
                info.version(ver);
                info.setNew(isStartVersion());
                info.setDeleted(deletedUnlocked());

                if (!expired)
                    info.value(val);
            }
        }
        finally {
            unlockEntry();
        }

        return info;
    }

    /** {@inheritDoc} */
    @Nullable @Override public List<GridCacheEntryInfo> allVersionsInfo() throws IgniteCheckedException {
        assert cctx.mvccEnabled();

        lockEntry();

        try {
            if (obsolete())
                return Collections.emptyList();

            GridCursor<? extends CacheDataRow> cur =
                cctx.offheap().dataStore(localPartition()).mvccAllVersionsCursor(cctx, key, NO_KEY);

            List<GridCacheEntryInfo> res = new ArrayList<>();

            while (cur.next()) {
                CacheDataRow row = cur.get();

                GridCacheMvccEntryInfo info = new GridCacheMvccEntryInfo();

                info.key(key);
                info.value(row.value());
                info.cacheId(cctx.cacheId());
                info.version(row.version());
                info.setNew(false);
                info.setDeleted(false);

                byte txState = row.mvccTxState() != TxState.NA ? row.mvccTxState() :
                    MvccUtils.state(cctx, row.mvccCoordinatorVersion(), row.mvccCounter(),
                        row.mvccOperationCounter());

                if (txState == TxState.ABORTED)
                    continue;

                info.mvccVersion(row.mvccCoordinatorVersion(), row.mvccCounter(), row.mvccOperationCounter());
                info.mvccTxState(txState);

                byte newTxState = row.newMvccTxState() != TxState.NA ? row.newMvccTxState() :
                    MvccUtils.state(cctx, row.newMvccCoordinatorVersion(), row.newMvccCounter(),
                        row.newMvccOperationCounter());

                if (newTxState != TxState.ABORTED) {
                    info.newMvccVersion(row.newMvccCoordinatorVersion(),
                        row.newMvccCounter(),
                        row.newMvccOperationCounter());

                    info.newMvccTxState(newTxState);
                }

                long expireTime = row.expireTime();

                long ttl;

                ttl = expireTime == CU.EXPIRE_TIME_ETERNAL ? CU.TTL_ETERNAL : expireTime - U.currentTimeMillis();

                if (ttl < 0)
                    ttl = CU.TTL_MINIMUM;

                info.ttl(ttl);
                info.expireTime(expireTime);

                res.add(info);
            }

            return res;
        }
        finally {
            unlockEntry();
        }
    }

    /** {@inheritDoc} */
    @Override public final CacheObject unswap() throws IgniteCheckedException, GridCacheEntryRemovedException {
        return unswap(true);
    }

    /** {@inheritDoc} */
    @Override public final CacheObject unswap(CacheDataRow row) throws IgniteCheckedException, GridCacheEntryRemovedException {
        row = unswap(row, true);

        return row != null ? row.value() : null;
    }

    /** {@inheritDoc} */
    @Nullable @Override public final CacheObject unswap(boolean needVal)
        throws IgniteCheckedException, GridCacheEntryRemovedException {
        CacheDataRow row = unswap(null, true);

        return row != null ? row.value() : null;
    }

    /**
     * Unswaps an entry.
     *
     * @param row Already extracted cache data.
     * @param checkExpire If {@code true} checks for expiration, as result entry can be obsoleted or marked deleted.
     * @return Value.
     * @throws IgniteCheckedException If failed.
     * @throws GridCacheEntryRemovedException If entry was removed.
     */
    @Nullable protected CacheDataRow unswap(@Nullable CacheDataRow row, boolean checkExpire)
        throws IgniteCheckedException, GridCacheEntryRemovedException {
        boolean obsolete = false;
        cctx.shared().database().checkpointReadLock();

        lockEntry();

        try {
            checkObsolete();

            if (isStartVersion() && ((flags & IS_UNSWAPPED_MASK) == 0)) {
                assert row == null || Objects.equals(row.key(), key) :
                    "Unexpected row key [row.key=" + row.key() + ", cacheEntry.key=" + key + "]";

                CacheDataRow read = row == null ? cctx.offheap().read(this) : row;

                flags |= IS_UNSWAPPED_MASK;

                if (read != null) {
                    CacheObject val = read.value();

                    if (read.tombstone())
                        val = null;

                    update(val, read.expireTime(), 0, read.version(), false);

                    if (!(checkExpire && read.expireTime() > 0) || (read.expireTime() > U.currentTimeMillis()))
                        return read;
                    else {
                        if (onExpired(this.val, null))
                            obsolete = true;
                    }
                }
            }
        }
        finally {
            unlockEntry();

            cctx.shared().database().checkpointReadUnlock();
        }

        if (obsolete) {
            onMarkedObsolete();

            cctx.cache().removeEntry(this);
        }

        return null;
    }

    /**
     * @return Value bytes and flag indicating whether value is byte array.
     */
    protected IgniteBiTuple<byte[], Byte> valueBytes0() {
        assert lock.isHeldByCurrentThread();

        assert val != null;

        try {
            byte[] bytes = val.valueBytes(cctx.cacheObjectContext());

            return new IgniteBiTuple<>(bytes, val.cacheObjectType());
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }

    /**
     * @param tx Transaction.
     * @param key Key.
     * @param reload flag.
     * @param subjId Subject ID.
     * @param taskName Task name.
     * @return Read value.
     * @throws IgniteCheckedException If failed.
     */
    @Nullable protected Object readThrough(@Nullable IgniteInternalTx tx, KeyCacheObject key, boolean reload, UUID subjId,
                                           String taskName) throws IgniteCheckedException {
        return cctx.store().load(tx, key);
    }

    /** {@inheritDoc} */
    @Override public final CacheObject innerGet(
        @Nullable GridCacheVersion ver,
        @Nullable IgniteInternalTx tx,
        boolean readThrough,
        boolean updateMetrics,
        boolean evt,
        UUID subjId,
        Object transformClo,
        String taskName,
        @Nullable IgniteCacheExpiryPolicy expirePlc,
        boolean keepBinary)
        throws IgniteCheckedException, GridCacheEntryRemovedException {
        return (CacheObject)innerGet0(
            ver,
            tx,
            readThrough,
            evt,
            updateMetrics,
            subjId,
            transformClo,
            taskName,
            expirePlc,
            false,
            keepBinary,
            false,
            null);
    }

    /** {@inheritDoc} */
    @Override public EntryGetResult innerGetAndReserveForLoad(boolean updateMetrics,
                                                              boolean evt,
                                                              UUID subjId,
                                                              String taskName,
                                                              @Nullable IgniteCacheExpiryPolicy expiryPlc,
                                                              boolean keepBinary,
                                                              @Nullable ReaderArguments readerArgs) throws IgniteCheckedException, GridCacheEntryRemovedException {
        return (EntryGetResult)innerGet0(
            /*ver*/null,
            /*tx*/null,
            /*readThrough*/false,
            evt,
            updateMetrics,
            subjId,
            /*transformClo*/null,
            taskName,
            expiryPlc,
            true,
            keepBinary,
            /*reserve*/true,
            readerArgs);
    }

    /** {@inheritDoc} */
    @Override public EntryGetResult innerGetVersioned(
        @Nullable GridCacheVersion ver,
        IgniteInternalTx tx,
        boolean updateMetrics,
        boolean evt,
        UUID subjId,
        Object transformClo,
        String taskName,
        @Nullable IgniteCacheExpiryPolicy expiryPlc,
        boolean keepBinary,
        @Nullable ReaderArguments readerArgs)
        throws IgniteCheckedException, GridCacheEntryRemovedException {
        return (EntryGetResult)innerGet0(
            ver,
            tx,
            false,
            evt,
            updateMetrics,
            subjId,
            transformClo,
            taskName,
            expiryPlc,
            true,
            keepBinary,
            false,
            readerArgs);
    }

    /** */
    @SuppressWarnings({"TooBroadScope"})
    private Object innerGet0(
        GridCacheVersion nextVer,
        IgniteInternalTx tx,
        boolean readThrough,
        boolean evt,
        boolean updateMetrics,
        UUID subjId,
        Object transformClo,
        String taskName,
        @Nullable IgniteCacheExpiryPolicy expiryPlc,
        boolean retVer,
        boolean keepBinary,
        boolean reserveForLoad,
        @Nullable ReaderArguments readerArgs
    ) throws IgniteCheckedException, GridCacheEntryRemovedException {
        assert !(retVer && readThrough);
        assert !(reserveForLoad && readThrough);

        // Disable read-through if there is no store.
        if (readThrough && !cctx.readThrough())
            readThrough = false;

        GridCacheVersion startVer;
        GridCacheVersion resVer = null;

        boolean obsolete = false;
        GridCacheVersion ver0 = null;

        Object res = null;

        lockEntry();

        try {
            checkObsolete();

            CacheObject val;

            AffinityTopologyVersion topVer = tx != null ? tx.topologyVersion() : cctx.affinity().affinityTopologyVersion();
            boolean valid = valid(topVer);

            if (valid) {
                val = this.val;

                if (val == null) {
                    if (isStartVersion()) {
                        unswap(null, false);

                        val = this.val;

                        if (val != null && tx == null)
                            updatePlatformCache(val, topVer);
                    }
                }

                if (val != null) {
                    if (checkExpired()) {
                        if (onExpired(cctx.unwrapTemporary(val), null)) {
                            val = null;
                            evt = false;
                            obsolete = true;
                        }
                    }
                }
            }
            else
                val = null;

            CacheObject ret = val;

            if (ret == null) {
                if (updateMetrics && cctx.statisticsEnabled())
                    cctx.cache().metrics0().onRead(false);
            }
            else {
                if (updateMetrics && cctx.statisticsEnabled())
                    cctx.cache().metrics0().onRead(true);
            }

            if (evt && cctx.events().isRecordable(EVT_CACHE_OBJECT_READ)) {
                transformClo = EntryProcessorResourceInjectorProxy.unwrap(transformClo);

                GridCacheMvcc mvcc = mvccExtras();

                cctx.events().addEvent(
                    partition(),
                    key,
                    tx,
                    mvcc != null ? mvcc.anyOwner() : null,
                    EVT_CACHE_OBJECT_READ,
                    ret,
                    ret != null,
                    ret,
                    ret != null,
                    subjId,
                    transformClo != null ? transformClo.getClass().getName() : null,
                    taskName,
                    keepBinary);

                // No more notifications.
                evt = false;
            }

            if (ret != null && expiryPlc != null)
                updateTtl(expiryPlc);

            if (retVer && resVer == null) {
                resVer = (isNear() && cctx.transactional()) ? ((GridNearCacheEntry)this).dhtVersion() : this.ver;

                if (resVer == null)
                    ret = null;
            }

            // Cache version for optimistic check.
            startVer = ver;

            addReaderIfNeed(readerArgs);

            if (ret != null) {
                assert !obsolete;

                // If return value is consistent, then done.
                res = retVer ? entryGetResult(ret, resVer, false) : ret;
            }
            else if (reserveForLoad && !obsolete) {
                assert !readThrough;
                assert retVer;

                boolean reserve = !evictionDisabled();

                if (reserve)
                    flags |= IS_EVICT_DISABLED;

                res = entryGetResult(null, resVer, reserve);
            }
        }
        finally {
            unlockEntry();
        }

        if (obsolete) {
            onMarkedObsolete();

            throw new GridCacheEntryRemovedException();
        }

        if (res != null)
            return res;

        CacheObject ret = null;

        if (readThrough) {
            IgniteInternalTx tx0 = null;

            if (tx != null && tx.local()) {
                if (cctx.isReplicated() || cctx.isColocated() || tx.near())
                    tx0 = tx;
                else if (tx.dht()) {
                    GridCacheVersion ver = tx.nearXidVersion();

                    tx0 = cctx.dht().near().context().tm().tx(ver);
                }
            }

            Object storeVal = readThrough(tx0, key, false, subjId, taskName);

            ret = cctx.toCacheObject(storeVal);
        }

        if (ret == null && !evt)
            return null;

        lockEntry();

        try {
            long ttl = ttlExtras();

            // If version matched, set value.
            if (startVer.equals(ver)) {
                if (ret != null) {
                    // Detach value before index update.
                    ret = ret.prepareForCache(cctx.cacheObjectContext(), true);

                    nextVer = nextVer != null ? nextVer : nextVersion();

                    long expTime = CU.toExpireTime(ttl);

                    // Update indexes before actual write to entry.
                    storeValue(ret, expTime, nextVer);

                    update(ret, expTime, ttl, nextVer, true);

                    assert readerArgs == null;
                }

                if (evt && cctx.events().isRecordable(EVT_CACHE_OBJECT_READ)) {
                    transformClo = EntryProcessorResourceInjectorProxy.unwrap(transformClo);

                    GridCacheMvcc mvcc = mvccExtras();

                    cctx.events().addEvent(
                        partition(),
                        key,
                        tx,
                        mvcc != null ? mvcc.anyOwner() : null,
                        EVT_CACHE_OBJECT_READ,
                        ret,
                        ret != null,
                        null,
                        false,
                        subjId,
                        transformClo != null ? transformClo.getClass().getName() : null,
                        taskName,
                        keepBinary);
                }
            }
        }
        finally {
            unlockEntry();
        }

        assert ret == null || !retVer;

        return ret;
    }

    /**
     * Creates EntryGetResult or EntryGetWithTtlResult if expire time information exists.
     *
     * @param val Value.
     * @param ver Version.
     * @param reserve Reserve flag.
     * @return EntryGetResult.
     */
    private EntryGetResult entryGetResult(CacheObject val, GridCacheVersion ver, boolean reserve) {
        return extras == null || extras.expireTime() == 0
            ? new EntryGetResult(val, ver, reserve)
            : new EntryGetWithTtlResult(val, ver, reserve, rawExpireTime(), rawTtl());
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"TooBroadScope"})
    @Nullable @Override public final CacheObject innerReload()
        throws IgniteCheckedException, GridCacheEntryRemovedException {
        CU.checkStore(cctx);

        GridCacheVersion startVer;

        boolean wasNew;

        lockEntry();

        try {
            checkObsolete();

            // Cache version for optimistic check.
            startVer = ver;

            wasNew = isNew();
        }
        finally {
            unlockEntry();
        }

        String taskName = cctx.kernalContext().job().currentTaskName();

        // Check before load.
        CacheObject ret = cctx.toCacheObject(readThrough(null, key, true, cctx.localNodeId(), taskName));

        boolean touch = false;

        try {
            ensureFreeSpace();

            lockEntry();

            try {
                long ttl = ttlExtras();

                // Generate new version.
                GridCacheVersion nextVer = cctx.versions().nextForLoad(ver);

                // If entry was loaded during read step.
                if (wasNew && !isNew())
                    // Map size was updated on entry creation.
                    return ret;

                // If version matched, set value.
                if (startVer.equals(ver)) {
                    long expTime = CU.toExpireTime(ttl);

                    // Update indexes.
                    if (ret != null) {
                        // Detach value before index update.
                        ret = ret.prepareForCache(cctx.cacheObjectContext(), true);

                        storeValue(ret, expTime, nextVer);
                    }
                    else {
                        if (cctx.mvccEnabled())
                            cctx.offheap().mvccRemoveAll(this);
                        else
                            removeValue(nextVer);
                    }

                    update(ret, expTime, ttl, nextVer, true);

                    touch = true;

                    // If value was set - return, otherwise try again.
                    return ret;
                }
            }
            finally {
                unlockEntry();
            }

            touch = true;

            return ret;
        }
        finally {
            if (touch)
                touch();
        }
    }

    /**
     * @param nodeId Node ID.
     */
    protected void recordNodeId(UUID nodeId, AffinityTopologyVersion topVer) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public final GridCacheUpdateTxResult mvccSet(
        IgniteInternalTx tx,
        UUID affNodeId,
        CacheObject val,
        EntryProcessor entryProc,
        Object[] invokeArgs,
        long ttl0,
        AffinityTopologyVersion topVer,
        MvccSnapshot mvccVer,
        GridCacheOperation op,
        boolean needHistory,
        boolean noCreate,
        boolean needOldVal,
        CacheEntryPredicate filter,
        boolean retVal,
        boolean keepBinary) throws IgniteCheckedException, GridCacheEntryRemovedException {
        assert tx != null;

        final boolean valid = valid(tx.topologyVersion());

        final boolean invoke = entryProc != null;

        final GridCacheVersion newVer;

        WALPointer logPtr = null;

        ensureFreeSpace();

        lockEntry();

        MvccUpdateResult res;

        try {
            checkObsolete();

            assert tx.writeVersion() != null : "Failed to get write version for tx: " + tx;

            newVer = nextVersion(null, tx);

            // Determine new ttl and expire time.
            long expireTime, ttl = ttl0;

            if (ttl == -1L) {
                ttl = ttlExtras();
                expireTime = expireTimeExtras();
            }
            else
                expireTime = CU.toExpireTime(ttl);

            assert ttl >= 0 : ttl;
            assert expireTime >= 0 : expireTime;
            assert val != null || invoke;

            // Detach value before index update.
            if (val != null)
                val = val.prepareForCache(cctx.cacheObjectContext(), true);

            res = cctx.offheap().mvccUpdate(this, val, newVer, expireTime, mvccVer, tx.local(), needHistory,
                noCreate, needOldVal, filter, retVal, keepBinary, entryProc, invokeArgs);

            assert res != null;

            // VERSION_FOUND is possible only on primary node when inserting the same key, or on backup when
            // updating the key which just has been rebalanced.
            assert res.resultType() != ResultType.VERSION_FOUND || op == CREATE && tx.local() || !tx.local();

            // PREV_NOT_NULL on CREATE is possible only on primary.
            assert res.resultType() != ResultType.PREV_NOT_NULL || op != CREATE || tx.local();

            if (res.resultType() == ResultType.VERSION_MISMATCH)
                throw serializationError();
            else if (res.resultType() == ResultType.FILTERED) {
                GridCacheUpdateTxResult updRes = new GridCacheUpdateTxResult(invoke);

                assert !invoke || res.invokeResult() != null;

                if (invoke) // No-op invoke happened.
                    updRes.invokeResult(res.invokeResult());

                updRes.filtered(true);

                if (retVal)
                    updRes.prevValue(res.oldValue());

                return updRes;
            }
            else if (noCreate && !invoke && res.resultType() == ResultType.PREV_NULL)
                return new GridCacheUpdateTxResult(false);
            else if (res.resultType() == ResultType.LOCKED) {
                unlockEntry();

                MvccVersion lockVer = res.resultVersion();

                GridFutureAdapter<GridCacheUpdateTxResult> resFut = new GridFutureAdapter<>();

                IgniteInternalFuture<?> lockFut = cctx.kernalContext().coordinators().waitForLock(cctx, mvccVer, lockVer);

                lockFut.listen(new MvccUpdateLockListener(tx, this, affNodeId, topVer, val, ttl0, mvccVer,
                    op, needHistory, noCreate, resFut, needOldVal, filter, retVal, keepBinary, entryProc, invokeArgs));

                return new GridCacheUpdateTxResult(false, resFut);
            }
            else if (op == CREATE && tx.local() && (res.resultType() == ResultType.PREV_NOT_NULL ||
                res.resultType() == ResultType.VERSION_FOUND))
                throw new IgniteTxDuplicateKeyCheckedException(key);

            if (res.resultType() == ResultType.PREV_NULL) {
                TxCounters counters = tx.txCounters(true);

                if (compareIgnoreOpCounter(res.resultVersion(), mvccVer) == 0) {
                    if (res.isKeyAbsentBefore())
                        counters.incrementUpdateCounter(cctx.cacheId(), partition());
                }
                else
                    counters.incrementUpdateCounter(cctx.cacheId(), partition());

                counters.accumulateSizeDelta(cctx.cacheId(), partition(), 1);
            }
            else if (res.resultType() == ResultType.PREV_NOT_NULL && compareIgnoreOpCounter(res.resultVersion(), mvccVer) != 0) {
                TxCounters counters = tx.txCounters(true);

                counters.incrementUpdateCounter(cctx.cacheId(), partition());
            }
            else if (res.resultType() == ResultType.REMOVED_NOT_NULL) {
                TxCounters counters = tx.txCounters(true);

                if (compareIgnoreOpCounter(res.resultVersion(), mvccVer) == 0) {
                    if (res.isKeyAbsentBefore()) // Do not count own update removal.
                        counters.decrementUpdateCounter(cctx.cacheId(), partition());
                }
                else
                    counters.incrementUpdateCounter(cctx.cacheId(), partition());

                counters.accumulateSizeDelta(cctx.cacheId(), partition(), -1);
            }

            if (cctx.group().persistenceEnabled() && cctx.group().walEnabled()) {
                logPtr = cctx.shared().wal().log(new MvccDataRecord(new MvccDataEntry(
                    cctx.cacheId(),
                    key,
                    val,
                    res.resultType() == ResultType.PREV_NULL ? CREATE :
                        (res.resultType() == ResultType.REMOVED_NOT_NULL) ? DELETE : UPDATE,
                    tx.nearXidVersion(),
                    newVer,
                    expireTime,
                    key.partition(),
                    0L,
                    mvccVer)
                ));
            }

            update(val, expireTime, ttl, newVer, true);

            recordNodeId(affNodeId, topVer);
        }
        finally {
            if (lockedByCurrentThread()) {
                unlockEntry();

                cctx.evicts().touch(this);
            }
        }

        onUpdateFinished(0L);

        GridCacheUpdateTxResult updRes = valid ? new GridCacheUpdateTxResult(true, 0L, logPtr) :
            new GridCacheUpdateTxResult(false, logPtr);

        if (retVal && (res.resultType() == ResultType.PREV_NOT_NULL || res.resultType() == ResultType.VERSION_FOUND))
            updRes.prevValue(res.oldValue());

        if (needOldVal && compareIgnoreOpCounter(res.resultVersion(), mvccVer) != 0 && (
            res.resultType() == ResultType.PREV_NOT_NULL || res.resultType() == ResultType.REMOVED_NOT_NULL))
            updRes.oldValue(res.oldValue());

        updRes.newValue(res.newValue());

        if (invoke && res.resultType() != ResultType.VERSION_FOUND) {
            assert res.invokeResult() != null;

            updRes.invokeResult(res.invokeResult());
        }

        updRes.mvccHistory(res.history());

        return updRes;
    }

    /** {@inheritDoc} */
    @Override public final GridCacheUpdateTxResult mvccRemove(
        IgniteInternalTx tx,
        UUID affNodeId,
        AffinityTopologyVersion topVer,
        MvccSnapshot mvccVer,
        boolean needHistory,
        boolean needOldVal,
        @Nullable CacheEntryPredicate filter,
        boolean retVal) throws IgniteCheckedException, GridCacheEntryRemovedException {
        assert tx != null;
        assert mvccVer != null;

        final boolean valid = valid(tx.topologyVersion());

        final GridCacheVersion newVer;

        WALPointer logPtr = null;

        lockEntry();

        MvccUpdateResult res;

        try {
            checkObsolete();

            newVer = tx.writeVersion();

            assert newVer != null : "Failed to get write version for tx: " + tx;

            res = cctx.offheap().mvccRemove(this, mvccVer, tx.local(), needHistory, needOldVal, filter, retVal);

            assert res != null;

            if (res.resultType() == ResultType.VERSION_MISMATCH)
                throw serializationError();
            else if (res.resultType() == ResultType.PREV_NULL)
                return new GridCacheUpdateTxResult(false);
            else if (res.resultType() == ResultType.FILTERED) {
                GridCacheUpdateTxResult updRes = new GridCacheUpdateTxResult(false);

                updRes.filtered(true);

                return updRes;
            }
            else if (res.resultType() == ResultType.LOCKED) {
                unlockEntry();

                MvccVersion lockVer = res.resultVersion();

                GridFutureAdapter<GridCacheUpdateTxResult> resFut = new GridFutureAdapter<>();

                IgniteInternalFuture<?> lockFut = cctx.kernalContext().coordinators().waitForLock(cctx, mvccVer, lockVer);

                lockFut.listen(new MvccRemoveLockListener(tx, this, affNodeId, topVer, mvccVer, needHistory,
                    resFut, needOldVal, retVal, filter));

                return new GridCacheUpdateTxResult(false, resFut);
            }

            if (res.resultType() == ResultType.PREV_NOT_NULL) {
                TxCounters counters = tx.txCounters(true);

                if (compareIgnoreOpCounter(res.resultVersion(), mvccVer) == 0) {
                    if (res.isKeyAbsentBefore()) // Do not count own update removal.
                        counters.decrementUpdateCounter(cctx.cacheId(), partition());
                }
                else
                    counters.incrementUpdateCounter(cctx.cacheId(), partition());

                counters.accumulateSizeDelta(cctx.cacheId(), partition(), -1);
            }

            if (cctx.group().persistenceEnabled() && cctx.group().walEnabled())
                logPtr = logMvccUpdate(tx, null, 0, 0L, mvccVer);

            update(null, 0, 0, newVer, true);

            recordNodeId(affNodeId, topVer);
        }
        finally {
            if (lockedByCurrentThread()) {
                unlockEntry();

                cctx.evicts().touch(this);
            }
        }

        onUpdateFinished(0L);

        GridCacheUpdateTxResult updRes = valid ? new GridCacheUpdateTxResult(true, 0L, logPtr) :
            new GridCacheUpdateTxResult(false, logPtr);

        if (retVal && (res.resultType() == ResultType.PREV_NOT_NULL || res.resultType() == ResultType.VERSION_FOUND))
            updRes.prevValue(res.oldValue());

        if (needOldVal && compareIgnoreOpCounter(res.resultVersion(), mvccVer) != 0 &&
            (res.resultType() == ResultType.PREV_NOT_NULL || res.resultType() == ResultType.REMOVED_NOT_NULL))
            updRes.oldValue(res.oldValue());

        updRes.mvccHistory(res.history());

        return updRes;
    }

    /** {@inheritDoc} */
    @Override public GridCacheUpdateTxResult mvccLock(GridDhtTxLocalAdapter tx, MvccSnapshot mvccVer)
        throws GridCacheEntryRemovedException, IgniteCheckedException {
        assert tx != null;
        assert mvccVer != null;

        final boolean valid = valid(tx.topologyVersion());

        final GridCacheVersion newVer;

        WALPointer logPtr = null;

        lockEntry();

        try {
            checkObsolete();

            newVer = tx.writeVersion();

            assert newVer != null : "Failed to get write version for tx: " + tx;

            assert tx.local();

            MvccUpdateResult res = cctx.offheap().mvccLock(this, mvccVer);

            assert res != null;

            if (res.resultType() == ResultType.VERSION_MISMATCH)
                throw serializationError();
            else if (res.resultType() == ResultType.LOCKED) {
                unlockEntry();

                MvccVersion lockVer = res.resultVersion();

                GridFutureAdapter<GridCacheUpdateTxResult> resFut = new GridFutureAdapter<>();

                IgniteInternalFuture<?> lockFut = cctx.kernalContext().coordinators().waitForLock(cctx, mvccVer, lockVer);

                lockFut.listen(new MvccAcquireLockListener(tx, this, mvccVer, resFut));

                return new GridCacheUpdateTxResult(false, resFut);
            }
        }
        finally {
            if (lockedByCurrentThread()) {
                unlockEntry();

                cctx.evicts().touch(this);
            }
        }

        onUpdateFinished(0L);

        return new GridCacheUpdateTxResult(valid, logPtr);
    }

    /** {@inheritDoc} */
    @Override public final GridCacheUpdateTxResult innerSet(
        @Nullable IgniteInternalTx tx,
        UUID evtNodeId,
        UUID affNodeId,
        CacheObject val,
        boolean writeThrough,
        boolean retval,
        long ttl,
        boolean evt,
        boolean metrics,
        boolean keepBinary,
        boolean oldValPresent,
        @Nullable CacheObject oldVal,
        AffinityTopologyVersion topVer,
        CacheEntryPredicate[] filter,
        GridDrType drType,
        long drExpireTime,
        @Nullable GridCacheVersion explicitVer,
        @Nullable UUID subjId,
        String taskName,
        @Nullable GridCacheVersion dhtVer,
        @Nullable Long updateCntr
    ) throws IgniteCheckedException, GridCacheEntryRemovedException {
        CacheObject old;

        final boolean valid = valid(tx != null ? tx.topologyVersion() : topVer);

        // Lock should be held by now.
        if (!cctx.isAll(this, filter))
            return new GridCacheUpdateTxResult(false);

        final GridCacheVersion newVer;

        boolean intercept = cctx.config().getInterceptor() != null;

        Object key0 = null;
        Object val0 = null;
        WALPointer logPtr = null;

        long updateCntr0;

        ensureFreeSpace();

        lockListenerReadLock();
        lockEntry();

        try {
            checkObsolete();

            if (isNear()) {
                assert dhtVer != null;

                // It is possible that 'get' could load more recent value.
                if (!((GridNearCacheEntry)this).recordDhtVersion(dhtVer))
                    return new GridCacheUpdateTxResult(false, logPtr);
            }

            assert tx == null || (!tx.local() && tx.onePhaseCommit()) || tx.ownsLock(this) :
                "Transaction does not own lock for update [entry=" + this + ", tx=" + tx + ']';

            // Load and remove from swap if it is new.
            boolean startVer = isStartVersion();

            boolean internal = isInternal() || !context().userCache();

            Map<UUID, CacheContinuousQueryListener> lsnrCol =
                notifyContinuousQueries() ?
                    cctx.continuousQueries().updateListeners(internal, false) : null;

            if (startVer && (retval || intercept || lsnrCol != null))
                unswap(retval);

            old = oldValPresent ? oldVal : this.val;

            if (intercept)
                intercept = !skipInterceptor(explicitVer);

            if (intercept) {
                val0 = cctx.unwrapBinaryIfNeeded(val, keepBinary, false, null);

                CacheLazyEntry e = new CacheLazyEntry(cctx, key, old, keepBinary);

                key0 = e.key();

                Object interceptorVal = cctx.config().getInterceptor().onBeforePut(e, val0);

                if (interceptorVal == null)
                    return new GridCacheUpdateTxResult(false, logPtr);
                else if (interceptorVal != val0)
                    val0 = cctx.unwrapTemporary(interceptorVal);

                val = cctx.toCacheObject(val0);
            }

            // Determine new ttl and expire time.
            long expireTime;

            if (drExpireTime >= 0) {
                assert ttl >= 0 : ttl;

                expireTime = drExpireTime;
            }
            else {
                if (ttl == -1L) {
                    ttl = ttlExtras();
                    expireTime = deletedUnlocked() ? 0 : expireTimeExtras();
                }
                else
                    expireTime = CU.toExpireTime(ttl);
            }

            assert ttl >= 0 : ttl;
            assert expireTime >= 0 : expireTime;
            assert val != null;

            // Detach value before index update.
            val = val.prepareForCache(cctx.cacheObjectContext(), true);

            newVer = nextVersion(explicitVer, tx);

            updateCntr0 = nextPartitionCounter(tx, updateCntr);

            newVer.updateCounter(updateCntr0);

            storeValue(val, expireTime, newVer);

            if (tx != null && cctx.group().persistenceEnabled())
                logPtr = logTxUpdate(tx, val, expireTime, newVer, cctx.group().walEnabled());

            update(val, expireTime, ttl, newVer, true);

            drReplicate(drType, val, newVer, topVer);

            recordNodeId(affNodeId, topVer);

            if (metrics && cctx.statisticsEnabled() && tx != null) {
                cctx.cache().metrics0().onWrite();

                IgniteTxEntry txEntry = tx.entry(txKey());

                if (txEntry != null) {
                    T2<GridCacheOperation, CacheObject> entryProcRes = txEntry.entryProcessorCalculatedValue();

                    if (entryProcRes != null && UPDATE.equals(entryProcRes.get1()))
                        cctx.cache().metrics0().onInvokeUpdate(old != null);
                }
            }

            if (evt && newVer != null && cctx.events().isRecordable(EVT_CACHE_OBJECT_PUT)) {
                CacheObject evtOld = cctx.unwrapTemporary(old);

                cctx.events().addEvent(partition(),
                    key,
                    evtNodeId,
                    tx,
                    null,
                    newVer,
                    EVT_CACHE_OBJECT_PUT,
                    val,
                    val != null,
                    evtOld,
                    evtOld != null || hasValueUnlocked(),
                    subjId, null, taskName,
                    keepBinary);
            }

            if (lsnrCol != null) {
                cctx.continuousQueries().onEntryUpdated(
                    lsnrCol,
                    key,
                    val,
                    old,
                    internal,
                    partition(),
                    tx.local(),
                    false,
                    updateCntr0,
                    null,
                    topVer);
            }
        }
        finally {
            unlockEntry();
            unlockListenerReadLock();
        }

        onUpdateFinished(updateCntr0);

        if (log.isDebugEnabled())
            log.debug("Updated cache entry [val=" + val + ", old=" + old + ", entry=" + this + ']');

        // Persist outside of synchronization. The correctness of the
        // value will be handled by current transaction.
        if (writeThrough)
            cctx.store().put(tx, key, val, newVer);

        if (intercept)
            cctx.config().getInterceptor().onAfterPut(new CacheLazyEntry(cctx, key, key0, val, val0, keepBinary, updateCntr0));

        updatePlatformCache(val, topVer);

        return valid ? new GridCacheUpdateTxResult(true, updateCntr0, logPtr) :
            new GridCacheUpdateTxResult(false, logPtr);
    }

    /**
     * @param cpy Copy flag.
     * @return Key value.
     */
    protected Object keyValue(boolean cpy) {
        return key.value(cctx.cacheObjectContext(), cpy);
    }

    /** {@inheritDoc} */
    @Override public final GridCacheUpdateTxResult innerRemove(
        @Nullable IgniteInternalTx tx,
        UUID evtNodeId,
        UUID affNodeId,
        boolean retval,
        boolean evt,
        boolean metrics,
        boolean keepBinary,
        boolean oldValPresent,
        @Nullable CacheObject oldVal,
        AffinityTopologyVersion topVer,
        CacheEntryPredicate[] filter,
        GridDrType drType,
        @Nullable GridCacheVersion explicitVer,
        @Nullable UUID subjId,
        String taskName,
        @Nullable GridCacheVersion dhtVer,
        @Nullable Long updateCntr
    ) throws IgniteCheckedException, GridCacheEntryRemovedException {
        assert cctx.transactional();

        CacheObject old;

        GridCacheVersion newVer;

        final boolean valid = valid(tx != null ? tx.topologyVersion() : topVer);

        // Lock should be held by now.
        if (!cctx.isAll(this, filter))
            return new GridCacheUpdateTxResult(false);

        GridCacheVersion obsoleteVer = null;

        boolean intercept = cctx.config().getInterceptor() != null;

        IgniteBiTuple<Boolean, Object> interceptRes = null;

        CacheLazyEntry entry0 = null;

        long updateCntr0;

        WALPointer logPtr = null;

        boolean marked = false;

        lockListenerReadLock();
        lockEntry();

        try {
            checkObsolete();

            if (isNear()) {
                assert dhtVer != null;

                // It is possible that 'get' could load more recent value.
                if (!((GridNearCacheEntry)this).recordDhtVersion(dhtVer)) {
                    logOnMoreRecent();

                    return new GridCacheUpdateTxResult(false, logPtr);
                }
            }

            assert tx == null || (!tx.local() && tx.onePhaseCommit()) || tx.ownsLock(this) :
                "Transaction does not own lock for remove[entry=" + this + ", tx=" + tx + ']';

            boolean startVer = isStartVersion();

            boolean internal = isInternal() || !context().userCache();

            Map<UUID, CacheContinuousQueryListener> lsnrCol =
                notifyContinuousQueries() ?
                    cctx.continuousQueries().updateListeners(internal, false) : null;

            if (startVer && (retval || intercept || lsnrCol != null))
                unswap();

            old = oldValPresent ? oldVal : val;

            if (intercept)
                intercept = !skipInterceptor(explicitVer);

            if (intercept) {
                entry0 = new CacheLazyEntry(cctx, key, old, keepBinary);

                interceptRes = cctx.config().getInterceptor().onBeforeRemove(entry0);

                if (cctx.cancelRemove(interceptRes))
                    return new GridCacheUpdateTxResult(false, logPtr);
            }

            newVer = nextVersion(explicitVer, tx);

            updateCntr0 = nextPartitionCounter(tx, updateCntr);

            newVer.updateCounter(updateCntr0);

            removeValue(newVer);

            update(null, 0, 0, newVer, true);

            if (tx != null && cctx.group().persistenceEnabled())
                logPtr = logTxUpdate(tx, null, 0, newVer, cctx.group().walEnabled());

            drReplicate(drType, null, newVer, topVer);

            if (metrics && cctx.statisticsEnabled() && tx != null) {
                cctx.cache().metrics0().onRemove();

                IgniteTxEntry txEntry = tx.entry(txKey());

                if (txEntry != null) {
                    T2<GridCacheOperation, CacheObject> entryProcRes = txEntry.entryProcessorCalculatedValue();

                    if (entryProcRes != null && DELETE.equals(entryProcRes.get1()))
                        cctx.cache().metrics0().onInvokeRemove(old != null);
                }
            }

            if (tx == null)
                obsoleteVer = newVer;
            else {
                // Only delete entry if the lock is not explicit.
                if (lockedBy(tx.xidVersion()))
                    obsoleteVer = tx.xidVersion();
                else if (log.isDebugEnabled())
                    log.debug("Obsolete version was not set because lock was explicit: " + this);
            }

            if (evt && cctx.events().isRecordable(EVT_CACHE_OBJECT_REMOVED)) {
                CacheObject evtOld = cctx.unwrapTemporary(old);

                cctx.events().addEvent(partition(),
                    key,
                    evtNodeId,
                    tx,
                    null,
                    newVer,
                    EVT_CACHE_OBJECT_REMOVED,
                    null,
                    false,
                    evtOld,
                    evtOld != null || hasValueUnlocked(),
                    subjId,
                    null,
                    taskName,
                    keepBinary);
            }

            if (lsnrCol != null) {
                cctx.continuousQueries().onEntryUpdated(
                    lsnrCol,
                    key,
                    null,
                    old,
                    internal,
                    partition(),
                    tx.local(),
                    false,
                    updateCntr0,
                    null,
                    topVer);
            }

            if (intercept)
                entry0.updateCounter(updateCntr0);

            // If entry is still removed.
            assert newVer == ver;

            if (obsoleteVer == null || !(marked = markObsolete0(obsoleteVer, true, null))) {
                if (log.isDebugEnabled())
                    log.debug("Entry could not be marked obsolete (it is still used): " + this);
            }
            else {
                recordNodeId(affNodeId, topVer);

                if (log.isDebugEnabled())
                    log.debug("Entry was marked obsolete: " + this);
            }
        }
        finally {
            unlockEntry();
            unlockListenerReadLock();
        }

        if (marked)
            onMarkedObsolete();

        onUpdateFinished(updateCntr0);

        if (intercept)
            cctx.config().getInterceptor().onAfterRemove(entry0);

        if (valid)
            return new GridCacheUpdateTxResult(true, updateCntr0, logPtr);
        else
            return new GridCacheUpdateTxResult(false, logPtr);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public GridTuple3<Boolean, Object, EntryProcessorResult<Object>> innerUpdateLocal(
        GridCacheVersion ver,
        GridCacheOperation op,
        @Nullable Object writeObj,
        @Nullable Object[] invokeArgs,
        boolean writeThrough,
        boolean readThrough,
        boolean retval,
        boolean keepBinary,
        @Nullable ExpiryPolicy expiryPlc,
        boolean evt,
        boolean metrics,
        @Nullable CacheEntryPredicate[] filter,
        boolean intercept,
        @Nullable UUID subjId,
        String taskName,
        boolean transformOp
    ) throws IgniteCheckedException, GridCacheEntryRemovedException {
        assert cctx.isLocal() && cctx.atomic();

        CacheObject old;

        boolean res = true;

        IgniteBiTuple<Boolean, ?> interceptorRes = null;

        EntryProcessorResult<Object> invokeRes = null;

        lockListenerReadLock();
        lockEntry();

        try {
            checkObsolete();

            boolean internal = isInternal() || !context().userCache();

            Map<UUID, CacheContinuousQueryListener> lsnrCol =
                cctx.continuousQueries().updateListeners(internal, false);

            boolean needVal = retval ||
                intercept ||
                op == GridCacheOperation.TRANSFORM ||
                !F.isEmpty(filter) ||
                lsnrCol != null;

            // Load and remove from swap if it is new.
            if (isNew())
                unswap(null, false);

            old = val;

            if (expireTimeExtras() > 0 && expireTimeExtras() < U.currentTimeMillis()) {
                if (onExpired(val, null)) {
                    update(null, CU.TTL_ETERNAL, CU.EXPIRE_TIME_ETERNAL, ver, true);

                    old = null;
                }
            }

            boolean readFromStore = false;

            Object old0 = null;

            if (readThrough && needVal && old == null &&
                (cctx.readThrough() && (op == GridCacheOperation.TRANSFORM || cctx.loadPreviousValue()))) {
                old0 = readThrough(null, key, false, subjId, taskName);

                old = cctx.toCacheObject(old0);

                long ttl = CU.TTL_ETERNAL;
                long expireTime = CU.EXPIRE_TIME_ETERNAL;

                if (expiryPlc != null && old != null) {
                    ttl = CU.toTtl(expiryPlc.getExpiryForCreation());

                    if (ttl == CU.TTL_ZERO) {
                        ttl = CU.TTL_MINIMUM;
                        expireTime = CU.expireTimeInPast();
                    }
                    else if (ttl == CU.TTL_NOT_CHANGED)
                        ttl = CU.TTL_ETERNAL;
                    else
                        expireTime = CU.toExpireTime(ttl);
                }

                if (old != null) {
                    // Detach value before index update.
                    old = old.prepareForCache(cctx.cacheObjectContext(), true);

                    storeValue(old, expireTime, ver);
                }
                else
                    removeValue(ver);

                update(old, expireTime, ttl, ver, true);
            }

            // Apply metrics.
            if (metrics && cctx.statisticsEnabled() && needVal)
                cctx.cache().metrics0().onRead(old != null);

            // Check filter inside of synchronization.
            if (!F.isEmpty(filter)) {
                boolean pass = cctx.isAllLocked(this, filter);

                if (!pass) {
                    if (expiryPlc != null && !readFromStore && !cctx.putIfAbsentFilter(filter) && hasValueUnlocked())
                        updateTtl(expiryPlc);

                    Object val = retval ?
                        cctx.cacheObjectContext().unwrapBinaryIfNeeded(CU.value(old, cctx, false), keepBinary, false, null)
                        : null;

                    return new T3<>(false, val, null);
                }
            }

            String transformCloClsName = null;

            CacheObject updated;

            Object key0 = null;
            Object updated0 = null;

            // Calculate new value.
            if (op == GridCacheOperation.TRANSFORM) {
                transformCloClsName = EntryProcessorResourceInjectorProxy.unwrap(writeObj).getClass().getName();

                EntryProcessor<Object, Object, ?> entryProcessor = (EntryProcessor<Object, Object, ?>)writeObj;

                assert entryProcessor != null;

                CacheInvokeEntry<Object, Object> entry = new CacheInvokeEntry<>(key, old, version(), keepBinary, this);

                IgniteThread.onEntryProcessorEntered(false);

                try {
                    Object computed = entryProcessor.process(entry, invokeArgs);

                    transformOp = true;

                    if (entry.modified()) {
                        updated0 = cctx.unwrapTemporary(entry.getValue());

                        updated = cctx.toCacheObject(updated0);

                        cctx.validateKeyAndValue(key, updated);
                    }
                    else
                        updated = old;

                    key0 = entry.key();

                    invokeRes = computed != null ? CacheInvokeResult.fromResult(cctx.unwrapTemporary(computed)) : null;
                }
                catch (Exception e) {
                    updated = old;

                    invokeRes = CacheInvokeResult.fromError(e);
                }
                finally {
                    IgniteThread.onEntryProcessorLeft();
                }

                if (!entry.modified()) {
                    if (expiryPlc != null && !readFromStore && hasValueUnlocked())
                        updateTtl(expiryPlc);

                    updateMetrics(READ, metrics, transformOp, old != null);

                    return new GridTuple3<>(false, null, invokeRes);
                }
            }
            else
                updated = (CacheObject)writeObj;

            op = updated == null ? DELETE : UPDATE;

            if (intercept) {
                CacheLazyEntry e;

                if (op == UPDATE) {
                    updated0 = value(updated0, updated, keepBinary, false);

                    e = new CacheLazyEntry(cctx, key, key0, old, old0, keepBinary);

                    Object interceptorVal = cctx.config().getInterceptor().onBeforePut(e, updated0);

                    if (interceptorVal == null)
                        return new GridTuple3<>(false, cctx.unwrapTemporary(value(old0, old, keepBinary, false)), invokeRes);
                    else {
                        updated0 = cctx.unwrapTemporary(interceptorVal);

                        updated = cctx.toCacheObject(updated0);
                    }
                }
                else {
                    e = new CacheLazyEntry(cctx, key, key0, old, old0, keepBinary);

                    interceptorRes = cctx.config().getInterceptor().onBeforeRemove(e);

                    if (cctx.cancelRemove(interceptorRes))
                        return new GridTuple3<>(false, cctx.unwrapTemporary(interceptorRes.get2()), invokeRes);
                }

                key0 = e.key();
                old0 = e.value();
            }

            boolean hadVal = hasValueUnlocked();

            long ttl = CU.TTL_ETERNAL;
            long expireTime = CU.EXPIRE_TIME_ETERNAL;

            if (op == UPDATE) {
                if (expiryPlc != null) {
                    ttl = CU.toTtl(hadVal ? expiryPlc.getExpiryForUpdate() : expiryPlc.getExpiryForCreation());

                    if (ttl == CU.TTL_NOT_CHANGED) {
                        ttl = ttlExtras();
                        expireTime = expireTimeExtras();
                    }
                    else if (ttl != CU.TTL_ZERO)
                        expireTime = CU.toExpireTime(ttl);
                }
                else {
                    ttl = ttlExtras();
                    expireTime = expireTimeExtras();
                }
            }

            if (ttl == CU.TTL_ZERO) {
                op = DELETE;

                //If time expired no transformation needed.
                transformOp = false;
            }

            // Try write-through.
            if (op == UPDATE) {
                // Detach value before index update.
                if (updated != null)
                    updated = updated.prepareForCache(cctx.cacheObjectContext(), true);

                if (writeThrough)
                    // Must persist inside synchronization in non-tx mode.
                    cctx.store().put(null, key, updated, ver);

                storeValue(updated, expireTime, ver);

                assert ttl != CU.TTL_ZERO;

                update(updated, expireTime, ttl, ver, true);

                logUpdate(op, updated, ver, expireTime, 0, true);

                if (evt) {
                    CacheObject evtOld = null;

                    if (transformCloClsName != null && cctx.events().isRecordable(EVT_CACHE_OBJECT_READ)) {
                        evtOld = cctx.unwrapTemporary(old);

                        cctx.events().addEvent(partition(), key, cctx.localNodeId(),null, null,
                            (GridCacheVersion)null, EVT_CACHE_OBJECT_READ, evtOld, evtOld != null || hadVal, evtOld,
                            evtOld != null || hadVal, subjId, transformCloClsName, taskName, keepBinary);
                    }

                    if (cctx.events().isRecordable(EVT_CACHE_OBJECT_PUT)) {
                        if (evtOld == null)
                            evtOld = cctx.unwrapTemporary(old);

                        cctx.events().addEvent(partition(), key, cctx.localNodeId(), null, null,
                            (GridCacheVersion)null, EVT_CACHE_OBJECT_PUT, updated, updated != null, evtOld,
                            evtOld != null || hadVal, subjId, null, taskName, keepBinary);
                    }
                }
            }
            else {
                if (writeThrough)
                    // Must persist inside synchronization in non-tx mode.
                    cctx.store().remove(null, key);

                removeValue(ver);

                update(null, CU.TTL_ETERNAL, CU.EXPIRE_TIME_ETERNAL, ver, true);

                logUpdate(op, null, ver, CU.EXPIRE_TIME_ETERNAL, 0, true);

                if (evt) {
                    CacheObject evtOld = null;

                    if (transformCloClsName != null && cctx.events().isRecordable(EVT_CACHE_OBJECT_READ))
                        cctx.events().addEvent(partition(), key, cctx.localNodeId(), null, null,
                            (GridCacheVersion)null, EVT_CACHE_OBJECT_READ, evtOld, evtOld != null || hadVal, evtOld,
                            evtOld != null || hadVal, subjId, transformCloClsName, taskName, keepBinary);

                    if (cctx.events().isRecordable(EVT_CACHE_OBJECT_REMOVED)) {
                        if (evtOld == null)
                            evtOld = cctx.unwrapTemporary(old);

                        cctx.events().addEvent(partition(), key, cctx.localNodeId(), null, null, (GridCacheVersion)null,
                            EVT_CACHE_OBJECT_REMOVED, null, false, evtOld, evtOld != null || hadVal, subjId, null,
                            taskName, keepBinary);
                    }
                }

                res = hadVal;
            }

            if (res)
                updateMetrics(op, metrics, transformOp, old != null);
            else if (op == DELETE && transformOp)
                cctx.cache().metrics0().onInvokeRemove(old != null);

            if (lsnrCol != null) {
                long updateCntr = nextPartitionCounter(AffinityTopologyVersion.NONE, true, false, null);

                cctx.continuousQueries().onEntryUpdated(
                    lsnrCol,
                    key,
                    val,
                    old,
                    internal,
                    partition(),
                    true,
                    false,
                    updateCntr,
                    null,
                    AffinityTopologyVersion.NONE);

                onUpdateFinished(updateCntr);
            }

            if (intercept) {
                if (op == UPDATE)
                    cctx.config().getInterceptor().onAfterPut(new CacheLazyEntry(cctx, key, key0, updated, updated0, keepBinary, 0L));
                else
                    cctx.config().getInterceptor().onAfterRemove(new CacheLazyEntry(cctx, key, key0, old, old0, keepBinary, 0L));
            }

            updatePlatformCache(op == UPDATE ? updated : null, cctx.affinity().affinityTopologyVersion());
        }
        finally {
            unlockEntry();
            unlockListenerReadLock();
        }

        return new GridTuple3<>(res,
            cctx.unwrapTemporary(interceptorRes != null ?
                interceptorRes.get2() :
                cctx.cacheObjectContext().unwrapBinaryIfNeeded(old, keepBinary, false, null)),
            invokeRes);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public GridCacheUpdateAtomicResult innerUpdate(
        final GridCacheVersion newVer,
        final UUID evtNodeId,
        final UUID affNodeId,
        final GridCacheOperation op,
        @Nullable final Object writeObj,
        @Nullable final Object[] invokeArgs,
        final boolean writeThrough,
        final boolean readThrough,
        final boolean retval,
        final boolean keepBinary,
        @Nullable final IgniteCacheExpiryPolicy expiryPlc,
        final boolean evt,
        final boolean metrics,
        final boolean primary,
        final boolean verCheck,
        final AffinityTopologyVersion topVer,
        @Nullable final CacheEntryPredicate[] filter,
        final GridDrType drType,
        final long explicitTtl,
        final long explicitExpireTime,
        @Nullable final GridCacheVersion conflictVer,
        final boolean conflictResolve,
        final boolean intercept,
        @Nullable final UUID subjId,
        final String taskName,
        @Nullable final CacheObject prevVal,
        @Nullable final Long updateCntr,
        @Nullable final GridDhtAtomicAbstractUpdateFuture fut,
        boolean transformOp
    ) throws IgniteCheckedException, GridCacheEntryRemovedException, GridClosureException {
        assert cctx.atomic() && !detached();

        AtomicCacheUpdateClosure c;

        if (!primary && !isNear())
            ensureFreeSpace();

        lockListenerReadLock();
        lockEntry();

        boolean reserved = false;

        try {
            checkObsolete();

            GridDhtLocalPartition locPart = localPartition();

            // Reserve before update to avoid writing to evicting partition.
            if (locPart != null && !(reserved = locPart.reserve()))
                throw new GridDhtInvalidPartitionException(partition(), "The partition can't be reserved");

            boolean internal = isInternal() || !context().userCache();

            Map<UUID, CacheContinuousQueryListener> lsnrs = cctx.continuousQueries().updateListeners(internal, false);

            boolean needVal = lsnrs != null || intercept || retval || op == GridCacheOperation.TRANSFORM
                || !F.isEmptyOrNulls(filter);

            // Possibly read value from store.
            boolean readFromStore = readThrough && needVal && (cctx.readThrough() &&
                (op == GridCacheOperation.TRANSFORM || cctx.loadPreviousValue()));

            c = new AtomicCacheUpdateClosure(this,
                topVer,
                newVer.copy(),
                op,
                writeObj,
                invokeArgs,
                readFromStore,
                writeThrough,
                keepBinary,
                expiryPlc,
                primary,
                verCheck,
                filter,
                explicitTtl,
                explicitExpireTime,
                conflictVer,
                conflictResolve,
                intercept,
                updateCntr,
                cctx.disableTriggeringCacheInterceptorOnConflict()
            );

            if (isNear()) {
                CacheDataRow dataRow = val != null ? new CacheDataRowAdapter(key, val, ver, expireTimeExtras()) : null;

                c.call(dataRow);
            }
            else {
                boolean condition =
                    op == DELETE &&
                    !needVal &&
                    !readFromStore &&
                    !transformOp &&
                    !(evt && (cctx.events().isRecordable(EVT_CACHE_OBJECT_REMOVED) || cctx.events().isRecordable(EVT_CACHE_OBJECT_EXPIRED))) &&
                    !cctx.queries().enabled() &&
                    cctx.config().getInterceptor() == null &&
                    !cctx.conflictNeedResolve() &&
                    cctx.cacheObjectContext().compressionStrategy() == null;

                if (condition) {
                    // If we don't need to read previous value and don't need to notify about remove,
                    // then we can optimize removing and do not load the value into heap.
                    c.rowData(CacheDataRowAdapter.RowData.NO_KEY_WITH_VALUE_META_INFO);
                }

                cctx.offheap().invoke(cctx, key, localPartition(), c);
            }

            GridCacheUpdateAtomicResult updateRes = c.updateRes;

            assert updateRes != null : c;

            // We should ignore expired old row or tombstone.
            // Expired oldRow instance is needed for correct row replacement or deletion only.
            CacheObject oldVal = c.oldRow != null && !c.ignoreOldValue ? c.oldRow.value() : null;
            CacheObject updateVal = null;
            GridCacheVersion updateVer = c.newVer;

            boolean updateMetrics = metrics && cctx.statisticsEnabled();

            // Apply metrics.
            if (updateMetrics &&
                updateRes.outcome().updateReadMetrics() &&
                needVal)
                cctx.cache().metrics0().onRead(oldVal != null);

            if (updateMetrics && INVOKE_NO_OP == updateRes.outcome() && (transformOp || updateRes.transformed()))
                cctx.cache().metrics0().onReadOnlyInvoke(oldVal != null);
            else if (updateMetrics && REMOVE_NO_VAL == updateRes.outcome()
                && (transformOp || updateRes.transformed()))
                cctx.cache().metrics0().onInvokeRemove(oldVal != null);

            switch (updateRes.outcome()) {
                case VERSION_CHECK_FAILED: {
                    if (!cctx.isNear()) {
                        CacheObject evtVal;

                        if (op == GridCacheOperation.TRANSFORM) {
                            EntryProcessor<Object, Object, ?> entryProcessor =
                                (EntryProcessor<Object, Object, ?>)writeObj;

                            CacheInvokeEntry<Object, Object> entry =
                                new CacheInvokeEntry<>(key, prevVal, version(), keepBinary, this);

                            IgniteThread.onEntryProcessorEntered(true);

                            try {
                                entryProcessor.process(entry, invokeArgs);

                                evtVal = entry.modified() ?
                                    cctx.toCacheObject(cctx.unwrapTemporary(entry.getValue())) : prevVal;
                            }
                            catch (Exception ignore) {
                                evtVal = prevVal;
                            }
                            finally {
                                IgniteThread.onEntryProcessorLeft();
                            }
                        }
                        else
                            evtVal = (CacheObject)writeObj;

                        assert !primary && updateCntr != null;

                        onUpdateFinished(updateCntr);

                        cctx.continuousQueries().onEntryUpdated(
                            key,
                            evtVal,
                            prevVal,
                            isInternal() || !context().userCache(),
                            partition(),
                            primary,
                            false,
                            updateCntr,
                            null,
                            topVer);
                    }

                    return updateRes;
                }

                case CONFLICT_USE_OLD:
                case FILTER_FAILED:
                case INVOKE_NO_OP:
                case INTERCEPTOR_CANCEL:
                    return updateRes;
            }

            assert updateRes.outcome() == UpdateOutcome.SUCCESS || updateRes.outcome() == UpdateOutcome.REMOVE_NO_VAL;

            CacheObject evtOld = null;

            if (evt && op == TRANSFORM && cctx.events().isRecordable(EVT_CACHE_OBJECT_READ)) {
                assert writeObj instanceof EntryProcessor : writeObj;

                evtOld = cctx.unwrapTemporary(oldVal);

                Object transformClo = EntryProcessorResourceInjectorProxy.unwrap(writeObj);

                cctx.events().addEvent(partition(),
                    key,
                    evtNodeId,
                    null,
                    null,
                    updateVer,
                    EVT_CACHE_OBJECT_READ,
                    evtOld, evtOld != null,
                    evtOld, evtOld != null,
                    subjId,
                    transformClo.getClass().getName(),
                    taskName,
                    keepBinary);
            }

            if (c.op == UPDATE) {
                updateVal = val;

                assert updateVal != null : c;

                drReplicate(drType, updateVal, updateVer, topVer);

                recordNodeId(affNodeId, topVer);

                if (evt && cctx.events().isRecordable(EVT_CACHE_OBJECT_PUT)) {
                    if (evtOld == null)
                        evtOld = cctx.unwrapTemporary(oldVal);

                    cctx.events().addEvent(partition(),
                        key,
                        evtNodeId,
                        null,
                        null,
                        updateVer,
                        EVT_CACHE_OBJECT_PUT,
                        updateVal,
                        true,
                        evtOld,
                        evtOld != null,
                        subjId,
                        null,
                        taskName,
                        keepBinary);
                }
            }
            else {
                assert c.op == DELETE : c.op;

                clearReaders();

                drReplicate(drType, null, updateVer, topVer);

                recordNodeId(affNodeId, topVer);

                if (evt && cctx.events().isRecordable(EVT_CACHE_OBJECT_REMOVED)) {
                    if (evtOld == null)
                        evtOld = cctx.unwrapTemporary(oldVal);

                    cctx.events().addEvent(partition(),
                        key,
                        evtNodeId,
                        null,
                        null,
                        updateVer,
                        EVT_CACHE_OBJECT_REMOVED,
                        null, false,
                        evtOld, evtOld != null,
                        subjId,
                        null,
                        taskName,
                        keepBinary);
                }
            }

            if (updateRes.success())
                updateMetrics(c.op, metrics, transformOp || updateRes.transformed(), oldVal != null);

            // Continuous query filter should be performed under the lock.
            if (lsnrs != null) {
                CacheObject evtVal = cctx.unwrapTemporary(updateVal);
                CacheObject evtOldVal = cctx.unwrapTemporary(oldVal);

                cctx.continuousQueries().onEntryUpdated(lsnrs,
                    key,
                    evtVal,
                    evtOldVal,
                    internal,
                    partition(),
                    primary,
                    false,
                    c.updateRes.updateCounter(),
                    fut,
                    topVer);
            }

            if (intercept && c.wasIntercepted) {
                assert c.op == UPDATE || c.op == DELETE : c.op;

                Cache.Entry<?, ?> entry = new CacheLazyEntry<>(
                    cctx,
                    key,
                    null,
                    c.op == UPDATE ? updateVal : oldVal,
                    null,
                    keepBinary,
                    c.updateRes.updateCounter()
                );

                if (c.op == UPDATE)
                    cctx.config().getInterceptor().onAfterPut(entry);
                else
                    cctx.config().getInterceptor().onAfterRemove(entry);
            }

            updatePlatformCache(c.op == UPDATE ? updateVal : null, topVer);
        }
        finally {
            if (reserved)
                localPartition().release();

            unlockEntry();
            unlockListenerReadLock();
        }

        onUpdateFinished(c.updateRes.updateCounter());

        return c.updateRes;
    }

    /**
     * @param val Value.
     * @param cacheObj Cache object.
     * @param keepBinary Keep binary flag.
     * @param cpy Copy flag.
     * @return Cache object value.
     */
    @Nullable private Object value(@Nullable Object val, @Nullable CacheObject cacheObj, boolean keepBinary, boolean cpy) {
        if (val != null)
            return val;

        return cctx.unwrapBinaryIfNeeded(cacheObj, keepBinary, cpy, null);
    }

    /**
     * @param expiry Expiration policy.
     * @return Tuple holding initial TTL and expire time with the given expiry.
     */
    private static IgniteBiTuple<Long, Long> initialTtlAndExpireTime(IgniteCacheExpiryPolicy expiry) {
        assert expiry != null;

        long initTtl = expiry.forCreate();
        long initExpireTime;

        if (initTtl == CU.TTL_ZERO) {
            initTtl = CU.TTL_MINIMUM;
            initExpireTime = CU.expireTimeInPast();
        }
        else if (initTtl == CU.TTL_NOT_CHANGED) {
            initTtl = CU.TTL_ETERNAL;
            initExpireTime = CU.EXPIRE_TIME_ETERNAL;
        }
        else
            initExpireTime = CU.toExpireTime(initTtl);

        return F.t(initTtl, initExpireTime);
    }

    /**
     * Get TTL, expire time and remove flag for the given entry, expiration policy and explicit TTL and expire time.
     *
     * @param expiry Expiration policy.
     * @param ttl Explicit TTL.
     * @param expireTime Explicit expire time.
     * @return Result.
     */
    private GridTuple3<Long, Long, Boolean> ttlAndExpireTime(IgniteCacheExpiryPolicy expiry, long ttl, long expireTime) {
        assert !obsolete();

        boolean rmv = false;

        // 1. If TTL is not changed, then calculate it based on expiry.
        if (ttl == CU.TTL_NOT_CHANGED) {
            if (expiry != null)
                ttl = hasValueUnlocked() ? expiry.forUpdate() : expiry.forCreate();
        }

        // 2. If TTL is zero, then set delete marker.
        if (ttl == CU.TTL_ZERO) {
            rmv = true;

            ttl = CU.TTL_ETERNAL;
        }

        // 3. If TTL is still not changed, then either use old entry TTL or set it to "ETERNAL".
        // If a tombstone value is replaced with a new, use "ETERNAL".
        if (ttl == CU.TTL_NOT_CHANGED) {
            if (isStartVersion())
                ttl = CU.TTL_ETERNAL;
            else {
                ttl = ttlExtras();
                expireTime = deletedUnlocked() ? CU.EXPIRE_TIME_ETERNAL : expireTimeExtras();
            }
        }

        // 4 If expire time was not set explicitly, then calculate it.
        if (expireTime == CU.EXPIRE_TIME_CALCULATE)
            expireTime = CU.toExpireTime(ttl);

        return F.t(ttl, expireTime, rmv);
    }

    /**
     * Perform DR if needed.
     *
     * @param drType DR type.
     * @param val Value.
     * @param ver Version.
     * @param topVer Topology version.
     * @throws IgniteCheckedException In case of exception.
     */
    private void drReplicate(GridDrType drType, @Nullable CacheObject val, GridCacheVersion ver, AffinityTopologyVersion topVer)
        throws IgniteCheckedException {
        if (cctx.isDrEnabled() && drType != DR_NONE && !isInternal()) {
            if (log.isTraceEnabled())
                traceReplicate(ver, keyValueToLog());

            cctx.dr().replicate(key, val, rawTtl(), rawExpireTime(), ver.conflictVersion(), drType, topVer);
        }
    }

    private String keyValueToLog() {
        GridToStringBuilder.SensitiveDataLogging sensitiveDataLogging = S.getSensitiveDataLogging();
        String keyValue = "nil";

        if (sensitiveDataLogging == PLAIN)
            keyValue = keyValue(false).toString();
        else if (sensitiveDataLogging == HASH)
            keyValue = String.valueOf(IgniteUtils.hash(keyValue(false)));

        return keyValue;
    }

    /**
     * @return {@code true} if entry has readers. It makes sense only for dht entry.
     * @throws GridCacheEntryRemovedException If removed.
     */
    protected boolean hasReaders() throws GridCacheEntryRemovedException {
        return false;
    }

    /**
     *
     */
    protected void clearReaders() {
        // No-op.
    }

    /**
     * @param nodeId Node ID to clear.
     * @throws GridCacheEntryRemovedException If removed.
     */
    protected void clearReader(UUID nodeId) throws GridCacheEntryRemovedException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public boolean clear(GridCacheVersion ver, boolean readers) throws IgniteCheckedException {
        lockEntry();

        try {
            if (obsolete())
                return false;

            try {
                if (isStartVersion())
                    unswap(null, false);

                if (deletedUnlocked()) // Skip tombstone to avoid clear counter adjust on local operation.
                    return false;

                if ((!hasReaders() || readers)) {
                    // markObsolete will clear the value.
                    if (!(markObsolete0(ver, true, null))) {
                        if (log.isDebugEnabled())
                            log.debug("Entry could not be marked obsolete (it is still used): " + this);

                        return false;
                    }

                    clearReaders();
                }
                else {
                    if (log.isDebugEnabled())
                        log.debug("Entry could not be marked obsolete (it still has readers): " + this);

                    return false;
                }
            }
            catch (GridCacheEntryRemovedException ignore) {
                assert false;

                return false;
            }

            if (log.isTraceEnabled()) {
                log.trace("entry clear [key=" + key +
                    ", entry=" + System.identityHashCode(this) +
                    ", val=" + val + ']');
            }

            if (cctx.mvccEnabled())
                cctx.offheap().mvccRemoveAll(this);
            else
                removeExpiredValue(ver);
        }
        finally {
            unlockEntry();
        }

        onMarkedObsolete();

        cctx.cache().removeEntry(this); // Clear cache.

        return true;
    }

    /** {@inheritDoc} */
    @Override public GridCacheVersion obsoleteVersion() {
        lockEntry();

        try {
            return obsoleteVersionExtras();
        }
        finally {
            unlockEntry();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean markObsolete(GridCacheVersion ver) {
        boolean obsolete;

        lockEntry();

        try {
            obsolete = markObsolete0(ver, true, null);
        }
        finally {
            unlockEntry();
        }

        if (obsolete)
            onMarkedObsolete();

        return obsolete;
    }

    /** {@inheritDoc} */
    @Override public boolean markObsoleteIfEmpty(@Nullable GridCacheVersion obsoleteVer) throws IgniteCheckedException {
        boolean obsolete = false;
        GridCacheVersion ver0 = null;

        lockEntry();

        try {
            if (obsoleteVersionExtras() != null)
                return false;

            if (hasValueUnlocked()) {
                if (checkExpired()) {
                    if (obsoleteVer == null)
                        obsoleteVer = nextVersion();

                    if (onExpired(this.val, obsoleteVer))
                        obsolete = true;
                }
            }
            else {
                if (obsoleteVer == null)
                    obsoleteVer = nextVersion();

                obsolete = markObsolete0(obsoleteVer, true, null);
            }
        }
        finally {
            unlockEntry();

            if (obsolete)
                onMarkedObsolete();
        }

        return obsolete;
    }

    /** {@inheritDoc} */
    @Override public boolean markObsoleteVersion(GridCacheVersion ver) {
        boolean marked;

        lockEntry();

        try {
            if (obsoleteVersionExtras() != null)
                return true;

            if (!this.ver.equals(ver))
                return false;

            marked = markObsolete0(ver, true, null);
        }
        finally {
            unlockEntry();
        }

        if (marked)
            onMarkedObsolete();

        return marked;
    }

    /**
     * @return {@code True} if this entry should not be evicted from cache.
     */
    protected boolean evictionDisabled() {
        return (flags & IS_EVICT_DISABLED) != 0;
    }

    /**
     * <p>
     * Note that {@link #onMarkedObsolete()} should always be called after this method returns {@code true}.
     *
     * @param ver Version.
     * @param clear {@code True} to clear.
     * @param extras Predefined extras.
     * @return {@code True} if entry is obsolete, {@code false} if entry is still used by other threads or nodes.
     */
    protected final boolean markObsolete0(GridCacheVersion ver, boolean clear, GridCacheObsoleteEntryExtras extras) {
        assert lock.isHeldByCurrentThread();

        if (evictionDisabled()) {
            assert !obsolete() : this;

            return false;
        }

        GridCacheVersion obsoleteVer = obsoleteVersionExtras();

        if (ver != null) {
            // If already obsolete, then do nothing.
            if (obsoleteVer != null)
                return true;

            GridCacheMvcc mvcc = mvccExtras();

            if (mvcc == null || mvcc.isEmpty(ver)) {
                obsoleteVer = ver;

                obsoleteVersionExtras(obsoleteVer, extras);

                if (clear)
                    value(null);

                if (log.isTraceEnabled()) {
                    log.trace("markObsolete0 [key=" + key +
                        ", entry=" + System.identityHashCode(this) +
                        ", clear=" + clear +
                        ']');
                }
            }

            return obsoleteVer != null;
        }
        else
            return obsoleteVer != null;
    }

    /** {@inheritDoc} */
    @Override public void onMarkedObsolete() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public final boolean obsolete() {
        lockEntry();

        try {
            return obsoleteVersionExtras() != null;
        }
        finally {
            unlockEntry();
        }
    }

    /** {@inheritDoc} */
    @Override public final boolean obsolete(GridCacheVersion exclude) {
        lockEntry();

        try {
            GridCacheVersion obsoleteVer = obsoleteVersionExtras();

            return obsoleteVer != null && !obsoleteVer.equals(exclude);
        }
        finally {
            unlockEntry();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean invalidate(GridCacheVersion newVer)
        throws IgniteCheckedException {
        lockEntry();

        try {
            assert newVer != null;

            value(null);

            ver = newVer;
            flags &= ~IS_EVICT_DISABLED;

            onInvalidate();

            return obsoleteVersionExtras() != null;
        }
        finally {
            unlockEntry();
        }
    }

    /**
     * Called when entry invalidated.
     */
    protected void onInvalidate() {
        // No-op.
    }

    /**
     * @param val New value.
     * @param expireTime Expiration time.
     * @param ttl Time to live.
     * @param ver Update version.
     */
    protected final void update(@Nullable CacheObject val, long expireTime, long ttl, GridCacheVersion ver, boolean addTracked) {
        assert ver != null;
        assert lock.isHeldByCurrentThread();
        assert ttl != CU.TTL_ZERO && ttl != CU.TTL_NOT_CHANGED && ttl >= 0 : ttl;

        boolean trackNear = addTracked && isNear() && cctx.config().isEagerTtl();

        long oldExpireTime = expireTimeExtras();

        if (trackNear && oldExpireTime != 0 && (expireTime != oldExpireTime || isStartVersion()))
            cctx.ttl().removeTrackedEntry((GridNearCacheEntry)this);

        value(val);

        ttlAndExpireTimeExtras(ttl, expireTime);

        this.ver = ver;
        flags &= ~IS_EVICT_DISABLED;

        if (trackNear && expireTime != 0 && (expireTime != oldExpireTime || isStartVersion()))
            cctx.ttl().addTrackedEntry((GridNearCacheEntry)this);
    }

    /**
     * @return {@code True} if should notify continuous query manager on updates of this entry.
     */
    private boolean notifyContinuousQueries() {
        return !isNear();
    }

    /**
     * Update TTL if it is changed.
     *
     * @param expiryPlc Expiry policy.
     */
    private void updateTtl(ExpiryPolicy expiryPlc) throws IgniteCheckedException, GridCacheEntryRemovedException {
        long ttl = CU.toTtl(expiryPlc.getExpiryForAccess());

        if (ttl != CU.TTL_NOT_CHANGED)
            updateTtl(ttl, false);
    }

    /**
     * Update TTL if it is changed.
     *
     * @param expiryPlc Expiry policy.
     * @throws GridCacheEntryRemovedException If failed.
     */
    private void updateTtl(IgniteCacheExpiryPolicy expiryPlc) throws GridCacheEntryRemovedException,
        IgniteCheckedException {
        long ttl = expiryPlc.forAccess();

        if (ttl != CU.TTL_NOT_CHANGED) {
            updateTtl(ttl, isFastUpdateTtlPossible());

            expiryPlc.ttlUpdated(key(), version(), hasReaders() ? ((GridDhtCacheEntry)this).readers() : null);
        }
    }

    /**
     * @param ttl Time to live.
     * @param fastUpdate If {@code true} then implementation uses a fast way to update the time-to-live value in the persistence store,
     *                   it updates only pages that store ttl instead of rewriting the whole entry.
     * @see #isFastUpdateTtlPossible()
     */
    private void updateTtl(long ttl, boolean fastUpdate) throws IgniteCheckedException, GridCacheEntryRemovedException {
        assert ttl >= 0 || ttl == CU.TTL_ZERO : ttl;
        assert lock.isHeldByCurrentThread();

        long expireTime;

        if (ttl == CU.TTL_ZERO) {
            ttl = CU.TTL_MINIMUM;
            expireTime = CU.expireTimeInPast();
        }
        else
            expireTime = CU.toExpireTime(ttl);

        ttlAndExpireTimeExtras(ttl, expireTime);

        cctx.shared().database().checkpointReadLock();

        try {
            if (fastUpdate)
                storeTtlValue(val, expireTime, ver);
            else
                storeValue(val, expireTime, ver);
        }
        finally {
            cctx.shared().database().checkpointReadUnlock();
        }
    }

    /**
     * Returns {@code true} if fast update of TTL is possible for this entry and {@code false} otherwise.
     * For now, fast update is not possible if any of the following conditions are met:
     * <ul>
     *     <li>SQL indexes are enabled.</li>
     *     <li>MVCC is enabled.</li>
     *     <li>Compression is enabled.</li>
     *     <li>Entry is near.</li>
     * </ul>
     * @return {@code true} if fast update of TTL is possible for this entry and {@code false} otherwise.
     */
    private boolean isFastUpdateTtlPossible() {
        if (cctx.queries().enabled() || cctx.mvccEnabled() || cctx.cacheObjectContext().compressionStrategy() != null || isNear()) {
            // Fast update is not possible for this entry. Need to fall back to writing the whole entry.
            //  - in general, enabling sql indices should not prevent from using fast update. Can be improved later.
            //  - mvcc and compression are not supported for fast update yet.
            //  - near entries only require update on the entry itself.
            return false;

        }

        return true;
    }

    /**
     * @throws GridCacheEntryRemovedException If entry is obsolete.
     */
    protected void checkObsolete() throws GridCacheEntryRemovedException {
        assert lock.isHeldByCurrentThread();

        if (obsoleteVersionExtras() != null)
            throw new GridCacheEntryRemovedException();
    }

    /** {@inheritDoc} */
    @Override public KeyCacheObject key() {
        return key;
    }

    /** {@inheritDoc} */
    @Override public IgniteTxKey txKey() {
        return cctx.txKey(key);
    }

    /** {@inheritDoc} */
    @Override public GridCacheVersion version() throws GridCacheEntryRemovedException {
        lockEntry();

        try {
            checkObsolete();

            return ver;
        }
        finally {
            unlockEntry();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean checkSerializableReadVersion(GridCacheVersion serReadVer)
        throws GridCacheEntryRemovedException {
        lockEntry();

        try {
            checkObsolete();

            if (!serReadVer.equals(ver)) {
                boolean empty = isStartVersion() || val == null;

                if (serReadVer.equals(IgniteTxEntry.SER_READ_EMPTY_ENTRY_VER))
                    return empty;
                else if (serReadVer.equals(IgniteTxEntry.SER_READ_NOT_EMPTY_VER))
                    return !empty;

                return false;
            }

            return true;
        }
        finally {
            unlockEntry();
        }
    }

    /**
     * Gets hash value for the entry key.
     *
     * @return Hash value.
     */
    int hash() {
        return hash;
    }

    /** {@inheritDoc} */
    @Nullable @Override public CacheObject mvccPeek(boolean onheapOnly)
        throws GridCacheEntryRemovedException, IgniteCheckedException {
        if (onheapOnly)
            return null;

        lockEntry();

        try {
            checkObsolete();

            CacheDataRow row = cctx.offheap().mvccRead(cctx, key, MVCC_MAX_SNAPSHOT);

            return row != null ? row.value() : null;
        }
        finally {
            unlockEntry();
        }
    }

    /** {@inheritDoc} */
    @Nullable @Override public CacheObject peek(
        boolean heap,
        boolean offheap,
        AffinityTopologyVersion topVer,
        @Nullable IgniteCacheExpiryPolicy expiryPlc)
        throws GridCacheEntryRemovedException, IgniteCheckedException {
        assert heap || offheap;

        boolean rmv = false;

        try {
            lockEntry();

            try {
                checkObsolete();

                if (!valid(topVer))
                    return null;

                if (val == null && offheap)
                    unswap(null, false);

                if (checkExpired()) {
                    rmv = onExpired(val, null);

                    return null;
                }
                else {
                    CacheObject val = this.val;

                    if (val != null && expiryPlc != null)
                        updateTtl(expiryPlc);

                    return val;
                }
            }
            finally {
                unlockEntry();
            }
        }
        finally {
            if (rmv) {
                onMarkedObsolete();

                cctx.cache().removeEntry(this);
            }
        }
    }

    /** {@inheritDoc} */
    @Nullable @Override public CacheObject peek()
        throws GridCacheEntryRemovedException, IgniteCheckedException {
        IgniteInternalTx tx = cctx.tm().localTx();

        AffinityTopologyVersion topVer = tx != null ? tx.topologyVersion() : cctx.affinity().affinityTopologyVersion();

        return peek(true, false, topVer, null);
    }

    /** {@inheritDoc} */
    @Override public int localSize(AffinityTopologyVersion topVer) throws GridCacheEntryRemovedException, IgniteCheckedException {
        lockEntry();

        try {
            checkObsolete();

            if (valid(topVer)) {
                CacheObject val = this.val;
                boolean isExpired = false;

                if (val == null) {
                    CacheDataRow row = cctx.offheap().find(this, false);

                    if (row != null && !row.tombstone()) {
                        val = row.value();
                        isExpired = row.expireTime() > 0 && row.expireTime() < U.currentTimeMillis();
                    }
                }
                else
                    isExpired = checkExpired();

                if (val == null || isExpired)
                    return 0;

                key.prepareMarshal(cctx.cacheObjectContext());

                val.prepareMarshal(cctx.cacheObjectContext());

                return key.valueBytesOriginLength(cctx.cacheObjectContext()) + val.valueBytesOriginLength(cctx.cacheObjectContext());
            }
        }
        finally {
            unlockEntry();
        }

        // Entry is not valid. Just return zero value.
        return 0;
    }

    /**
     * TODO: IGNITE-3500: do we need to generate event and invalidate value?
     *
     * @return {@code True} if expired.
     */
    private boolean checkExpired() {
        long expireTime = expireTimeExtras();

        return expireTime > 0 && expireTime < U.currentTimeMillis();
    }

    /**
     * @return Value.
     */
    @Override public CacheObject rawGet() {
        lockEntry();

        try {
            return val;
        }
        finally {
            unlockEntry();
        }
    }

    /** {@inheritDoc} */
    @Override public final boolean hasValue() {
        lockEntry();

        try {
            return hasValueUnlocked();
        }
        finally {
            unlockEntry();
        }
    }

    /**
     * @return {@code True} if this entry has value.
     */
    protected final boolean hasValueUnlocked() {
        assert lock.isHeldByCurrentThread();

        return val != null;
    }

    /**
     * Checks, that changes were got by DR.
     *
     * @param explicitVer – Explicit version (if any).
     * @return {@code true} if changes were got by DR and {@code false} otherwise.
     */
    private boolean isRemoteDrUpdate(@Nullable GridCacheVersion explicitVer) {
        return explicitVer != null && explicitVer.dataCenterId() != cctx.dr().dataCenterId();
    }

    /**
     * Checks, that cache interceptor should be skipped.
     * <p>
     * It is expects by default behavior that Interceptor methods ({@link CacheInterceptor#onBeforePut(Cache.Entry,
     * Object)}, {@link CacheInterceptor#onAfterPut(Cache.Entry)}, {@link CacheInterceptor#onBeforeRemove(Cache.Entry)}
     * and {@link CacheInterceptor#onAfterRemove(Cache.Entry)}) will be called, but {@link
     * CacheInterceptor#onGet(Object, Object)}. This can even make DR-update flow broken in case of non-idempotent
     * Interceptor and force users to call onGet manually as the only workaround. Also, user may want to skip
     * Interceptor to avoid redundant entry transformation for DR updates and exchange with internal data b/w data
     * centres which is a normal case.
     *
     * @param explicitVer - Explicit version (if any).
     * @return {@code true} if cache interceptor should be skipped and {@code false} otherwise.
     */
    private boolean skipInterceptor(@Nullable GridCacheVersion explicitVer) {
        return isRemoteDrUpdate(explicitVer) && cctx.disableTriggeringCacheInterceptorOnConflict();
    }

    /** {@inheritDoc} */
    @Override public CacheObject rawPut(CacheObject val, long ttl) {
        lockEntry();

        try {
            CacheObject old = this.val;

            update(val, CU.toExpireTime(ttl), ttl, nextVersion(), true);

            return old;
        }
        finally {
            unlockEntry();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean initialValue(
        CacheObject val,
        GridCacheVersion ver,
        MvccVersion mvccVer,
        MvccVersion newMvccVer,
        byte mvccTxState,
        byte newMvccTxState,
        long ttl,
        long expireTime,
        boolean preload,
        AffinityTopologyVersion topVer,
        GridDrType drType,
        boolean fromStore,
        boolean primary,
        CacheDataRow row
    ) throws IgniteCheckedException, GridCacheEntryRemovedException {
        assert !primary || !(preload || fromStore);

        ensureFreeSpace();

        boolean obsolete = false;

        lockListenerReadLock();
        lockEntry();

        try {
            checkObsolete();

            boolean walEnabled = !cctx.isNear() && cctx.group().persistenceEnabled() && cctx.group().walEnabled();

            long expTime = expireTime < 0 ? CU.toExpireTime(ttl) : expireTime;

            if (val != null)
                val = val.prepareForCache(cctx.cacheObjectContext(), true);

            final boolean unswapped = (flags & IS_UNSWAPPED_MASK) != 0;

            boolean update;

            IgniteBiPredicate<CacheObject, GridCacheVersion> p = (oldVal, oldVer) -> {
                GridCacheVersion testVer = oldVer != null ? oldVer : this.ver;

                return preload ? cctx.shared().versions().isStartVersion(testVer) || testVer.compareTo(ver) < 0 : oldVal == null;
            };

            long updateCntr = 0;

            if (unswapped) {
                update = p.apply(this.val, this.ver);

                if (update) {
                    // If entry is already unswapped and we are modifying it, we must run deletion callbacks for old value.
                    long oldExpTime = expireTimeUnlocked();

                    if (oldExpTime > 0 && oldExpTime < U.currentTimeMillis()) {
                        if (onExpired(this.val, null))
                            obsolete = true;
                    }

                    if (cctx.mvccEnabled()) {
                        assert !preload;

                        cctx.offheap().mvccInitialValue(this, val, ver, expTime, mvccVer, newMvccVer);
                    }
                    else {
                        if (!preload)
                            ver.updateCounter((updateCntr = nextPartitionCounter(topVer, true, true, null)));

                        storeValue(val, expTime, ver);
                    }
                }
            }
            else {
                if (cctx.mvccEnabled()) {
                    // cannot identify whether the entry is exist on the fly
                    unswap(false);

                    if (update = p.apply(this.val, this.ver)) {
                        // If entry is already unswapped and we are modifying it, we must run deletion callbacks for old value.
                        long oldExpTime = expireTimeUnlocked();
                        long delta = (oldExpTime == 0 ? 0 : oldExpTime - U.currentTimeMillis());

                        if (delta < 0) {
                            if (onExpired(this.val, null))
                                obsolete = true;
                        }

                        assert !preload;

                        cctx.offheap().mvccInitialValue(this, val, ver, expTime, mvccVer, newMvccVer);
                    }
                }
                else {
                    if (!preload)
                        ver.updateCounter((updateCntr = nextPartitionCounter(topVer, true, true, null)));

                    // Optimization to access storage only once.
                    UpdateClosure c = storeValue(val, expTime, ver, p);

                    update = c.operationType() != IgniteTree.OperationType.NOOP;
                }
            }

            if (update) {
                update(val, expTime, ttl, ver, true);

                if (walEnabled) {
                    if (cctx.mvccEnabled()) {
                        cctx.shared().wal().log(new MvccDataRecord(new MvccDataEntry(
                            cctx.cacheId(),
                            key,
                            val,
                            val == null ? DELETE : GridCacheOperation.CREATE,
                            null,
                            ver,
                            expireTime,
                            partition(),
                            updateCntr,
                            mvccVer == null ? MvccUtils.INITIAL_VERSION : mvccVer
                        )));
                    } else {
                        cctx.shared().wal().log(new DataRecord(new DataEntry(
                            cctx.cacheId(),
                            key,
                            val,
                            val == null ? DELETE : GridCacheOperation.CREATE,
                            null,
                            ver,
                            expireTime,
                            partition(),
                            updateCntr,
                            DataEntry.flags(primary, preload, fromStore)
                        )));
                    }
                }

                drReplicate(drType, val, ver, topVer);

                cctx.continuousQueries().onEntryUpdated(
                    key,
                    val,
                    null,
                    this.isInternal() || !this.context().userCache(),
                    this.partition(),
                    true,
                    preload,
                    updateCntr,
                    null,
                    topVer);

                updatePlatformCache(val, topVer);

                onUpdateFinished(updateCntr);

                if (!fromStore && cctx.store().isLocal()) {
                    if (val != null)
                        cctx.store().put(null, key, val, ver);
                }

                return true;
            }

            return false;
        }
        finally {
            unlockEntry();
            unlockListenerReadLock();

            // It is necessary to execute these callbacks outside of lock to avoid deadlocks.

            if (obsolete) {
                onMarkedObsolete();

                cctx.cache().removeEntry(this);
            }
        }
    }

    /**
     * @param cntr Updated partition counter.
     */
    protected void onUpdateFinished(long cntr) {
        // No-op.
    }

    /**
     * @param topVer Topology version for current operation.
     * @param primary Primary node update flag.
     * @param initial {@code True} if initial value.
     * @param primaryCntr Counter assigned on primary node.
     * @return Update counter.
     */
    protected long nextPartitionCounter(AffinityTopologyVersion topVer, boolean primary, boolean initial,
        @Nullable Long primaryCntr) {
        return 0;
    }

    /**
     * @param tx Tx.
     * @param updateCntr Update counter.
     */
    protected long nextPartitionCounter(IgniteInternalTx tx, @Nullable Long updateCntr) {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public GridCacheVersionedEntryEx versionedEntry(final boolean keepBinary)
        throws IgniteCheckedException, GridCacheEntryRemovedException {
        lockEntry();

        try {
            boolean isNew = isStartVersion();

            if (isNew)
                unswap(null, false);

            CacheObject val = this.val;

            return new GridCacheLazyPlainVersionedEntry<>(cctx,
                key,
                val,
                ttlExtras(),
                expireTimeExtras(),
                ver.conflictVersion(),
                isNew,
                keepBinary);
        }
        finally {
            unlockEntry();
        }
    }

    /** {@inheritDoc} */
    @Override public void clearReserveForLoad(GridCacheVersion ver) {
        lockEntry();

        try {
            if (obsoleteVersionExtras() != null)
                return;

            if (ver.equals(this.ver)) {
                assert evictionDisabled() : this;

                flags &= ~IS_EVICT_DISABLED;
            }
        }
        finally {
            unlockEntry();
        }
    }

    /** {@inheritDoc} */
    @Override public EntryGetResult versionedValue(CacheObject val,
        GridCacheVersion curVer,
        GridCacheVersion newVer,
        @Nullable IgniteCacheExpiryPolicy loadExpiryPlc,
        @Nullable ReaderArguments readerArgs
    ) throws IgniteCheckedException, GridCacheEntryRemovedException {
        lockEntry();

        try {
            checkObsolete();

            addReaderIfNeed(readerArgs);

            if (curVer == null || curVer.equals(ver)) {
                if (val != this.val) {
                    GridCacheMvcc mvcc = mvccExtras();

                    if (mvcc != null && !mvcc.isEmpty())
                        return entryGetResult(this.val, ver, false);

                    if (newVer == null)
                        newVer = cctx.cache().nextVersion();

                    long ttl;
                    long expTime;

                    if (loadExpiryPlc != null) {
                        IgniteBiTuple<Long, Long> initTtlAndExpireTime = initialTtlAndExpireTime(loadExpiryPlc);

                        ttl = initTtlAndExpireTime.get1();
                        expTime = initTtlAndExpireTime.get2();
                    }
                    else {
                        ttl = ttlExtras();
                        expTime = expireTimeExtras();
                    }

                    if (val != null) {
                        // Detach value before index update.
                        val = val.prepareForCache(cctx.cacheObjectContext(), true);

                        storeValue(val, expTime, newVer);
                    }

                    // Version does not change for load ops.
                    update(val, expTime, ttl, newVer, true);

                    return entryGetResult(val, newVer, false);
                }

                assert !evictionDisabled() : this;
            }

            return entryGetResult(this.val, ver, false);
        }
        finally {
            unlockEntry();
        }
    }

    /**
     * @param readerArgs Reader arguments
     */
    private void addReaderIfNeed(@Nullable ReaderArguments readerArgs) {
        if (readerArgs != null) {
            assert this instanceof GridDhtCacheEntry : this;
            assert lock.isHeldByCurrentThread();

            try {
                ((GridDhtCacheEntry)this).addReader(readerArgs.reader(),
                    readerArgs.messageId(),
                    readerArgs.topologyVersion());
            }
            catch (GridCacheEntryRemovedException e) {
                // Entry was expired.
            }
        }
    }

    /**
     * Gets next version for this cache entry.
     *
     * @return Next version.
     */
    private GridCacheVersion nextVersion() {
        // Do not change topology version when generating next version.
        return cctx.versions().next(ver);
    }

    /**
     * Calculate next version for entry.
     *
     * Prefers explicit version if it is provided,
     * then tries tx write version if tx provided,
     * otherwise generate a new version.
     *
     * @param explicitVer Explicit version.
     */
    private GridCacheVersion nextVersion(GridCacheVersion explicitVer, IgniteInternalTx tx) {
        if (explicitVer != null)
            return explicitVer.copy();

        if (tx == null)
            return nextVersion();

        GridCacheVersion newVer = tx.writeVersion();

        assert newVer != null : "Failed to get write version for tx: " + tx;

        return newVer.copy(); // clone.
    }

    /** {@inheritDoc} */
    @Override public boolean hasLockCandidate(GridCacheVersion ver) throws GridCacheEntryRemovedException {
        lockEntry();

        try {
            checkObsolete();

            GridCacheMvcc mvcc = mvccExtras();

            return mvcc != null && mvcc.hasCandidate(ver);
        }
        finally {
            unlockEntry();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean hasLockCandidate(long threadId) throws GridCacheEntryRemovedException {
        lockEntry();

        try {
            checkObsolete();

            GridCacheMvcc mvcc = mvccExtras();

            return mvcc != null && mvcc.localCandidate(threadId) != null;
        }
        finally {
            unlockEntry();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean lockedByAny(GridCacheVersion... exclude)
        throws GridCacheEntryRemovedException {
        lockEntry();

        try {
            checkObsolete();

            GridCacheMvcc mvcc = mvccExtras();

            return mvcc != null && !mvcc.isEmpty(exclude);
        }
        finally {
            unlockEntry();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean lockedByThread() throws GridCacheEntryRemovedException {
        return lockedByThread(Thread.currentThread().getId());
    }

    /** {@inheritDoc} */
    @Override public boolean lockedLocally(GridCacheVersion lockVer)
        throws GridCacheEntryRemovedException {
        lockEntry();

        try {
            checkObsolete();

            GridCacheMvcc mvcc = mvccExtras();

            return mvcc != null && mvcc.isLocallyOwned(lockVer);
        }
        finally {
            unlockEntry();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean lockedByThread(long threadId, GridCacheVersion exclude)
        throws GridCacheEntryRemovedException {
        lockEntry();

        try {
            checkObsolete();

            GridCacheMvcc mvcc = mvccExtras();

            return mvcc != null && mvcc.isLocallyOwnedByThread(threadId, false, exclude);
        }
        finally {
            unlockEntry();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean lockedLocallyByIdOrThread(GridCacheVersion lockVer, long threadId)
        throws GridCacheEntryRemovedException {
        lockEntry();

        try {
            GridCacheMvcc mvcc = mvccExtras();

            return mvcc != null && mvcc.isLocallyOwnedByIdOrThread(lockVer, threadId);
        }
        finally {
            unlockEntry();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean lockedByThread(long threadId) throws GridCacheEntryRemovedException {
        lockEntry();

        try {
            checkObsolete();

            GridCacheMvcc mvcc = mvccExtras();

            return mvcc != null && mvcc.isLocallyOwnedByThread(threadId, true);
        }
        finally {
            unlockEntry();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean lockedBy(GridCacheVersion ver) throws GridCacheEntryRemovedException {
        lockEntry();

        try {
            checkObsolete();

            GridCacheMvcc mvcc = mvccExtras();

            return mvcc != null && mvcc.isOwnedBy(ver);
        }
        finally {
            unlockEntry();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean lockedByThreadUnsafe(long threadId) {
        lockEntry();

        try {
            GridCacheMvcc mvcc = mvccExtras();

            return mvcc != null && mvcc.isLocallyOwnedByThread(threadId, true);
        }
        finally {
            unlockEntry();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean lockedByUnsafe(GridCacheVersion ver) {
        lockEntry();

        try {
            GridCacheMvcc mvcc = mvccExtras();

            return mvcc != null && mvcc.isOwnedBy(ver);
        }
        finally {
            unlockEntry();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean lockedLocallyUnsafe(GridCacheVersion lockVer) {
        lockEntry();

        try {
            GridCacheMvcc mvcc = mvccExtras();

            return mvcc != null && mvcc.isLocallyOwned(lockVer);
        }
        finally {
            unlockEntry();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean hasLockCandidateUnsafe(GridCacheVersion ver) {
        lockEntry();

        try {
            GridCacheMvcc mvcc = mvccExtras();

            return mvcc != null && mvcc.hasCandidate(ver);
        }
        finally {
            unlockEntry();
        }
    }

    /** {@inheritDoc} */
    @Override public Collection<GridCacheMvccCandidate> localCandidates(GridCacheVersion... exclude)
        throws GridCacheEntryRemovedException {
        lockEntry();

        try {
            checkObsolete();

            GridCacheMvcc mvcc = mvccExtras();

            return mvcc == null ? Collections.<GridCacheMvccCandidate>emptyList() : mvcc.localCandidates(exclude);
        }
        finally {
            unlockEntry();
        }
    }

    /** {@inheritDoc} */
    @Override public Collection<GridCacheMvccCandidate> remoteMvccSnapshot(GridCacheVersion... exclude) {
        return Collections.emptyList();
    }

    /** {@inheritDoc} */
    @Nullable @Override public GridCacheMvccCandidate candidate(GridCacheVersion ver)
        throws GridCacheEntryRemovedException {
        lockEntry();

        try {
            checkObsolete();

            GridCacheMvcc mvcc = mvccExtras();

            return mvcc == null ? null : mvcc.candidate(ver);
        }
        finally {
            unlockEntry();
        }
    }

    /** {@inheritDoc} */
    @Override public GridCacheMvccCandidate localCandidate(long threadId)
        throws GridCacheEntryRemovedException {
        lockEntry();

        try {
            checkObsolete();

            GridCacheMvcc mvcc = mvccExtras();

            return mvcc == null ? null : mvcc.localCandidate(threadId);
        }
        finally {
            unlockEntry();
        }
    }

    /** {@inheritDoc} */
    @Override public GridCacheMvccCandidate candidate(UUID nodeId, long threadId)
        throws GridCacheEntryRemovedException {
        boolean loc = cctx.nodeId().equals(nodeId);

        lockEntry();

        try {
            checkObsolete();

            GridCacheMvcc mvcc = mvccExtras();

            return mvcc == null ? null : loc ? mvcc.localCandidate(threadId) :
                mvcc.remoteCandidate(nodeId, threadId);
        }
        finally {
            unlockEntry();
        }
    }

    /** {@inheritDoc} */
    @Override public GridCacheMvccCandidate localOwner() throws GridCacheEntryRemovedException {
        lockEntry();

        try {
            checkObsolete();

            GridCacheMvcc mvcc = mvccExtras();

            return mvcc == null ? null : mvcc.localOwner();
        }
        finally {
            unlockEntry();
        }
    }

    /** {@inheritDoc} */
    @Override public long rawExpireTime() {
        lockEntry();

        try {
            return expireTimeExtras();
        }
        finally {
            unlockEntry();
        }
    }

    /** {@inheritDoc} */
    @Override public long expireTimeUnlocked() {
        assert lock.isHeldByCurrentThread();

        return expireTimeExtras();
    }

    /** {@inheritDoc} */
    @Override public boolean onTtlExpired(long expireTime) throws GridCacheEntryRemovedException {
        boolean obsolete = false;

        assert !cctx.mvccEnabled();

        GridCacheVersion obsoleteVer = cctx.versions().startVersion();

        lockEntry();

        try {
            checkObsolete();

            if (isStartVersion())
                unswap(null, false);

            long expireTime0 = expireTimeExtras();

            if (!(expireTime0 > 0 && expireTime0 <= expireTime))
                return false;

            if (this.val != null) { // Do not trigger events for tombstones.
                if (cctx.events().isRecordable(EVT_CACHE_OBJECT_EXPIRED)) {
                    cctx.events().addEvent(partition(),
                        key,
                        cctx.localNodeId(),
                        null,
                        EVT_CACHE_OBJECT_EXPIRED,
                        null,
                        false,
                        this.val,
                        true,
                        null,
                        null,
                        null,
                        true);
                }

                cctx.continuousQueries().onEntryExpired(this, key, this.val);
            }

            if (markObsolete0(obsoleteVer, true, null))
                obsolete = true;

            removeExpiredValue(obsoleteVer);

            updatePlatformCache(null, null);
        }
        catch (NodeStoppingException e) {
            if (log.isDebugEnabled())
                log.warning("Node is stopping while removing expired value.", e);
        }
        catch (IgniteCheckedException e) {
            U.error(log, "Failed to clean up expired cache entry: " + this, e);
        }
        finally {
            unlockEntry();

            if (obsolete) {
                onMarkedObsolete();

                cctx.cache().removeEntry(this);

                if (cctx.statisticsEnabled())
                    cctx.cache().metrics0().onEvict();
            }
        }

        return true;
    }

    /**
     * @param expiredVal Expired value, will be null if a tombstone has expired.
     * @param obsoleteVer Version.
     * @return {@code True} if entry was marked as removed.
     * @throws IgniteCheckedException If failed.
     */
    private boolean onExpired(@Nullable CacheObject expiredVal, GridCacheVersion obsoleteVer) throws IgniteCheckedException {
        boolean rmvd = false;

        if (mvccExtras() != null || expiredVal == null) // Tombstone shouldn't expire here.
            return false;

        if (obsoleteVer == null)
            obsoleteVer = cctx.versions().startVersion();

        if (markObsolete0(obsoleteVer, true, null))
            rmvd = true;

        if (log.isTraceEnabled())
            log.trace("onExpired clear [key=" + key + ", entry=" + System.identityHashCode(this) + ']');

        if (cctx.mvccEnabled())
            cctx.offheap().mvccRemoveAll(this);
        else
            removeExpiredValue(obsoleteVer);

        if (cctx.events().isRecordable(EVT_CACHE_OBJECT_EXPIRED)) {
            cctx.events().addEvent(partition(),
                key,
                cctx.localNodeId(),
                null,
                EVT_CACHE_OBJECT_EXPIRED,
                null,
                false,
                expiredVal,
                expiredVal != null,
                null,
                null,
                null,
                true);
        }

        cctx.continuousQueries().onEntryExpired(this, key, expiredVal);

        updatePlatformCache(null, null);

        return rmvd;
    }

    /** {@inheritDoc} */
    @Override public long rawTtl() {
        lockEntry();

        try {
            return ttlExtras();
        }
        finally {
            unlockEntry();
        }
    }

    /** {@inheritDoc} */
    @Override public long expireTime() throws GridCacheEntryRemovedException {
        IgniteTxLocalAdapter tx = currentTx();

        if (tx != null) {
            long time = tx.entryExpireTime(txKey());

            if (time > 0)
                return time;
        }

        lockEntry();

        try {
            checkObsolete();

            return expireTimeExtras();
        }
        finally {
            unlockEntry();
        }
    }

    /** {@inheritDoc} */
    @Override public long ttl() throws GridCacheEntryRemovedException {
        IgniteTxLocalAdapter tx = currentTx();

        if (tx != null) {
            long entryTtl = tx.entryTtl(txKey());

            if (entryTtl > 0)
                return entryTtl;
        }

        lockEntry();

        try {
            checkObsolete();

            return ttlExtras();
        }
        finally {
            unlockEntry();
        }
    }

    /**
     * @return Current transaction.
     */
    private IgniteTxLocalAdapter currentTx() {
        return cctx.tm().localTx();
    }

    /** {@inheritDoc} */
    @Override public void updateTtl(@Nullable GridCacheVersion ver, long ttl) throws GridCacheEntryRemovedException {
        lockEntry();

        try {
            checkObsolete();

            if (hasValueUnlocked()) {
                try {
                    updateTtl(ttl, false);
                }
                catch (IgniteCheckedException e) {
                    U.error(log, "Failed to update TTL: " + e, e);
                }
            }

            /*
            TODO IGNITE-305.
            try {
                if (var == null || ver.equals(version()))
                    updateTtl(ttl);
            }
            catch (GridCacheEntryRemovedException ignored) {
                // No-op.
            }
            */
        }
        finally {
            unlockEntry();
        }
    }

    /** {@inheritDoc} */
    @Override public CacheObject updateTimeToLiveOnTtlUpdateRequest(@Nullable GridCacheVersion ver, long ttl) throws GridCacheEntryRemovedException {
        assert cctx.shared().database().checkpointLockIsHeldByThread() :
            "Checkpoint lock must be held by the thread before acquiring entry lock";

        if (!isFastUpdateTtlPossible()) {
            // Fast update is not possible for this entry. Need to fall back to writing the whole entry.
            //  - in general, enabling sql indices should not prevent from using fast update. Can be improved later.
            //  - mvcc and compression are not supported for fast update yet.
            //  - near entries only require update on the entry itself.
            try {
                unswap(null, true);
            }
            catch (IgniteCheckedException e) {
                U.error(log, "Failed to update TTL: " + e, e);

                return null;
            }

            updateTtl(ver, ttl);

            return val;
        }

        lockEntry();

        try {
            checkObsolete();

            CacheObject val = this.val;
            boolean isExpired = false;

            if (val == null) {
                CacheDataRow row = cctx.offheap().find(this, false);

                if (row != null && !row.tombstone()) {
                    val = row.value();
                    isExpired = row.expireTime() > 0 && row.expireTime() < U.currentTimeMillis();
                }
            }
            else
                isExpired = checkExpired();

            if (val == null || isExpired)
                return null;

            // TODO IGNITE-305
            // TTL messages are not ordered, so the version cannot be used to properly handle a new ttl value.
            updateTtl(ttl, true);

            return val;
        }
        catch (IgniteCheckedException e) {
            U.error(log, "Failed to update TTL: " + e, e);
        }
        finally {
            unlockEntry();
        }

        return null;
    }

    /** {@inheritDoc} */
    @Override public CacheObject touchTtl(
        @Nullable IgniteCacheExpiryPolicy expiryPlc,
        boolean updateMetrics
    ) throws GridCacheEntryRemovedException {
        CacheObject res = null;

        if (expiryPlc != null) {
            long ttl = expiryPlc.forAccess();

            if (ttl != CU.TTL_NOT_CHANGED) {
                res = updateTimeToLiveOnTtlUpdateRequest(null, ttl);

                expiryPlc.ttlUpdated(key(), version(), hasReaders() ? ((GridDhtCacheEntry)this).readers() : null);
            }
        }

        if (updateMetrics && cctx.statisticsEnabled())
            cctx.cache().metrics0().onCacheTouch(res != null);

        return res;
    }

    /** {@inheritDoc} */
    @Override public EntryGetResult touchTtlVersioned(@Nullable IgniteCacheExpiryPolicy expiryPlc) throws GridCacheEntryRemovedException {
        CacheObject v = touchTtl(expiryPlc, true);

        return v != null ? new EntryGetResult(v, version()) : null;
    }

    /** {@inheritDoc} */
    @Override public CacheObject valueBytes() throws GridCacheEntryRemovedException {
        lockEntry();

        try {
            checkObsolete();

            return this.val;
        }
        finally {
            unlockEntry();
        }
    }

    /** {@inheritDoc} */
    @Nullable @Override public CacheObject valueBytes(@Nullable GridCacheVersion ver)
        throws IgniteCheckedException, GridCacheEntryRemovedException {
        CacheObject val = null;

        lockEntry();

        try {
            checkObsolete();

            if (ver == null || this.ver.equals(ver))
                val = this.val;
        }
        finally {
            unlockEntry();
        }

        return val;
    }

    /**
     * Stores value in offheap.
     *
     * @param val Value.
     * @param expireTime Expire time.
     * @param ver New entry version.
     * @throws IgniteCheckedException If update failed.
     */
    protected void storeValue(
        @Nullable CacheObject val,
        long expireTime,
        GridCacheVersion ver
    ) throws IgniteCheckedException {
        storeValue(val, expireTime, ver, null);
    }

    /**
     * Stores value in offheap.
     *
     * @param val Value.
     * @param expireTime Expire time.
     * @param ver New entry version.
     * @throws IgniteCheckedException If update failed.
     */
    protected void storeTtlValue(
        @Nullable CacheObject val,
        long expireTime,
        GridCacheVersion ver
    ) throws IgniteCheckedException {
        storeValue(new UpdateTtlClosure(this, expireTime));
    }

    /**
     * Stores value in off-heap.
     *
     * @param val Value.
     * @param expireTime Expire time.
     * @param ver New entry version.
     * @param p Optional predicate.
     * @return {@code True} if storage was modified.
     * @throws IgniteCheckedException If update failed.
     */
    private UpdateClosure storeValue(
        @Nullable CacheObject val,
        long expireTime,
        GridCacheVersion ver,
        @Nullable IgniteBiPredicate<CacheObject, GridCacheVersion> p
    ) throws IgniteCheckedException {
        return storeValue(new UpdateClosure(this, val, ver, expireTime, p));
    }

    /**
     * Stores value in off-heap.
     *
     * @param clo Update closure.
     * @return Closure after execution.
     * @throws IgniteCheckedException If update failed.
     */
    private <T extends IgniteCacheOffheapManager.OffheapInvokeClosure> T storeValue(T clo) throws IgniteCheckedException {
        assert lock.isHeldByCurrentThread();
        assert localPartition() == null || localPartition().state() != RENTING : localPartition();

        cctx.offheap().invoke(cctx, key, localPartition(), clo);

        return clo;
    }

    /**
     * @param op Update operation.
     * @param val Write value.
     * @param writeVer Write version.
     * @param expireTime Expire time.
     * @param updCntr Update counter.
     * @param primary {@code True} if node is primary for entry in the moment of logging.
     */
    protected void logUpdate(
        GridCacheOperation op,
        CacheObject val,
        GridCacheVersion writeVer,
        long expireTime,
        long updCntr,
        boolean primary
    ) throws IgniteCheckedException {
        // We log individual updates only in ATOMIC cache.
        assert cctx.atomic();

        try {
            if (cctx.group().persistenceEnabled() && cctx.group().walEnabled())
                cctx.shared().wal().log(new DataRecord(new DataEntry(
                    cctx.cacheId(),
                    key,
                    val,
                    op,
                    null,
                    writeVer,
                    expireTime,
                    partition(),
                    updCntr,
                    DataEntry.flags(primary))));
        }
        catch (StorageException e) {
            throw new IgniteCheckedException("Failed to log ATOMIC cache update [key=" + key + ", op=" + op +
                ", val=" + val + ']', e);
        }
    }

    /**
     * Appends a new out-of-order update record to the write-ahead log if persistence enabled.
     * This record keeps track the out-of-order updates in the ATOMIC cache.
     *
     * @param op Update operation.
     * @param val Write value.
     * @param writeVer Write version.
     * @param expireTime Expire time.
     * @param updCntr Update counter.
     */
    protected void logOutOfOrderUpdate(
        GridCacheOperation op,
        CacheObject val,
        GridCacheVersion writeVer,
        long expireTime,
        Long updCntr
    ) throws IgniteCheckedException {
        assert cctx.atomic() : "Individual updates must be logged only for ATOMIC caches.";
        assert updCntr != null : "Out-of-order update must provide an update counter [entry=" + this + ']';

        try {
            if (cctx.group().persistenceEnabled() && cctx.group().walEnabled())
                cctx.shared().wal().log(new OutOfOrderDataRecord(new DataEntry(
                    cctx.cacheId(),
                    key,
                    val,
                    op,
                    null,
                    writeVer,
                    expireTime,
                    partition(),
                    updCntr,
                    DataEntry.flags(false)))); // out of order update can happen only on a backup node
        }
        catch (StorageException e) {
            throw new IgniteCheckedException("Failed to log ATOMIC cache out-of-order update [key=" + key +
                ", updCntr=" + updCntr + ']', e);
        }
    }

    /**
     * @param tx Transaction.
     * @param val Value.
     * @param expireTime Expire time (or 0 if not applicable).
     * @param writeVersion Tx write version.
     * @param walEnabled Whether WAL is enabled.
     * @throws IgniteCheckedException In case of log failure.
     */
    protected WALPointer logTxUpdate(
        IgniteInternalTx tx,
        CacheObject val,
        long expireTime,
        GridCacheVersion writeVersion,
        boolean walEnabled
    ) throws IgniteCheckedException {
        assert cctx.transactional() && !cctx.transactionalSnapshot();

        if (tx.local()) { // For remote tx we log all updates in batch: GridDistributedTxRemoteAdapter.commitIfLocked()
            cctx.tm().pendingTxsTracker().onKeysWritten(tx.nearXidVersion(), Collections.singletonList(key));

            if (walEnabled) {
                GridCacheOperation op;
                if (val == null)
                    op = DELETE;
                else
                    op = this.val == null ? GridCacheOperation.CREATE : UPDATE;

                return cctx.shared().wal().log(new DataRecord(new DataEntry(
                    cctx.cacheId(),
                    key,
                    val,
                    op,
                    tx.nearXidVersion(),
                    writeVersion,
                    expireTime,
                    key.partition(),
                    writeVersion.updateCounter(),
                    DataEntry.flags(CU.txOnPrimary(tx)))));
            }
            else
                return null;
        }
        else
            return null;
    }

    /**
     * @param tx Transaction.
     * @param val Value.
     * @param expireTime Expire time (or 0 if not applicable).     *
     * @param updCntr Update counter.
     * @param mvccVer Mvcc version.
     * @throws IgniteCheckedException In case of log failure.
     */
    protected WALPointer logMvccUpdate(IgniteInternalTx tx, CacheObject val, long expireTime, long updCntr,
        MvccSnapshot mvccVer)
        throws IgniteCheckedException {
        assert mvccVer != null;
        assert cctx.transactionalSnapshot();

        if (tx.local()) { // For remote tx we log all updates in batch: GridDistributedTxRemoteAdapter.commitIfLocked()
            GridCacheOperation op;
            if (val == null)
                op = DELETE;
            else
                op = this.val == null ? GridCacheOperation.CREATE : UPDATE;

            return cctx.shared().wal().log(new MvccDataRecord(new MvccDataEntry(
                cctx.cacheId(),
                key,
                val,
                op,
                tx.nearXidVersion(),
                tx.writeVersion(),
                expireTime,
                key.partition(),
                updCntr,
                mvccVer)));
        }
        else
            return null;
    }

    /**
     * Removes value from offheap. Can produce a tombstone depending on cache type.
     *
     * @param clearVer Clear version.
     * @throws IgniteCheckedException If failed.
     */
    protected void removeValue(GridCacheVersion clearVer) throws IgniteCheckedException {
        assert lock.isHeldByCurrentThread();

        cctx.offheap().remove(cctx, key, partition(), localPartition());
    }

    /**
     * Removes expired value or tombstone.
     *
     * @param clearVer Clear version
     * @throws IgniteCheckedException If failed.
     */
    protected void removeExpiredValue(GridCacheVersion clearVer) throws IgniteCheckedException {
        assert lock.isHeldByCurrentThread();

        cctx.offheap().remove(cctx, key, partition(), localPartition());
    }

    /** {@inheritDoc} */
    @Override public <K, V> Cache.Entry<K, V> wrap() {
        try {
            IgniteInternalTx tx = cctx.tm().userTx();

            CacheObject val;

            if (tx != null) {
                GridTuple<CacheObject> peek = tx.peek(cctx, false, key);

                val = peek == null ? rawGet() : peek.get();
            }
            else
                val = rawGet();

            return new CacheEntryImpl<>(key.<K>value(cctx.cacheObjectContext(), false),
                CU.<V>value(val, cctx, false), ver);
        }
        catch (GridCacheFilterFailedException ignored) {
            throw new IgniteException("Should never happen.");
        }
    }

    /** {@inheritDoc} */
    @Override public <K, V> Cache.Entry<K, V> wrapLazyValue(boolean keepBinary) {
        return new LazyValueEntry<>(key, keepBinary);
    }

    /** {@inheritDoc} */
    @Override @Nullable public CacheObject peekVisibleValue() {
        try {
            IgniteInternalTx tx = cctx.tm().userTx();

            if (tx != null) {
                GridTuple<CacheObject> peek = tx.peek(cctx, false, key);

                if (peek != null)
                    return peek.get();
            }

            if (detached())
                return rawGet();

            for (; ; ) {
                GridCacheEntryEx e = cctx.cache().peekEx(key);

                if (e == null)
                    return null;

                try {
                    return e.peek();
                }
                catch (GridCacheEntryRemovedException ignored) {
                    // No-op.
                }
                catch (IgniteCheckedException ex) {
                    throw new IgniteException(ex);
                }
            }
        }
        catch (GridCacheFilterFailedException ignored) {
            throw new IgniteException("Should never happen.");
        }
    }

    /** {@inheritDoc} */
    @Override public void updateIndex(
        SchemaIndexCacheVisitorClosure clo
    ) throws IgniteCheckedException, GridCacheEntryRemovedException {
        lockEntry();

        try {
            if (isInternal())
                return;

            checkObsolete();

            CacheDataRow row = cctx.offheap().read(this);

            if (row == null || row.tombstone())
                return;

            clo.apply(row);
        }
        finally {
            unlockEntry();
        }
    }

    /** {@inheritDoc} */
    @Override public <K, V> EvictableEntry<K, V> wrapEviction() {
        return new CacheEvictableEntryImpl<>(this);
    }

    /** {@inheritDoc} */
    @Override public <K, V> CacheEntryImplEx<K, V> wrapVersioned() {
        lockEntry();

        try {
            return new CacheEntryImplEx<>(key.<K>value(cctx.cacheObjectContext(), false), null, ver);
        }
        finally {
            unlockEntry();
        }
    }

    /**
     * Evicts necessary number of data pages if per-page eviction is configured in current {@link DataRegion}.
     */
    private void ensureFreeSpace() throws IgniteCheckedException {
        // Deadlock alert: evicting data page causes removing (and locking) all entries on the page one by one.
        assert !lock.isHeldByCurrentThread();

        cctx.shared().database().ensureFreeSpace(cctx.dataRegion());
    }

    /**
     * @return Entry which holds key, value and version.
     */
    private <K, V> CacheEntryImplEx<K, V> wrapVersionedWithValue() {
        lockEntry();

        try {
            V val = this.val == null ? null : this.val.<V>value(cctx.cacheObjectContext(), false);

            return new CacheEntryImplEx<>(key.<K>value(cctx.cacheObjectContext(), false), val, ver);
        }
        finally {
            unlockEntry();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean evictInternal(
        GridCacheVersion obsoleteVer,
        @Nullable CacheEntryPredicate[] filter,
        boolean evictOffheap)
        throws IgniteCheckedException {

        boolean marked = false;

        try {
            if (F.isEmptyOrNulls(filter)) {
                lockEntry();

                try {
                    if (evictionDisabled()) {
                        assert !obsolete();

                        return false;
                    }

                    if (obsoleteVersionExtras() != null)
                        return true;

                    if (!hasReaders() && markObsolete0(obsoleteVer, false, null)) {
                        // Nullify value after swap.
                        value(null);

                        if (evictOffheap)
                            removeExpiredValue(obsoleteVer);

                        marked = true;

                        return true;
                    }
                }
                finally {
                    unlockEntry();
                }
            }
            else {
                // For optimistic check.
                while (true) {
                    GridCacheVersion v;

                    lockEntry();

                    try {
                        v = ver;
                    }
                    finally {
                        unlockEntry();
                    }

                    if (!cctx.isAll(/*version needed for sync evicts*/this, filter))
                        return false;

                    lockEntry();

                    try {
                        if (evictionDisabled()) {
                            assert !obsolete();

                            return false;
                        }

                        if (obsoleteVersionExtras() != null)
                            return true;

                        if (!v.equals(ver))
                            // Version has changed since entry passed the filter. Do it again.
                            continue;

                        if (!hasReaders() && markObsolete0(obsoleteVer, false, null)) {
                            // Nullify value after swap.
                            value(null);

                            if (evictOffheap)
                                removeExpiredValue(obsoleteVer);

                            marked = true;

                            return true;
                        }
                        else
                            return false;
                    }
                    finally {
                        unlockEntry();
                    }
                }
            }
        }
        catch (GridCacheEntryRemovedException ignore) {
            if (log.isDebugEnabled())
                log.debug("Got removed entry when evicting (will simply return): " + this);

            return true;
        }
        finally {
            if (marked)
                onMarkedObsolete();
        }

        return false;
    }

    /**
     * @param filter Entry filter.
     * @return {@code True} if entry is visitable.
     */
    public final boolean visitable(CacheEntryPredicate[] filter) {
        boolean rmv = false;

        try {
            lockEntry();

            try {
                if (obsoleteOrDeleted())
                    return false;

                if (checkExpired()) {
                    rmv = onExpired(val, null);

                    return false;
                }
            }
            finally {
                unlockEntry();
            }

            if (filter != CU.empty0() && !cctx.isAll(this, filter))
                return false;
        }
        catch (IgniteCheckedException e) {
            U.error(log, "An exception was thrown while filter checking.", e);

            RuntimeException ex = e.getCause(RuntimeException.class);

            if (ex != null)
                throw ex;

            Error err = e.getCause(Error.class);

            if (err != null)
                throw err;

            return false;
        }
        finally {
            if (rmv) {
                onMarkedObsolete();

                cctx.cache().removeEntry(this);
            }
        }

        IgniteInternalTx tx = cctx.tm().localTx();

        if (tx != null) {
            IgniteTxEntry e = tx.entry(txKey());

            boolean rmvd = e != null && e.op() == DELETE;

            return !rmvd;
        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public final boolean deleted() {
        lockEntry();

        try {
            return deletedUnlocked();
        }
        finally {
            unlockEntry();
        }
    }

    /** {@inheritDoc} */
    @Override public final boolean obsoleteOrDeleted() {
        lockEntry();

        try {
            return obsoleteVersionExtras() != null;
        }
        finally {
            unlockEntry();
        }
    }

    /**
     * @return {@code True} if deleted.
     */
    @SuppressWarnings("SimplifiableIfStatement")
    protected final boolean deletedUnlocked() {
        assert lock.isHeldByCurrentThread();

        return !isStartVersion() && !hasValueUnlocked();
    }

    /**
     * Increments public size of map.
     */
    protected void incrementMapPublicSize() {
        GridDhtLocalPartition locPart = localPartition();

        if (locPart != null)
            locPart.incrementPublicSize(null, this);
        else
            cctx.incrementPublicSize(this);
    }

    /**
     * Decrements public size of map.
     */
    protected void decrementMapPublicSize() {
        GridDhtLocalPartition locPart = localPartition();

        if (locPart != null)
            locPart.decrementPublicSize(null, this);
        else
            cctx.decrementPublicSize(this);
    }

    /**
     * @return MVCC.
     */
    @Nullable protected final GridCacheMvcc mvccExtras() {
        return extras != null ? extras.mvcc() : null;
    }

    /**
     * @return All MVCC local and non near candidates.
     */
    @SuppressWarnings("ForLoopReplaceableByForEach")
    @Nullable public final List<GridCacheMvccCandidate> mvccAllLocal() {
        lockEntry();

        try {
            GridCacheMvcc mvcc = extras != null ? extras.mvcc() : null;

            if (mvcc == null)
                return null;

            List<GridCacheMvccCandidate> allLocs = mvcc.allLocal();

            if (allLocs == null || allLocs.isEmpty())
                return null;

            List<GridCacheMvccCandidate> locs = new ArrayList<>(allLocs.size());

            for (int i = 0; i < allLocs.size(); i++) {
                GridCacheMvccCandidate loc = allLocs.get(i);

                if (!loc.nearLocal())
                    locs.add(loc);
            }

            return locs.isEmpty() ? null : locs;
        }
        finally {
            unlockEntry();
        }
    }

    /**
     * @param mvcc MVCC.
     */
    protected final void mvccExtras(@Nullable GridCacheMvcc mvcc) {
        extras = (extras != null) ? extras.mvcc(mvcc) : mvcc != null ? new GridCacheMvccEntryExtras(mvcc) : null;
    }

    /**
     * @return Obsolete version.
     */
    @Nullable protected final GridCacheVersion obsoleteVersionExtras() {
        return extras != null ? extras.obsoleteVersion() : null;
    }

    /**
     * @param obsoleteVer Obsolete version.
     * @param ext Extras.
     */
    private void obsoleteVersionExtras(@Nullable GridCacheVersion obsoleteVer, GridCacheObsoleteEntryExtras ext) {
        extras = (extras != null) ?
            extras.obsoleteVersion(obsoleteVer) :
            obsoleteVer != null ?
                (ext != null) ? ext : new GridCacheObsoleteEntryExtras(obsoleteVer) :
                null;
    }

    /**
     * @param prevOwners Previous owners.
     * @param owners Current owners.
     * @param val Entry value.
     */
    protected final void checkOwnerChanged(
        @Nullable CacheLockCandidates prevOwners,
        @Nullable CacheLockCandidates owners,
        CacheObject val
    ) {
        checkOwnerChanged(prevOwners, owners, val, false);
    }

    /**
     * @param prevOwners Previous owners.
     * @param owners Current owners.
     * @param val Entry value.
     * @param inThreadChain {@code True} if called during thread chain checking.
     */
    protected final void checkOwnerChanged(
        @Nullable CacheLockCandidates prevOwners,
        @Nullable CacheLockCandidates owners,
        CacheObject val,
        boolean inThreadChain
    ) {
        assert !lock.isHeldByCurrentThread();

        if (prevOwners != null && owners == null) {
            cctx.mvcc().callback().onOwnerChanged(this, null);

            if (cctx.events().isRecordable(EVT_CACHE_OBJECT_UNLOCKED)) {
                boolean hasVal = hasValue();

                GridCacheMvccCandidate cand = prevOwners.candidate(0);

                cctx.events().addEvent(partition(),
                    key,
                    cand.nodeId(),
                    cand,
                    EVT_CACHE_OBJECT_UNLOCKED,
                    val,
                    hasVal,
                    val,
                    hasVal,
                    null,
                    null,
                    null,
                    true);
            }
        }

        if (owners != null) {
            for (int i = 0; i < owners.size(); i++) {
                GridCacheMvccCandidate owner = owners.candidate(i);

                boolean locked = prevOwners == null || !prevOwners.hasCandidate(owner.version());

                if (locked) {
                    cctx.mvcc().callback().onOwnerChanged(this, owner);

                    if (owner.local() && !inThreadChain)
                        checkThreadChain(owner);

                    if (cctx.events().isRecordable(EVT_CACHE_OBJECT_LOCKED)) {
                        boolean hasVal = hasValue();

                        // Event notification.
                        cctx.events().addEvent(partition(),
                            key,
                            owner.nodeId(),
                            owner,
                            EVT_CACHE_OBJECT_LOCKED,
                            val,
                            hasVal,
                            val,
                            hasVal,
                            null,
                            null,
                            null,
                            true);
                    }
                }
            }
        }
    }

    /**
     * @param owner Starting candidate in the chain.
     */
    protected abstract void checkThreadChain(GridCacheMvccCandidate owner);

    /**
     * Updates metrics.
     *
     * @param op Operation.
     * @param metrics Update merics flag.
     * @param transformed {@code True} if transform operation caused update.
     * @param hasOldVal {@code True} if entry has old value.
     */
    private void updateMetrics(GridCacheOperation op, boolean metrics, boolean transformed, boolean hasOldVal) {
        if (metrics && cctx.statisticsEnabled()) {
            if (op == DELETE) {
                cctx.cache().metrics0().onRemove();

                if (transformed)
                    cctx.cache().metrics0().onInvokeRemove(hasOldVal);
            }
            else if (op == READ && transformed)
                cctx.cache().metrics0().onReadOnlyInvoke(hasOldVal);
            else {
                cctx.cache().metrics0().onWrite();

                if (transformed)
                    cctx.cache().metrics0().onInvokeUpdate(hasOldVal);
            }
        }
    }

    /**
     * @return TTL.
     */
    public long ttlExtras() {
        return extras != null ? extras.ttl() : 0;
    }

    /**
     * @return Expire time.
     */
    public long expireTimeExtras() {
        return extras != null ? extras.expireTime() : 0L;
    }

    /**
     * @param ttl TTL.
     * @param expireTime Expire time.
     */
    protected void ttlAndExpireTimeExtras(long ttl, long expireTime) {
        assert ttl != CU.TTL_NOT_CHANGED && ttl != CU.TTL_ZERO;

        extras = (extras != null) ? extras.ttlAndExpireTime(ttl, expireTime) : expireTime != CU.EXPIRE_TIME_ETERNAL ?
            new GridCacheTtlEntryExtras(ttl, expireTime) : null;
    }

    /**
     * @return Size of extras object.
     */
    private int extrasSize() {
        return extras != null ? extras.size() : 0;
    }

    /** {@inheritDoc} */
    @Override public void txUnlock(IgniteInternalTx tx) throws GridCacheEntryRemovedException {
        removeLock(tx.xidVersion());
    }

    /** {@inheritDoc} */
    @Override public void lockEntry() {
        lock.lock();
    }

    /** {@inheritDoc} */
    @Override public boolean tryLockEntry(long timeout) {
        try {
            return lock.tryLock(timeout, TimeUnit.MILLISECONDS);
        }
        catch (InterruptedException ignite) {
            Thread.currentThread().interrupt();

            return false;
        }
    }

    /** {@inheritDoc} */
    @Override public void unlockEntry() {
        lock.unlock();
    }

    /**
     * This method would obtain read lock for continuous query listener setup. This
     * is to prevent race condition between entry update and continuous query setup.
     * You should make sure you obtain this read lock first before locking the entry
     * in order to ensure that the entry update is completed and existing continuous
     * query notified before the next cache listener update
     */
    private void lockListenerReadLock() {
        listenerLock.readLock().lock();
    }

    /**
     * unlock the listener read lock
     *
     * @see #lockListenerReadLock()
     */
    private void unlockListenerReadLock() {
        listenerLock.readLock().unlock();
    }

    /** {@inheritDoc} */
    @Override public boolean lockedByCurrentThread() {
        return lock.isHeldByCurrentThread();
    }

    /** {@inheritDoc} */
    @Override public void touch() {
        context().evicts().touch(this);
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        // Identity comparison left on purpose.
        return o == this;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return hash;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return toStringWithTryLock(() -> S.toString(GridCacheMapEntry.class, this));
    }

    /**
     * Does thread safe {@link #toString} for {@link GridCacheMapEntry} classes.
     *
     * @param dfltToStr {@link #toString()} supplier.
     * @return Result of dfltToStr call If lock acquired or a short representation of {@link GridCacheMapEntry}.
     */
    protected String toStringWithTryLock(Supplier<String> dfltToStr) {
        if (tryLockEntry(ENTRY_LOCK_TIMEOUT)) {
            try {
                return dfltToStr.get();
            }
            finally {
                unlockEntry();
            }
        }
        else {
            GridToStringBuilder.SensitiveDataLogging sensitiveDataLogging = S.getSensitiveDataLogging();

            String keySens;

            if (sensitiveDataLogging == PLAIN)
                keySens = ", key=" + key;
            else if (sensitiveDataLogging == HASH)
                keySens = ", key=" + (key == null ? "null" : String.valueOf(IgniteUtils.hash(key)));
            else
                keySens = "";

            return "GridCacheMapEntry [err='Partial result represented because entry lock wasn't acquired."
                + " Waiting time elapsed.'"
                + keySens
                + ", hash=" + hash
                + "]";
        }
    }

    /** */
    private static class MvccRemoveLockListener implements IgniteInClosure<IgniteInternalFuture> {
        /** */
        private static final long serialVersionUID = -1578749008606139541L;

        /** */
        private final IgniteInternalTx tx;

        /** */
        private final AffinityTopologyVersion topVer;

        /** */
        private final UUID affNodeId;

        /** */
        private final MvccSnapshot mvccVer;

        /** */
        private final boolean needHistory;

        /** */
        private final GridFutureAdapter<GridCacheUpdateTxResult> resFut;

        /** Need previous value flag. */
        private final boolean needVal;

        /** Filter. */
        private final CacheEntryPredicate filter;

        /** */
        private GridCacheMapEntry entry;

        /** */
        private IgniteUuid futId;

        /** */
        private int batchNum;

        /** Flag if it is need to return the old value (value before current tx has been started). */
        private final boolean needOldVal;

        /**
         *
         * @param tx Transaction.
         * @param entry Entry.
         * @param affNodeId Node id.
         * @param topVer Topology version.
         * @param mvccVer Mvcc version.
         * @param needHistory Need history flag.
         * @param resFut Result future.
         * @param needOldVal Flag if it is need to return the old value (value before current tx has been started).
         */
        MvccRemoveLockListener(IgniteInternalTx tx,
            GridCacheMapEntry entry,
            UUID affNodeId,
            AffinityTopologyVersion topVer,
            MvccSnapshot mvccVer,
            boolean needHistory,
            GridFutureAdapter<GridCacheUpdateTxResult> resFut,
            boolean needOldVal,
            boolean retVal,
            @Nullable CacheEntryPredicate filter) {
            this.tx = tx;
            this.entry = entry;
            this.topVer = topVer;
            this.affNodeId = affNodeId;
            this.mvccVer = mvccVer;
            this.needHistory = needHistory;
            this.resFut = resFut;
            this.needOldVal = needOldVal;
            this.needVal = retVal;
            this.filter = filter;
        }

        /** {@inheritDoc} */
        @Override public void apply(IgniteInternalFuture lockFut) {
            WALPointer logPtr = null;
            boolean valid;

            GridCacheContext cctx = entry.context();
            GridCacheVersion newVer = tx.writeVersion();

            MvccUpdateResult res;

            try {
                lockFut.get();

                while (true) {
                    entry.lockEntry();

                    if (entry.obsoleteVersionExtras() == null)
                        break;

                    entry.unlockEntry();

                    entry = (GridCacheMapEntry)cctx.cache().entryEx(entry.key());
                }

                valid = entry.valid(tx.topologyVersion());

                boolean needOldVal = tx.txState().useMvccCaching(cctx.cacheId());

                cctx.shared().database().checkpointReadLock();

                try {
                    res = cctx.offheap().mvccRemove(entry, mvccVer, tx.local(), needHistory, needOldVal, filter, needVal);
                } finally {
                    cctx.shared().database().checkpointReadUnlock();
                }

                assert res != null;

                if (res.resultType() == ResultType.VERSION_MISMATCH) {
                    resFut.onDone(serializationError());

                    return;
                }
                else if (res.resultType() == ResultType.PREV_NULL) {
                    resFut.onDone(new GridCacheUpdateTxResult(false));

                    return;
                }
                else if (res.resultType() == ResultType.FILTERED) {
                    GridCacheUpdateTxResult updRes = new GridCacheUpdateTxResult(false);

                    updRes.filtered(true);

                    resFut.onDone(updRes);

                    return;
                }
                else if (res.resultType() == ResultType.LOCKED) {
                    entry.unlockEntry();

                    IgniteInternalFuture<?> lockFuture =
                        cctx.kernalContext().coordinators().waitForLock(cctx, mvccVer, res.resultVersion());

                    lockFuture.listen(this);

                    return;
                }

                if (res.resultType() == ResultType.PREV_NOT_NULL) {
                    TxCounters counters = tx.txCounters(true);

                    if (compareIgnoreOpCounter(res.resultVersion(), mvccVer) == 0) {
                        if (res.isKeyAbsentBefore()) // Do not count own update removal.
                            counters.decrementUpdateCounter(cctx.cacheId(), entry.partition());
                    }
                    else
                        counters.incrementUpdateCounter(cctx.cacheId(), entry.partition());

                    counters.accumulateSizeDelta(cctx.cacheId(), entry.partition(), -1);
                }

                if (cctx.group().persistenceEnabled() && cctx.group().walEnabled())
                    entry.logMvccUpdate(tx, null, 0, 0, mvccVer);

                entry.update(null, 0, 0, newVer, true);

                entry.recordNodeId(affNodeId, topVer);
            }
            catch (IgniteCheckedException e) {
                resFut.onDone(e);

                return;
            }
            finally {
                if (entry.lockedByCurrentThread()) {
                    entry.unlockEntry();

                    cctx.evicts().touch(entry);
                }
            }

            entry.onUpdateFinished(0L);

            GridCacheUpdateTxResult updRes = valid ? new GridCacheUpdateTxResult(true, 0L, logPtr)
                : new GridCacheUpdateTxResult(false, logPtr);

            updRes.mvccHistory(res.history());

            if (needOldVal && compareIgnoreOpCounter(res.resultVersion(), mvccVer) != 0 &&
                (res.resultType() == ResultType.PREV_NOT_NULL || res.resultType() == ResultType.REMOVED_NOT_NULL))
                updRes.oldValue(res.oldValue());

            resFut.onDone(updRes);
        }
    }

    /** */
    private static class MvccAcquireLockListener implements IgniteInClosure<IgniteInternalFuture> {
        /** */
        private static final long serialVersionUID = -1578749008606139541L;

        /** */
        private final IgniteInternalTx tx;

        /** */
        private final MvccSnapshot mvccVer;

        /** */
        private final GridFutureAdapter<GridCacheUpdateTxResult> resFut;

        /** */
        private GridCacheMapEntry entry;

        /** */
        MvccAcquireLockListener(IgniteInternalTx tx,
            GridCacheMapEntry entry,
            MvccSnapshot mvccVer,
            GridFutureAdapter<GridCacheUpdateTxResult> resFut) {
            this.tx = tx;
            this.entry = entry;
            this.mvccVer = mvccVer;
            this.resFut = resFut;
        }

        /** {@inheritDoc} */
        @Override public void apply(IgniteInternalFuture lockFut) {
            WALPointer logPtr = null;
            boolean valid;

            GridCacheContext cctx = entry.context();

            try {
                lockFut.get();

                while (true) {
                    entry.lockEntry();

                    if (entry.obsoleteVersionExtras() == null)
                        break;

                    entry.unlockEntry();

                    entry = (GridCacheMapEntry)cctx.cache().entryEx(entry.key());
                }

                valid = entry.valid(tx.topologyVersion());

                cctx.shared().database().checkpointReadLock();

                MvccUpdateResult res;

                try {
                    res = cctx.offheap().mvccLock(entry, mvccVer);
                }
                finally {
                    cctx.shared().database().checkpointReadUnlock();
                }

                assert res != null;

                if (res.resultType() == ResultType.VERSION_MISMATCH) {
                    resFut.onDone(serializationError());

                    return;
                }
                else if (res.resultType() == ResultType.LOCKED) {
                    entry.unlockEntry();

                    cctx.kernalContext().coordinators().waitForLock(cctx, mvccVer, res.resultVersion()).listen(this);

                    return;
                }
            }
            catch (IgniteCheckedException e) {
                resFut.onDone(e);

                return;
            }
            finally {
                if (entry.lockedByCurrentThread()) {
                    entry.unlockEntry();

                    cctx.evicts().touch(entry);
                }
            }

            entry.onUpdateFinished(0L);

            resFut.onDone(new GridCacheUpdateTxResult(valid, logPtr));
        }
    }

    /** */
    private static class MvccUpdateLockListener implements IgniteInClosure<IgniteInternalFuture> {
        /** */
        private static final long serialVersionUID = 8452738214760268397L;

        /** */
        private final IgniteInternalTx tx;

        /** */
        private final UUID affNodeId;

        /** */
        private final AffinityTopologyVersion topVer;

        /** */
        private final CacheObject val;

        /** */
        private final long ttl;

        /** */
        private final MvccSnapshot mvccVer;

        /** */
        private final GridFutureAdapter<GridCacheUpdateTxResult> resFut;

        /** */
        private GridCacheMapEntry entry;

        /** */
        private GridCacheOperation op;

        /** */
        private final boolean keepBinary;

        /** Entry processor. */
        private final EntryProcessor entryProc;

        /** Invoke arguments. */
        private final Object[] invokeArgs;

        /** Filter. */
        private final CacheEntryPredicate filter;

        /** */
        private final boolean needHistory;

        /** */
        private final boolean noCreate;

        /** Need previous value flag.*/
        private final boolean needVal;

        /** Flag if it is need to return the old value (value before current tx has been started). */
        private boolean needOldVal;

        /** */
        MvccUpdateLockListener(IgniteInternalTx tx,
            GridCacheMapEntry entry,
            UUID affNodeId,
            AffinityTopologyVersion topVer,
            CacheObject val,
            long ttl,
            MvccSnapshot mvccVer,
            GridCacheOperation op,
            boolean needHistory,
            boolean noCreate,
            GridFutureAdapter<GridCacheUpdateTxResult> resFut,
            boolean needOldVal,
            CacheEntryPredicate filter,
            boolean needVal,
            boolean keepBinary,
            EntryProcessor entryProc,
            Object[] invokeArgs) {
            this.tx = tx;
            this.entry = entry;
            this.affNodeId = affNodeId;
            this.topVer = topVer;
            this.val = val;
            this.ttl = ttl;
            this.mvccVer = mvccVer;
            this.op = op;
            this.needHistory = needHistory;
            this.noCreate = noCreate;
            this.filter = filter;
            this.needVal = needVal;
            this.resFut = resFut;
            this.needOldVal = needOldVal;
            this.keepBinary = keepBinary;
            this.entryProc = entryProc;
            this.invokeArgs = invokeArgs;
        }

        /** {@inheritDoc} */
        @Override public void apply(IgniteInternalFuture lockFut) {
            WALPointer logPtr = null;
            boolean valid;

            GridCacheContext cctx = entry.context();
            GridCacheVersion newVer = tx.writeVersion();

            final boolean invoke = entryProc != null;

            MvccUpdateResult res;

            try {
                lockFut.get();

                entry.ensureFreeSpace();

                while (true) {
                    entry.lockEntry();

                    if (entry.obsoleteVersionExtras() == null)
                        break;

                    entry.unlockEntry();

                    entry = (GridCacheMapEntry)cctx.cache().entryEx(entry.key());
                }

                valid = entry.valid(tx.topologyVersion());

                // Determine new ttl and expire time.
                long expireTime, ttl = this.ttl;

                if (ttl == -1L) {
                    ttl = entry.ttlExtras();
                    expireTime = entry.expireTimeExtras();
                }
                else
                    expireTime = CU.toExpireTime(ttl);

                assert ttl >= 0 : ttl;
                assert expireTime >= 0 : expireTime;

                cctx.shared().database().checkpointReadLock();

                try {
                    res = cctx.offheap().mvccUpdate(entry, val, newVer, expireTime, mvccVer, tx.local(), needHistory,
                        noCreate, needOldVal, filter, needVal, keepBinary, entryProc, invokeArgs);
                } finally {
                    cctx.shared().database().checkpointReadUnlock();
                }

                assert res != null;

                if (res.resultType() == ResultType.VERSION_MISMATCH) {
                    resFut.onDone(serializationError());

                    return;
                }
                else if (res.resultType() == ResultType.LOCKED) {
                    entry.unlockEntry();

                    cctx.kernalContext().coordinators().waitForLock(cctx, mvccVer, res.resultVersion()).listen(this);

                    return;
                }
                else if (res.resultType() == ResultType.FILTERED) {
                    GridCacheUpdateTxResult updRes = new GridCacheUpdateTxResult(invoke);

                    if (invoke) { // No-op invoke happened.
                        assert res.invokeResult() != null;

                        updRes.invokeResult(res.invokeResult());
                    }

                    updRes.filtered(true);

                    if (needVal)
                        updRes.prevValue(res.oldValue());

                    resFut.onDone(updRes);

                    return;
                }
                else if (op == CREATE && tx.local() && (res.resultType() == ResultType.PREV_NOT_NULL ||
                    res.resultType() == ResultType.VERSION_FOUND)) {
                    resFut.onDone(new IgniteTxDuplicateKeyCheckedException(entry.key()));

                    return;
                }

                if (res.resultType() == ResultType.PREV_NULL) {
                    TxCounters counters = tx.txCounters(true);

                    if (compareIgnoreOpCounter(res.resultVersion(), mvccVer) == 0) {
                        if (res.isKeyAbsentBefore())
                            counters.incrementUpdateCounter(cctx.cacheId(), entry.partition());
                    }
                    else
                        counters.incrementUpdateCounter(cctx.cacheId(), entry.partition());

                    counters.accumulateSizeDelta(cctx.cacheId(), entry.partition(), 1);
                }
                else if (res.resultType() == ResultType.PREV_NOT_NULL && compareIgnoreOpCounter(res.resultVersion(), mvccVer) != 0) {
                    TxCounters counters = tx.txCounters(true);

                    counters.incrementUpdateCounter(cctx.cacheId(), entry.partition());
                }
                else if (res.resultType() == ResultType.REMOVED_NOT_NULL) {
                    TxCounters counters = tx.txCounters(true);

                    if (compareIgnoreOpCounter(res.resultVersion(), mvccVer) == 0) {
                        if (res.isKeyAbsentBefore()) // Do not count own update removal.
                            counters.decrementUpdateCounter(cctx.cacheId(), entry.partition());
                    }
                    else
                        counters.incrementUpdateCounter(cctx.cacheId(), entry.partition());

                    counters.accumulateSizeDelta(cctx.cacheId(), entry.partition(), -1);
                }

                if (cctx.group().persistenceEnabled() && cctx.group().walEnabled())
                    logPtr = cctx.shared().wal().log(new MvccDataRecord(new MvccDataEntry(
                        cctx.cacheId(),
                        entry.key(),
                        val,
                        res.resultType() == ResultType.PREV_NULL ? CREATE :
                            (res.resultType() == ResultType.REMOVED_NOT_NULL) ? DELETE : UPDATE,
                        tx.nearXidVersion(),
                        newVer,
                        expireTime,
                        entry.key().partition(),
                        0L,
                        mvccVer)));

                entry.update(val, expireTime, ttl, newVer, true);

                entry.recordNodeId(affNodeId, topVer);
            }
            catch (IgniteCheckedException e) {
                resFut.onDone(e);

                return;
            }
            finally {
                if (entry.lockedByCurrentThread()) {
                    entry.unlockEntry();

                    cctx.evicts().touch(entry);
                }
            }

            entry.onUpdateFinished(0L);

            GridCacheUpdateTxResult updRes = valid ? new GridCacheUpdateTxResult(true, 0L, logPtr)
                : new GridCacheUpdateTxResult(false, logPtr);

            if (invoke) {
                assert res.invokeResult() != null;

                updRes.invokeResult(res.invokeResult());
            }

            updRes.newValue(res.newValue());

            if (needOldVal && compareIgnoreOpCounter(res.resultVersion(), mvccVer) != 0 &&
                (res.resultType() == ResultType.PREV_NOT_NULL || res.resultType() == ResultType.REMOVED_NOT_NULL))
                updRes.oldValue(res.oldValue());

            updRes.mvccHistory(res.history());

            resFut.onDone(updRes);
        }
    }

    /**
     *
     */
    private class LazyValueEntry<K, V> implements Cache.Entry<K, V> {
        /** */
        private final KeyCacheObject key;

        /** */
        private boolean keepBinary;

        /**
         * @param key Key.
         * @param keepBinary Keep binary flag.
         */
        private LazyValueEntry(KeyCacheObject key, boolean keepBinary) {
            this.key = key;
            this.keepBinary = keepBinary;
        }

        /** {@inheritDoc} */
        @Override public K getKey() {
            return (K)cctx.cacheObjectContext().unwrapBinaryIfNeeded(key, keepBinary, true, null);
        }

        /** {@inheritDoc} */
        @Override public V getValue() {
            return (V)cctx.cacheObjectContext().unwrapBinaryIfNeeded(peekVisibleValue(), keepBinary, true, null);
        }

        /** {@inheritDoc} */
        @Override public <T> T unwrap(Class<T> cls) {
            if (cls.isAssignableFrom(IgniteCache.class))
                return (T)cctx.grid().cache(cctx.name());

            if (cls.isAssignableFrom(getClass()))
                return (T)this;

            if (cls.isAssignableFrom(EvictableEntry.class))
                return (T)wrapEviction();

            if (cls.isAssignableFrom(CacheEntryImplEx.class))
                return cls == CacheEntryImplEx.class ? (T)wrapVersioned() : (T)wrapVersionedWithValue();

            if (cls.isAssignableFrom(GridCacheVersion.class))
                return (T)ver;

            if (cls.isAssignableFrom(GridCacheMapEntry.this.getClass()))
                return (T)GridCacheMapEntry.this;

            throw new IllegalArgumentException("Unwrapping to class is not supported: " + cls);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "IteratorEntry [key=" + key + ']';
        }
    }

    /**
     * @param row Data row.
     * @return {@code True} if row expired.
     * @throws IgniteCheckedException If failed.
     */
    private boolean checkRowExpired(CacheDataRow row) throws IgniteCheckedException {
        assert row != null;
        assert !row.tombstone() : row;

        if (!(row.expireTime() > 0 && row.expireTime() <= U.currentTimeMillis()))
            return false;

        CacheObject expiredVal = row.value();

        if (isNear()) {
            update(null, CU.TTL_ETERNAL, CU.EXPIRE_TIME_ETERNAL, ver, true);

            cctx.decrementPublicSize(this);
        }

        if (cctx.events().isRecordable(EVT_CACHE_OBJECT_EXPIRED)) {
            cctx.events().addEvent(partition(),
                key(),
                cctx.localNodeId(),
                null,
                EVT_CACHE_OBJECT_EXPIRED,
                null,
                false,
                expiredVal,
                expiredVal != null,
                null,
                null,
                null,
                true);
        }

        cctx.continuousQueries().onEntryExpired(this, key(), expiredVal);

        return true;
    }

    /**
     *
     */
    private static class UpdateClosure implements IgniteCacheOffheapManager.OffheapInvokeClosure {
        /** */
        private final GridCacheMapEntry entry;

        /** */
        @Nullable private final CacheObject val;

        /** */
        private final GridCacheVersion ver;

        /** */
        private final long expireTime;

        /** */
        @Nullable private final IgniteBiPredicate<CacheObject, GridCacheVersion> p;

        /** */
        private CacheDataRow newRow;

        /** */
        private CacheDataRow oldRow;

        /** */
        private IgniteTree.OperationType treeOp = IgniteTree.OperationType.PUT;

        /**
         * @param entry Entry.
         * @param val New value.
         * @param ver New version.
         * @param expireTime New expire time.
         * @param p Optional predicate.
         */
        private UpdateClosure(
            GridCacheMapEntry entry,
            @Nullable CacheObject val,
            GridCacheVersion ver,
            long expireTime,
            @Nullable IgniteBiPredicate<CacheObject, GridCacheVersion> p
        ) {
            this.entry = entry;
            this.val = val;
            this.ver = ver;
            this.expireTime = expireTime;
            this.p = p;
        }

        /** {@inheritDoc} */
        @Override public void call(@Nullable CacheDataRow oldRow) throws IgniteCheckedException {
            if (oldRow != null)
                oldRow.key(entry.key);

            this.oldRow = oldRow;

            if (p != null) {
                CacheObject val = null;
                GridCacheVersion ver = entry.ver;

                if (oldRow != null) {
                    if (!oldRow.tombstone() && !entry.checkRowExpired(oldRow))
                        val = oldRow.value();

                    ver = oldRow.version();
                }

                if (!p.apply(val, ver)) {
                    treeOp = IgniteTree.OperationType.NOOP;

                    return;
                }
            }

            if (val != null) {
                newRow = entry.cctx.offheap().dataStore(entry.localPartition()).createRow(
                    entry.cctx,
                    entry.key,
                    val,
                    ver,
                    expireTime,
                    oldRow);
            }
            else {
                // Create tombstone for preloaded removal.
                newRow = entry.cctx.offheap().dataStore(entry.localPartition()).createRow(entry.cctx, entry.key(),
                    TombstoneCacheObject.INSTANCE, ver, entry.cctx.shared().ttl().tombstoneExpireTime(), oldRow);
            }

            treeOp = oldRow != null && oldRow.link() == newRow.link() ?
                IgniteTree.OperationType.IN_PLACE : IgniteTree.OperationType.PUT;
        }

        /** {@inheritDoc} */
        @Override public CacheDataRow newRow() {
            return newRow;
        }

        /** {@inheritDoc} */
        @Override public IgniteTree.OperationType operationType() {
            return treeOp;
        }

        /** {@inheritDoc} */
        @Nullable @Override public CacheDataRow oldRow() {
            return oldRow;
        }
    }

    /**
     * Invoke closure that allows to in-place update entry TTL.
     */
    private static class UpdateTtlClosure implements IgniteCacheOffheapManager.OffheapInvokeClosure {
        /** Entry to be updated. */
        private final GridCacheMapEntry entry;

        /** New expiration time. */
        private final long expireTime;

        /** Reference to the updated row. It can be {@code null} or equals to {@link #oldRow}. */
        private CacheDataRow newRow;

        /** Reference to the row to be updated. */
        private CacheDataRow oldRow;

        /**
         * Result of applying this closure to the row.
         * The possible values are:
         *  - NOOP - entry not found.
         *  - IN_PLACE - in-place update of ttl.
         **/
        private IgniteTree.OperationType treeOp = IgniteTree.OperationType.IN_PLACE;

        /**
         * @param entry Entry.
         * @param expireTime New expiration time.
         */
        private UpdateTtlClosure(
            GridCacheMapEntry entry,
            long expireTime
        ) {
            this.entry = entry;
            this.expireTime = expireTime;
        }

        /** {@inheritDoc} */
        @Override public void call(@Nullable CacheDataRow oldRow) throws IgniteCheckedException {
            if (oldRow == null) {
                treeOp = IgniteTree.OperationType.NOOP;

                return;
            }

            oldRow.key(entry.key);
            this.oldRow = oldRow;

            newRow = entry.cctx.offheap().dataStore(entry.localPartition()).createRowForTtlUpdate(
                entry.cctx,
                expireTime,
                oldRow);

            assert oldRow.link() == newRow.link() :
                "Links to the rows are not equal for update TTL closure [oldRow=" + oldRow + ", newRow=" + newRow + ']';

            treeOp = IgniteTree.OperationType.IN_PLACE;
        }

        /** {@inheritDoc} */
        @Override public CacheDataRowAdapter.RowData rowData() {
            return CacheDataRowAdapter.RowData.NO_KEY_WITH_VALUE_META_INFO;
        }

        /** {@inheritDoc} */
        @Override public CacheDataRow newRow() {
            return newRow;
        }

        /** {@inheritDoc} */
        @Override public IgniteTree.OperationType operationType() {
            return treeOp;
        }

        /** {@inheritDoc} */
        @Nullable @Override public CacheDataRow oldRow() {
            return oldRow;
        }
    }

    /**
     *
     */
    private static class AtomicCacheUpdateClosure implements IgniteCacheOffheapManager.OffheapInvokeClosure {
        /** */
        private final GridCacheMapEntry entry;

        /** */
        private final AffinityTopologyVersion topVer;

        /** */
        private GridCacheVersion newVer;

        /** */
        private GridCacheOperation op;

        /** */
        private Object writeObj;

        /** */
        private Object[] invokeArgs;

        /** */
        private final boolean readThrough;

        /** */
        private final boolean writeThrough;

        /** */
        private final boolean keepBinary;

        /** */
        private final IgniteCacheExpiryPolicy expiryPlc;

        /** */
        private final boolean primary;

        /** */
        private final boolean verCheck;

        /** */
        private final CacheEntryPredicate[] filter;

        /** */
        private final long explicitTtl;

        /** */
        private final long explicitExpireTime;

        /** */
        private GridCacheVersion conflictVer;

        /** */
        private final boolean conflictResolve;

        /** */
        private final boolean intercept;

        /** */
        private final Long updateCntr;

        /** */
        private final boolean skipInterceptorOnConflict;

        /** */
        private GridCacheUpdateAtomicResult updateRes;

        /** */
        private IgniteTree.OperationType treeOp;

        /** */
        private CacheDataRow newRow;

        /** */
        private CacheDataRow oldRow;

        /** OldRow expiration flag. */
        private boolean ignoreOldValue;

        /** Disable interceptor invocation onAfter* methods flag. */
        private boolean wasIntercepted;

        /** Flag indicates that a new update has version less than the version that is already stored. */
        private boolean outOfOrderUpdate;

        /** */
        AtomicCacheUpdateClosure(
            GridCacheMapEntry entry,
            AffinityTopologyVersion topVer,
            GridCacheVersion newVer,
            GridCacheOperation op,
            Object writeObj,
            Object[] invokeArgs,
            boolean readThrough,
            boolean writeThrough,
            boolean keepBinary,
            @Nullable IgniteCacheExpiryPolicy expiryPlc,
            boolean primary,
            boolean verCheck,
            @Nullable CacheEntryPredicate[] filter,
            long explicitTtl,
            long explicitExpireTime,
            @Nullable GridCacheVersion conflictVer,
            boolean conflictResolve,
            boolean intercept,
            @Nullable Long updateCntr,
            boolean skipInterceptorOnConflict) {
            assert op == UPDATE || op == DELETE || op == TRANSFORM : op;

            this.entry = entry;
            this.topVer = topVer;
            this.newVer = newVer;
            this.op = op;
            this.writeObj = writeObj;
            this.invokeArgs = invokeArgs;
            this.readThrough = readThrough;
            this.writeThrough = writeThrough;
            this.keepBinary = keepBinary;
            this.expiryPlc = expiryPlc;
            this.primary = primary;
            this.verCheck = verCheck;
            this.filter = filter;
            this.explicitTtl = explicitTtl;
            this.explicitExpireTime = explicitExpireTime;
            this.conflictVer = conflictVer;
            this.conflictResolve = conflictResolve;
            this.intercept = intercept;
            this.updateCntr = updateCntr;
            this.skipInterceptorOnConflict = skipInterceptorOnConflict;

            switch (op) {
                case UPDATE:
                    treeOp = IgniteTree.OperationType.PUT;

                    break;

                case DELETE:
                    treeOp = IgniteTree.OperationType.REMOVE;

                    break;
            }
        }

        /** {@inheritDoc} */
        @Nullable @Override public CacheDataRow oldRow() {
            return oldRow;
        }

        /** {@inheritDoc} */
        @Override public CacheDataRow newRow() {
            return newRow;
        }

        /** {@inheritDoc} */
        @Override public IgniteTree.OperationType operationType() {
            return treeOp;
        }

        /** Mode that is used to find and prepare the old row before this closure is executed. */
        private CacheDataRowAdapter.RowData rowData;

        /**
         * Sets a mode that is used to find and prepare the old row before this closure is executed.
         *
         * @param rowData Row data.
         */
        public void rowData(CacheDataRowAdapter.RowData rowData) {
            this.rowData = rowData;
        }

        @Override public CacheDataRowAdapter.RowData rowData() {
            return rowData != null ? rowData : IgniteCacheOffheapManager.OffheapInvokeClosure.super.rowData();
        }

        /** {@inheritDoc} */
        @Override public void call(@Nullable CacheDataRow oldRow) throws IgniteCheckedException {
            assert entry.isNear() || oldRow == null || oldRow.link() != 0 : oldRow;

            GridCacheContext cctx = entry.context();

            CacheObject oldVal = null;
            CacheObject storeLoadedVal = null;

            this.oldRow = oldRow;

            if (oldRow != null) {
                oldRow.key(entry.key());

                // Tombstone must be treated as empty value, same as expired row.
                oldVal = !oldRow.tombstone() && !entry.checkRowExpired(oldRow) ? oldRow.value() : null;

                entry.update(oldVal, oldRow.expireTime(), 0, oldRow.version(), false);

                if (oldVal == null)
                    ignoreOldValue = true;
            }

            if (oldVal == null && readThrough) {
                storeLoadedVal = cctx.toCacheObject(cctx.store().load(null, entry.key));

                if (storeLoadedVal != null) {
                    oldVal = storeLoadedVal.prepareForCache(cctx.cacheObjectContext(), true);

                    entry.val = oldVal;
                }
            }

            CacheInvokeEntry<Object, Object> invokeEntry = null;
            IgniteBiTuple<Object, Exception> invokeRes = null;

            boolean invoke = op == TRANSFORM;

            boolean transformed = false;

            if (invoke) {
                invokeEntry = new CacheInvokeEntry<>(entry.key, oldVal, entry.ver, keepBinary, entry);

                invokeRes = runEntryProcessor(invokeEntry);

                op = writeObj == null ? DELETE : UPDATE;

                transformed = true;
            }

            CacheObject newVal = (CacheObject)writeObj;

            GridCacheVersionConflictContext<?, ?> conflictCtx = null;

            if (conflictResolve) {
                conflictCtx = resolveConflict(newVal, invokeRes);

                if (updateRes != null) {
                    assert conflictCtx != null && conflictCtx.isUseOld() : conflictCtx;
                    assert treeOp == IgniteTree.OperationType.NOOP : treeOp;

                    return;
                }
            }

            if (conflictCtx == null) {
                // Perform version check only in case there was no explicit conflict resolution.
                versionCheck(invokeRes);

                if (updateRes != null) {
                    assert treeOp == IgniteTree.OperationType.NOOP : treeOp;

                    if (outOfOrderUpdate)
                        outOfOrderUpdate(conflictCtx);

                    return;
                }
            }

            if (!F.isEmptyOrNulls(filter)) {
                boolean pass = cctx.isAllLocked(entry, filter);

                if (!pass) {
                    initResultOnCancelUpdate(storeLoadedVal, !cctx.putIfAbsentFilter(filter));

                    updateRes = new GridCacheUpdateAtomicResult(UpdateOutcome.FILTER_FAILED,
                        oldVal,
                        null,
                        invokeRes,
                        CU.TTL_ETERNAL,
                        CU.EXPIRE_TIME_ETERNAL,
                        null,
                        null,
                        0,
                        false);

                    return;
                }
            }

            if (invoke) {
                if (!invokeEntry.modified()) {
                    initResultOnCancelUpdate(storeLoadedVal, true);

                    updateRes = new GridCacheUpdateAtomicResult(UpdateOutcome.INVOKE_NO_OP,
                        oldVal,
                        null,
                        invokeRes,
                        CU.TTL_ETERNAL,
                        CU.EXPIRE_TIME_ETERNAL,
                        null,
                        null,
                        0,
                        true);

                    return;
                }
                else if ((invokeRes == null || invokeRes.getValue() == null) && writeObj != null) {
                    try {
                        cctx.validateKeyAndValue(entry.key, (CacheObject)writeObj);
                    }
                    catch (Exception e) {
                        initResultOnCancelUpdate(null, true);

                        updateRes = new GridCacheUpdateAtomicResult(UpdateOutcome.INVOKE_NO_OP,
                            oldVal,
                            null,
                            new IgniteBiTuple<>(null, e),
                            CU.TTL_ETERNAL,
                            CU.EXPIRE_TIME_ETERNAL,
                            null,
                            null,
                            0,
                            false);

                        return;
                    }
                }

                op = writeObj == null ? DELETE : UPDATE;
            }

            // Incorporate conflict version into new version if needed.
            if (conflictVer != null && conflictVer != newVer) {
                newVer = new GridCacheVersionEx(newVer.topologyVersion(),
                    newVer.order(),
                    newVer.nodeOrder(),
                    newVer.dataCenterId(),
                    newVer.updateCounter(),
                    conflictVer);
            }

            if (op == UPDATE) {
                assert writeObj != null;

                update(conflictCtx, invokeRes, storeLoadedVal != null, transformed);
            }
            else {
                assert op == DELETE && writeObj == null : op;

                remove(conflictCtx, invokeRes, storeLoadedVal != null, transformed);
            }

            assert updateRes != null && treeOp != null;
        }

        /**
         * @param storeLoadedVal Value loaded from store.
         * @param updateExpireTime {@code True} if need update expire time.
         * @throws IgniteCheckedException If failed.
         */
        private void initResultOnCancelUpdate(@Nullable CacheObject storeLoadedVal, boolean updateExpireTime)
            throws IgniteCheckedException {
            boolean needUpdate = false;

            if (storeLoadedVal != null) {
                long initTtl;
                long initExpireTime;

                if (expiryPlc != null) {
                    IgniteBiTuple<Long, Long> initTtlAndExpireTime = initialTtlAndExpireTime(expiryPlc);

                    initTtl = initTtlAndExpireTime.get1();
                    initExpireTime = initTtlAndExpireTime.get2();
                }
                else {
                    initTtl = CU.TTL_ETERNAL;
                    initExpireTime = CU.EXPIRE_TIME_ETERNAL;
                }

                entry.update(storeLoadedVal, initExpireTime, initTtl, entry.ver, true);

                needUpdate = true;
            }
            else if (updateExpireTime && expiryPlc != null && entry.val != null) {
                long ttl = expiryPlc.forAccess();

                if (ttl != CU.TTL_NOT_CHANGED) {
                    long expireTime;

                    if (ttl == CU.TTL_ZERO) {
                        ttl = CU.TTL_MINIMUM;
                        expireTime = CU.expireTimeInPast();
                    }
                    else
                        expireTime = CU.toExpireTime(ttl);

                    if (entry.expireTimeExtras() != expireTime) {
                        entry.update(entry.val, expireTime, ttl, entry.ver, true);

                        expiryPlc.ttlUpdated(entry.key, entry.ver, null);

                        needUpdate = true;
                        storeLoadedVal = entry.val;
                    }
                }
            }

            if (needUpdate) {
                newRow = entry.localPartition().dataStore().createRow(
                    entry.cctx,
                    entry.key,
                    storeLoadedVal,
                    newVer,
                    entry.expireTimeExtras(),
                    oldRow);

                treeOp = IgniteTree.OperationType.PUT;
            }
            else
                treeOp = IgniteTree.OperationType.NOOP;
        }

        /**
         *
         * @param conflictCtx Conflict context.
         * @throws IgniteCheckedException If failed.
         */
        private void outOfOrderUpdate(@Nullable GridCacheVersionConflictContext<?, ?> conflictCtx) throws IgniteCheckedException {
            assert outOfOrderUpdate : "This method can be executed only for out-of-order updates.";
            assert !primary : "Out of order update can only be logged on backup nodes.";
            assert !intercept : "Out of order update should not be intercepted on backup node.";

            if (op == UPDATE) {
                assert writeObj != null;

                GridCacheContext cctx = entry.context();

                CacheObject prevUpdate = (CacheObject)writeObj;

                long newSysTtl;
                long newExpireTime;

                // Conflict context is null if there were no explicit conflict resolution.
                if (conflictCtx == null) {
                    // Calculate TTL and expire time for local update.
                    if (explicitTtl != CU.TTL_NOT_CHANGED) {
                        // If conflict existed, expire time must be explicit.
                        assert conflictVer == null || explicitExpireTime != CU.EXPIRE_TIME_CALCULATE;

                        newExpireTime = explicitExpireTime != CU.EXPIRE_TIME_CALCULATE ?
                            explicitExpireTime : CU.toExpireTime(explicitTtl);
                    }
                    else {
                        newSysTtl = expiryPlc == null ? CU.TTL_NOT_CHANGED :
                            entry.val != null ? expiryPlc.forUpdate() : expiryPlc.forCreate();

                        if (newSysTtl == CU.TTL_NOT_CHANGED)
                            newExpireTime = entry.expireTimeExtras();
                        else if (newSysTtl == CU.TTL_ZERO) {
                            op = DELETE;

                            writeObj = null;

                            entry.logOutOfOrderUpdate(op, null, newVer, 0, updateCntr);

                            return;
                        }
                        else
                            newExpireTime = CU.toExpireTime(newSysTtl);
                    }
                }
                else
                    newExpireTime = conflictCtx.expireTime();

                prevUpdate = prevUpdate.prepareForCache(cctx.cacheObjectContext(), true);

                entry.logOutOfOrderUpdate(op, prevUpdate, newVer, newExpireTime, updateCntr);
            }
            else {
                assert op == DELETE && writeObj == null : op;

                entry.logOutOfOrderUpdate(op, null, newVer, 0, updateCntr);
            }
        }

        /**
         * @param conflictCtx Conflict context.
         * @param invokeRes Entry processor result (for invoke operation).
         * @param readFromStore {@code True} if initial entry value was {@code null} and it was read from store.
         * @param transformed {@code True} if update caused by transformation operation.
         * @throws IgniteCheckedException If failed.
         */
        private void update(@Nullable GridCacheVersionConflictContext<?, ?> conflictCtx,
            @Nullable IgniteBiTuple<Object, Exception> invokeRes,
            boolean readFromStore,
            boolean transformed)
            throws IgniteCheckedException {
            GridCacheContext cctx = entry.context();

            final CacheObject oldVal = entry.val;
            CacheObject updated = (CacheObject)writeObj;

            long newSysTtl;
            long newSysExpireTime;

            long newTtl;
            long newExpireTime;

            // Conflict context is null if there were no explicit conflict resolution.
            if (conflictCtx == null) {
                // Calculate TTL and expire time for local update.
                if (explicitTtl != CU.TTL_NOT_CHANGED) {
                    // If conflict existed, expire time must be explicit.
                    assert conflictVer == null || explicitExpireTime != CU.EXPIRE_TIME_CALCULATE;

                    newSysTtl = newTtl = explicitTtl;
                    newSysExpireTime = explicitExpireTime;

                    newExpireTime = explicitExpireTime != CU.EXPIRE_TIME_CALCULATE ?
                        explicitExpireTime : CU.toExpireTime(explicitTtl);
                }
                else {
                    newSysTtl = expiryPlc == null ? CU.TTL_NOT_CHANGED :
                        entry.val != null ? expiryPlc.forUpdate() : expiryPlc.forCreate();

                    if (newSysTtl == CU.TTL_NOT_CHANGED) {
                        newSysExpireTime = CU.EXPIRE_TIME_CALCULATE;
                        newTtl = entry.ttlExtras();
                        newExpireTime = entry.deletedUnlocked() ? CU.EXPIRE_TIME_ETERNAL : entry.expireTimeExtras();
                    }
                    else if (newSysTtl == CU.TTL_ZERO) {
                        op = DELETE;

                        writeObj = null;

                        remove(conflictCtx, invokeRes, readFromStore, false);

                        return;
                    }
                    else {
                        newSysExpireTime = CU.EXPIRE_TIME_CALCULATE;
                        newTtl = newSysTtl;
                        newExpireTime = CU.toExpireTime(newTtl);
                    }
                }
            }
            else {
                newSysTtl = newTtl = conflictCtx.ttl();
                newSysExpireTime = newExpireTime = conflictCtx.expireTime();
            }

            if (intercept && (conflictVer == null || !skipInterceptorOnConflict)) {
                Object updated0 = cctx.unwrapBinaryIfNeeded(updated, keepBinary, false, null);

                CacheLazyEntry<Object, Object> interceptEntry =
                    new CacheLazyEntry<>(cctx, entry.key, null, oldVal, null, keepBinary);

                Object interceptorVal = null;

                try {
                    interceptorVal = cctx.config().getInterceptor().onBeforePut(interceptEntry, updated0);
                }
                catch (Throwable e) {
                    throw new IgniteCheckedException(e);
                }

                wasIntercepted = true;

                if (interceptorVal == null) {
                    treeOp = IgniteTree.OperationType.NOOP;

                    updateRes = new GridCacheUpdateAtomicResult(UpdateOutcome.INTERCEPTOR_CANCEL,
                        oldVal,
                        null,
                        invokeRes,
                        CU.TTL_ETERNAL,
                        CU.EXPIRE_TIME_ETERNAL,
                        null,
                        null,
                        0,
                        false);

                    return;
                }
                else if (interceptorVal != updated0) {
                    updated0 = cctx.unwrapTemporary(interceptorVal);

                    updated = cctx.toCacheObject(updated0);
                }
            }

            if (updated != null)
                updated = updated.prepareForCache(cctx.cacheObjectContext(), true);

            if (writeThrough)
                // Must persist inside synchronization in non-tx mode.
                cctx.store().put(null, entry.key, updated, newVer);

            if (entry.isNear()) {
                if (entry.val == null) {
                    boolean new0 = entry.isStartVersion();

                    if (!new0 && !entry.isInternal())
                        cctx.incrementPublicSize(entry);
                }
            }

            long updateCntr0 = entry.nextPartitionCounter(topVer, primary, false, updateCntr);

            if (newVer.updateCounter() == 0)  // primary and/or putAll
                newVer.updateCounter(updateCntr0);

            entry.logUpdate(op, updated, newVer, newExpireTime, updateCntr0, primary);

            if (!entry.isNear()) {
                newRow = entry.localPartition().dataStore().createRow(
                    entry.cctx,
                    entry.key,
                    updated,
                    newVer,
                    newExpireTime,
                    oldRow);

                treeOp = oldRow != null && oldRow.link() == newRow.link() ?
                    IgniteTree.OperationType.IN_PLACE : IgniteTree.OperationType.PUT;
            }
            else
                treeOp = IgniteTree.OperationType.PUT;

            entry.update(updated, newExpireTime, newTtl, newVer, true);

            if (entry.isNear()) {
                boolean updatedDht = ((GridNearCacheEntry)entry).recordDhtVersion(newVer);
                assert updatedDht : this;
            }

            updateRes = new GridCacheUpdateAtomicResult(UpdateOutcome.SUCCESS,
                oldVal,
                updated,
                invokeRes,
                newSysTtl,
                newSysExpireTime,
                null,
                conflictCtx,
                updateCntr0,
                transformed);
        }

        /**
         * @param conflictCtx Conflict context.
         * @param invokeRes Entry processor result (for invoke operation).
         * @param readFromStore {@code True} if initial entry value was {@code null} and it was read from store.
         * @param transformed {@code True} if remove caused by tranformation operation.
         * @throws IgniteCheckedException If failed.
         */
        @SuppressWarnings("unchecked")
        private void remove(@Nullable GridCacheVersionConflictContext<?, ?> conflictCtx,
            @Nullable IgniteBiTuple<Object, Exception> invokeRes,
            boolean readFromStore,
            boolean transformed)
            throws IgniteCheckedException {
            GridCacheContext cctx = entry.context();

            CacheObject oldVal = entry.val;

            IgniteBiTuple<Boolean, Object> interceptRes = null;

            if (intercept && (conflictVer == null || !skipInterceptorOnConflict)) {
                CacheLazyEntry<Object, Object> intercepEntry =
                    new CacheLazyEntry<>(cctx, entry.key, null, oldVal, null, keepBinary);

                interceptRes = cctx.config().getInterceptor().onBeforeRemove(intercepEntry);

                wasIntercepted = true;

                if (cctx.cancelRemove(interceptRes)) {
                    treeOp = IgniteTree.OperationType.NOOP;

                    updateRes = new GridCacheUpdateAtomicResult(UpdateOutcome.INTERCEPTOR_CANCEL,
                        cctx.toCacheObject(cctx.unwrapTemporary(interceptRes.get2())),
                        null,
                        invokeRes,
                        CU.TTL_ETERNAL,
                        CU.EXPIRE_TIME_ETERNAL,
                        null,
                        null,
                        0,
                        false);

                    return;
                }
            }

            if (writeThrough)
                // Must persist inside synchronization in non-tx mode.
                cctx.store().remove(null, entry.key);

            long updateCntr0 = entry.nextPartitionCounter(topVer, primary, false, updateCntr);

            if (newVer.updateCounter() == 0)
                newVer.updateCounter(updateCntr0);

            entry.logUpdate(op, null, newVer, 0, updateCntr0, primary);

            if (entry.isNear()) {
                if (oldVal != null && !entry.isInternal())
                    cctx.decrementPublicSize(entry);
                else {
                    boolean new0 = entry.isStartVersion(); // TODO can ever be here ? replace with assert.

                    if (new0 && !entry.isInternal()) { // TODO cannot have internal entries on near atomic.
                        cctx.decrementPublicSize(entry);
                    }
                }
            }

            treeOp = cctx.isNear() ? IgniteTree.OperationType.NOOP : // Avoid offheap tree update for near cache.
                IgniteTree.OperationType.PUT; // Always write tombstone, overwrite with new version if existed before.

            if (treeOp != IgniteTree.OperationType.NOOP) {
                GridDhtLocalPartition part = entry.localPartition();

                newRow = part.dataStore().createRow(
                    cctx,
                    entry.key(),
                    TombstoneCacheObject.INSTANCE,
                    newVer,
                    cctx.shared().ttl().tombstoneExpireTime(),
                    oldRow);

                if (oldRow != null && oldRow.link() == newRow.link())
                    treeOp = IgniteTree.OperationType.IN_PLACE;
            }

            GridCacheVersion enqueueVer = newVer;

            entry.update(null, CU.TTL_ETERNAL, CU.EXPIRE_TIME_ETERNAL, newVer, true);

            UpdateOutcome outcome = oldVal != null ? UpdateOutcome.SUCCESS : UpdateOutcome.REMOVE_NO_VAL;

            if (interceptRes != null)
                oldVal = cctx.toCacheObject(cctx.unwrapTemporary(interceptRes.get2()));

            updateRes = new GridCacheUpdateAtomicResult(outcome,
                oldVal,
                null,
                invokeRes,
                CU.TTL_NOT_CHANGED,
                CU.EXPIRE_TIME_CALCULATE,
                enqueueVer,
                conflictCtx,
                updateCntr0,
                transformed);
        }

        /**
         * @param newVal New entry value.
         * @param invokeRes Entry processor result (for invoke operation).
         * @return Conflict context.
         * @throws IgniteCheckedException If failed.
         */
        private GridCacheVersionConflictContext<?, ?> resolveConflict(
            CacheObject newVal,
            @Nullable IgniteBiTuple<Object, Exception> invokeRes)
            throws IgniteCheckedException {
            GridCacheContext cctx = entry.context();

            // Cache is conflict-enabled.
            if (cctx.conflictNeedResolve()) {
                GridCacheVersion oldConflictVer = entry.ver.conflictVersion();

                // Prepare old and new entries for conflict resolution.
                GridCacheVersionedEntryEx oldEntry = new GridCacheLazyPlainVersionedEntry<>(cctx,
                    entry.key,
                    entry.val,
                    entry.ttlExtras(),
                    entry.expireTimeExtras(),
                    entry.ver.conflictVersion(),
                    entry.isStartVersion(),
                    keepBinary);

                GridTuple3<Long, Long, Boolean> expiration = entry.ttlAndExpireTime(expiryPlc,
                    explicitTtl,
                    explicitExpireTime);

                GridCacheVersionedEntryEx newEntry = new GridCacheLazyPlainVersionedEntry<>(
                    cctx,
                    entry.key,
                    newVal,
                    expiration.get1(),
                    expiration.get2(),
                    conflictVer != null ? conflictVer : newVer,
                    keepBinary);

                // Resolve conflict.
                GridCacheVersionConflictContext<?, ?> conflictCtx = cctx.conflictResolve(oldEntry, newEntry, verCheck);

                assert conflictCtx != null;

                // Use old value?
                if (conflictCtx.isUseOld()) {
                    GridCacheVersion newConflictVer = conflictVer != null ? conflictVer : newVer;

                    // Handle special case with atomic comparator.
                    if (!entry.isStartVersion() &&                                                        // Not initial value,
                        verCheck &&                                                                       // and atomic version check,
                        oldConflictVer.dataCenterId() == newConflictVer.dataCenterId() &&                 // and data centers are equal,
                        ATOMIC_VER_COMPARATOR.compare(oldConflictVer, newConflictVer) == 0 && // and both versions are equal,
                        cctx.writeThrough() &&                                                            // and store is enabled,
                        primary)                                                                          // and we are primary.
                    {
                        CacheObject val = entry.val;

                        if (val == null)
                            cctx.store().remove(null, entry.key);
                        else
                            cctx.store().put(null, entry.key, val, entry.ver);
                    }

                    treeOp = IgniteTree.OperationType.NOOP;

                    updateRes = new GridCacheUpdateAtomicResult(UpdateOutcome.CONFLICT_USE_OLD,
                        entry.val,
                        null,
                        invokeRes,
                        CU.TTL_ETERNAL,
                        CU.EXPIRE_TIME_ETERNAL,
                        null,
                        null,
                        0,
                        false);
                }
                // Will update something.
                else {
                    // Merge is a local update which override passed value bytes.
                    if (conflictCtx.isMerge()) {
                        writeObj = cctx.toCacheObject(conflictCtx.mergeValue());

                        conflictVer = null;
                    }
                    else
                        assert conflictCtx.isUseNew();

                    // Update value is known at this point, so update operation type.
                    op = writeObj != null ? UPDATE : DELETE;
                }

                return conflictCtx;
            }
            else
                // Nullify conflict version on this update, so that we will use regular version during next updates.
                conflictVer = null;

            return null;
        }

        /**
         * @param invokeRes Entry processor result (for invoke operation).
         * @throws IgniteCheckedException If failed.
         */
        private void versionCheck(@Nullable IgniteBiTuple<Object, Exception> invokeRes) throws IgniteCheckedException {
            GridCacheContext cctx = entry.context();

            if (verCheck) {
                if (!entry.isStartVersion() && ATOMIC_VER_COMPARATOR.compare(entry.ver, newVer) >= 0) {
                    outOfOrderUpdate = ATOMIC_VER_COMPARATOR.compare(entry.ver, newVer) > 0;

                    if (!outOfOrderUpdate && cctx.writeThrough() && primary) {
                        if (log.isDebugEnabled())
                            log.debug("Received entry update with same version as current (will update store) " +
                                "[entry=" + this + ", newVer=" + newVer + ']');

                        CacheObject val = entry.val;

                        if (val == null)
                            cctx.store().remove(null, entry.key);
                        else {
                            assert !(val instanceof CacheObjectShadow) : "Invalid value type [entry=" + this + ']';
                            cctx.store().put(null, entry.key, val, entry.ver);
                        }
                    }
                    else {
                        if (log.isDebugEnabled())
                            log.debug("Received entry update with smaller version than current (will ignore) " +
                                "[entry=" + this + ", newVer=" + newVer + ']');
                    }

                    treeOp = IgniteTree.OperationType.NOOP;

                    updateRes = new GridCacheUpdateAtomicResult(UpdateOutcome.VERSION_CHECK_FAILED,
                        entry.val,
                        null,
                        invokeRes,
                        CU.TTL_ETERNAL,
                        CU.EXPIRE_TIME_ETERNAL,
                        null,
                        null,
                        0,
                        false);
                }
            }
            else {
                assert entry.isStartVersion() || ATOMIC_VER_COMPARATOR.compare(entry.ver, newVer) <= 0 :
                    "Invalid version for inner update [name=" + cctx.group().cacheOrGroupName() +
                        ", topVer=" + cctx.group().topology().readyTopologyVersion() +
                        ", isNew=" + entry.isStartVersion() + ", entry=" + entry + ", newVer=" + newVer + ']';
            }
        }

        /**
         * @param invokeEntry Entry for {@link EntryProcessor}.
         * @return Entry processor return value.
         */
        private IgniteBiTuple<Object, Exception> runEntryProcessor(CacheInvokeEntry<Object, Object> invokeEntry) {
            EntryProcessor<Object, Object, ?> entryProcessor = (EntryProcessor<Object, Object, ?>)writeObj;

            IgniteThread.onEntryProcessorEntered(true);

            if (invokeEntry.cctx.kernalContext().deploy().enabled() &&
                invokeEntry.cctx.kernalContext().deploy().isGlobalLoader(entryProcessor.getClass().getClassLoader())) {
                U.restoreDeploymentContext(invokeEntry.cctx.kernalContext(), invokeEntry.cctx.kernalContext()
                    .deploy().getClassLoaderId(entryProcessor.getClass().getClassLoader()));
            }

            try {
                Object computed = entryProcessor.process(invokeEntry, invokeArgs);

                if (invokeEntry.modified()) {
                    GridCacheContext cctx = entry.context();

                    writeObj = cctx.toCacheObject(cctx.unwrapTemporary(invokeEntry.getValue()));
                }
                else
                    writeObj = invokeEntry.valObj;

                if (computed != null)
                    return new IgniteBiTuple<>(entry.cctx.unwrapTemporary(computed), null);

                return null;
            }
            catch (UnregisteredClassException | UnregisteredBinaryTypeException e) {
                throw e;
            }
            catch (Exception e) {
                writeObj = invokeEntry.valObj;

                return new IgniteBiTuple<>(null, e);
            }
            finally {
                IgniteThread.onEntryProcessorLeft();
            }
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(AtomicCacheUpdateClosure.class, this);
        }
    }

    /** {@inheritDoc} */
    @Override public GridCacheUpdateTxResult mvccUpdateRowsWithPreloadInfo(
        IgniteInternalTx tx,
        UUID affNodeId,
        AffinityTopologyVersion topVer,
        List<GridCacheEntryInfo> entries,
        GridCacheOperation op,
        MvccSnapshot mvccVer,
        IgniteUuid futId,
        int batchNum)
        throws IgniteCheckedException, GridCacheEntryRemovedException {
        assert mvccVer != null &&
            MvccUtils.mvccVersionIsValid(mvccVer.coordinatorVersion(), mvccVer.counter(), mvccVer.operationCounter());
        assert !F.isEmpty(entries);

        WALPointer logPtr = null;

        ensureFreeSpace();

        CacheObject val = null;
        CacheObject oldVal = null;

        lockEntry();

        try {
            checkObsolete();

            boolean walEnabled = cctx.group().persistenceEnabled() && cctx.group().walEnabled();

            List<DataEntry> walEntries = walEnabled ? new ArrayList<>(entries.size() + 1) : Collections.EMPTY_LIST;

            // Apply updates in reverse order (oldest is last) until any of previous versions found.
            // If we found prev version then it means history has been actualized either with previous update
            // or via rebalance.
            for (int i = 0; i < entries.size(); i++) {
                GridCacheMvccEntryInfo info = (GridCacheMvccEntryInfo)entries.get(i);

                assert info.mvccTxState() == TxState.COMMITTED ||
                    MvccUtils.compare(info, mvccVer.coordinatorVersion(), mvccVer.counter()) == 0;
                assert info.newMvccTxState() == TxState.COMMITTED ||
                    MvccUtils.compareNewVersion(info, mvccVer.coordinatorVersion(), mvccVer.counter()) == 0 ||
                    info.newMvccCoordinatorVersion() == MvccUtils.MVCC_CRD_COUNTER_NA;

                boolean added = cctx.offheap().mvccUpdateRowWithPreloadInfo(this,
                    info.value(),
                    info.version(),
                    info.expireTime(),
                    info.mvccVersion(),
                    info.newMvccVersion(),
                    info.mvccTxState(),
                    info.newMvccTxState());

                if (walEnabled)
                    walEntries.add(toMvccDataEntry(info, tx));

                if (oldVal == null
                    && MvccUtils.compare(info.mvccVersion(),
                    mvccVer.coordinatorVersion(),
                    mvccVer.counter()) != 0
                    && MvccUtils.compareNewVersion(info,
                    mvccVer.coordinatorVersion(),
                    mvccVer.counter()) == 0)
                    oldVal = info.value(); // Old means a value before current transaction.

                if (!added)
                    break;
            }

            GridCacheMvccEntryInfo last = (GridCacheMvccEntryInfo)entries.get(0);

            if (walEnabled)
                Collections.reverse(walEntries);

            if (op == DELETE) {
                assert MvccUtils.compareNewVersion(last, mvccVer) == 0;

                if (walEnabled)
                    walEntries.add(new MvccDataEntry(
                        cctx.cacheId(),
                        key,
                        null,
                        DELETE,
                        tx.nearXidVersion(),
                        last.version(),
                        last.expireTime(),
                        key.partition(),
                        0,
                        last.mvccVersion()));
            }
            else {
                assert last.newMvccCoordinatorVersion() == MvccUtils.MVCC_CRD_COUNTER_NA;
                assert MvccUtils.compare(last, mvccVer) == 0;

                val = last.value();
            }

            if (walEnabled)
                logPtr = cctx.shared().wal().log(new MvccDataRecord(walEntries));
        }
        finally {
            if (lockedByCurrentThread()) {
                unlockEntry();

                cctx.evicts().touch(this);
            }
        }

        GridCacheUpdateTxResult res = new GridCacheUpdateTxResult(true, logPtr);

        res.newValue(val);
        res.oldValue(oldVal);

        return res;
    }

    /** {@inheritDoc} */
    @Override public boolean mvccPreloadEntry(List<GridCacheMvccEntryInfo> entryHist)
        throws IgniteCheckedException, GridCacheEntryRemovedException {
        assert !entryHist.isEmpty();

        WALPointer logPtr = null;

        ensureFreeSpace();

        boolean updated = false;

        lockEntry();

        try {
            checkObsolete();

            if (cctx.offheap().mvccApplyHistoryIfAbsent(this, entryHist)) {
                updated = true;

                if (!cctx.isNear() && cctx.group().persistenceEnabled() && cctx.group().walEnabled()) {
                    MvccDataRecord rec;

                    if (entryHist.size() == 1) {
                        GridCacheMvccEntryInfo info = entryHist.get(0);

                        rec = new MvccDataRecord(toMvccDataEntry(info, null));
                    }
                    else {
                        // Batched WAL update.
                        List<DataEntry> dataEntries = new ArrayList<>(entryHist.size());

                        for (GridCacheMvccEntryInfo info : entryHist)
                            dataEntries.add(toMvccDataEntry(info, null));

                        rec = new MvccDataRecord(dataEntries);
                    }

                    logPtr = cctx.shared().wal().log(rec);
                }
            }
        }
        finally {
            if (lockedByCurrentThread()) {
                unlockEntry();

                cctx.evicts().touch(this);
            }
        }

        if (logPtr != null)
            cctx.shared().wal().flush(logPtr, false);

        return updated;
    }

    /**
     * Converts mvcc entry info to WAL record entry.
     * @param info Mvcc entry info.
     * @return Mvcc data entry.
     */
    private @NotNull MvccDataEntry toMvccDataEntry(@NotNull GridCacheMvccEntryInfo info, @Nullable IgniteInternalTx tx) {
        return new MvccDataEntry(
            cctx.cacheId(),
            key,
            info.value(),
            CREATE,
            tx == null ? null : tx.nearXidVersion(),
            info.version(),
            info.expireTime(),
            key.partition(),
            0,
            info.mvccVersion()
        );
    }

    /**
     * @return Common serialization error.
     */
    private static IgniteTxSerializationCheckedException serializationError() {
        return new IgniteTxSerializationCheckedException(
            "Cannot serialize transaction due to write conflict (transaction is marked for rollback)"
        );
    }

    /**
     * Invokes platform cache update callback, if applicable.
     *
     * @param val Updated value, null on remove.
     * @param ver Topology version, null on remove.
     */
    protected void updatePlatformCache(@Nullable CacheObject val, @Nullable AffinityTopologyVersion ver) {
        if (!hasPlatformCache())
            return;

        PlatformProcessor proc = cctx.kernalContext().platform();
        if (!proc.hasContext() || !proc.context().isPlatformCacheSupported())
            return;

        try {
            CacheObjectContext ctx = cctx.cacheObjectContext();
            byte[] keyBytes = key.valueBytes(ctx);

            // val is null when entry is removed.
            // valid(ver) is false when near cache entry is out of sync.
            boolean valid = val != null && ver != null && valid(ver);

            // null valBytes means that entry should be removed from platform cache.
            byte[] valBytes = valid ? val.valueBytes(ctx) : null;

            proc.context().updatePlatformCache(cctx.cacheId(), keyBytes, valBytes, partition(), ver);
        } catch (Throwable e) {
            U.error(log, "Failed to update Platform Cache: " + e);
        }
    }

    /**
     * Gets a value indicating whether platform cache exists for current cache.
     *
     * @return True when platform cache exists for this cache; false otherwise.
     */
    @SuppressWarnings("rawtypes")
    private boolean hasPlatformCache() {
        GridCacheAdapter cache = cctx.cache();

        return cache != null && cache.cacheCfg.getPlatformCacheConfiguration() != null;
    }

    /**
     * Trace replication calls.
     */
    private void traceReplicate(GridCacheVersion ver, String keyValue) {
        // log version here
        if (log.isTraceEnabled()) {
            log.trace("Invoking replication, key=" + keyValue +
                    ", near=" + cctx.isNear() + ", detached=" + detached()
                    + ", ver=" + ver + ", updateCounter=" + ver.updateCounter());
        }
    }

    /** */
    private void logOnMoreRecent() {
        if (log.isDebugEnabled())
            log.debug("recordDhtVersion is more recent, skipping cache op for key=" + keyValue(false));
    }
}
