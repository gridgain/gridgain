/*
 * Copyright 2023 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.internal.processors.query.h2.opt;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteInterruptedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.query.QueryRetryException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheContextInfo;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode;
import org.apache.ignite.internal.processors.cache.query.QueryTable;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.QueryField;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.processors.query.h2.H2TableDescriptor;
import org.apache.ignite.internal.processors.query.h2.H2Utils;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.apache.ignite.internal.processors.query.h2.IndexRebuildPartialClosure;
import org.apache.ignite.internal.processors.query.h2.database.H2IndexType;
import org.apache.ignite.internal.processors.query.h2.database.H2TreeIndex;
import org.apache.ignite.internal.processors.query.h2.database.H2TreeIndexBase;
import org.apache.ignite.internal.processors.query.h2.database.IndexInformation;
import org.apache.ignite.internal.processors.query.schema.SchemaOperationException;
import org.apache.ignite.internal.processors.query.stat.ObjectStatistics;
import org.apache.ignite.internal.processors.query.stat.StatisticsKey;
import org.apache.ignite.internal.util.tostring.GridToStringBuilder;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.spi.indexing.IndexingQueryCacheFilter;
import org.apache.ignite.spi.indexing.IndexingQueryFilter;
import org.gridgain.internal.h2.command.ddl.CreateTableData;
import org.gridgain.internal.h2.engine.DbObject;
import org.gridgain.internal.h2.engine.Session;
import org.gridgain.internal.h2.engine.SysProperties;
import org.gridgain.internal.h2.index.HashJoinIndex;
import org.gridgain.internal.h2.index.Index;
import org.gridgain.internal.h2.index.IndexType;
import org.gridgain.internal.h2.index.SpatialIndex;
import org.gridgain.internal.h2.message.DbException;
import org.gridgain.internal.h2.result.Row;
import org.gridgain.internal.h2.result.SortOrder;
import org.gridgain.internal.h2.schema.SchemaObject;
import org.gridgain.internal.h2.table.Column;
import org.gridgain.internal.h2.table.IndexColumn;
import org.gridgain.internal.h2.table.Table;
import org.gridgain.internal.h2.table.TableBase;
import org.gridgain.internal.h2.table.TableType;
import org.jetbrains.annotations.Nullable;

import static java.lang.Integer.toHexString;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion.NONE;
import static org.apache.ignite.internal.processors.query.h2.H2TableDescriptor.AFFINITY_KEY_IDX_NAME;
import static org.apache.ignite.internal.processors.query.h2.H2TableDescriptor.PK_HASH_IDX_NAME;
import static org.apache.ignite.internal.processors.query.h2.opt.H2TableScanIndex.SCAN_INDEX_NAME_SUFFIX;
import static org.apache.ignite.internal.processors.query.schema.SchemaOperationException.CODE_INDEX_EXISTS;
import static org.apache.ignite.internal.util.IgniteUtils.byteArray2HexString;
import static org.apache.ignite.internal.util.IgniteUtils.nl;
import static org.apache.ignite.internal.util.tostring.GridToStringBuilder.getSensitiveDataLogging;

/**
 * H2 Table implementation.
 */
public class GridH2Table extends TableBase {
    /** Exclusive lock constant. */
    private static final long EXCLUSIVE_LOCK = -1;

    /** 'rebuildFromHashInProgress' field updater */
    private static final AtomicIntegerFieldUpdater<GridH2Table> rebuildFromHashInProgressFieldUpdater =
        AtomicIntegerFieldUpdater.newUpdater(GridH2Table.class, "rebuildFromHashInProgress");

    /** False representation */
    private static final int FALSE = 0;

    /** True representation */
    private static final int TRUE = 1;

    /**
     * Row count statistics update threshold. Stats will be updated when the actual
     * table size change exceeds this threshold. Should be the number in interval (0,1).
     */
    private static final double STATS_UPDATE_THRESHOLD = 0.1; // 10%.

    /** Cache context info. */
    private final GridCacheContextInfo cacheInfo;

    /** */
    private final GridH2RowDescriptor desc;

    /** */
    private final GridH2IndexBase shadowedAffIndex;

    /** */
    private volatile ArrayList<Index> idxs;

    /** */
    private final int pkIndexPos;

    /** Total number of system indexes. */
    private final int sysIdxsCnt;

    /** */
    private final Map<String, GridH2IndexBase> tmpIdxs = new HashMap<>();

    /** */
    private final ReentrantReadWriteLock lock;

    /** */
    private final boolean hasHashIndex;

    /** */
    private volatile boolean destroyed;

    /**
     * Map of sessions locks.
     * Session -> EXCLUSIVE_LOCK (-1L) - for exclusive locks.
     * Session -> (table version) - for shared locks.
     */
    private final ConcurrentMap<Session, SessionLock> sessions = new ConcurrentHashMap<>();

    /** */
    private final IndexColumn affKeyCol;

    /** Whether affinity key column is the whole cache key. */
    private final boolean affKeyColIsKey;

    /** */
    private final LongAdder size = new LongAdder();

    /** */
    private volatile int rebuildFromHashInProgress = FALSE;

    /** Identifier. */
    private final QueryTable identifier;

    /** Identifier as string. */
    private final String identifierStr;

    /** Flag remove index or not when table will be destroyed. */
    private volatile boolean rmIndex;

    /** Columns with thread-safe access. */
    private volatile Column[] safeColumns;

    /** Table version. The version is changed when exclusive lock is acquired (DDL operation is started). */
    private final AtomicLong ver = new AtomicLong();

    /** Table statistics. */
    private volatile TableStatistics tblStats;

    /** Logger. */
    @GridToStringExclude
    private IgniteLogger log;

    /**
     * Creates table.
     *
     * @param createTblData Table description.
     * @param desc Row descriptor.
     * @param tblDesc Indexes factory.
     * @param cacheInfo Cache context info.
     */
    @SuppressWarnings("ConstantConditions")
    public GridH2Table(
        CreateTableData createTblData,
        GridH2RowDescriptor desc,
        H2TableDescriptor tblDesc,
        GridCacheContextInfo cacheInfo
    ) {
        super(createTblData);

        assert tblDesc != null;

        this.desc = desc;
        this.cacheInfo = cacheInfo;

        affKeyCol = calculateAffinityKeyColumn();
        affKeyColIsKey = affKeyCol != null && desc.isKeyColumn(affKeyCol.column.getColumnId());

        identifier = new QueryTable(getSchema().getName(), getName());

        identifierStr = identifier.schema() + "." + identifier.table();

        // Indexes must be created in the end when everything is ready.
        idxs = tblDesc.createSystemIndexes(this, log);

        assert idxs != null;

        List<Index> clones = new ArrayList<>(idxs.size());

        for (Index index : idxs) {
            Index clone = createDuplicateIndexIfNeeded(index);

            if (clone != null)
                clones.add(clone);
        }

        idxs.addAll(clones);

        hasHashIndex = idxs.size() >= 2 && index(0).getIndexType().isHash();

        // Add scan index at 0 which is required by H2.
        if (hasHashIndex)
            idxs.add(0, new H2TableScanIndex(this, index(1), index(0)));
        else
            idxs.add(0, new H2TableScanIndex(this, index(0), null));

        pkIndexPos = hasHashIndex ? 2 : 1;

        // HACK: Hide affinity index for compatibile behaviour.
        // Remember the index to don't forget later to drop it on cache destroy,
        // Othewise, there will be garbage in PDS, that may lead to broken tree
        // on next time when cache will be created.
        {
            Index affIdx = idxs.get(idxs.size() - 1);
            if (affIdx instanceof GridH2IndexBase &&
                AFFINITY_KEY_IDX_NAME.equals(affIdx.getName()) &&
                tblDesc.isSystemAffinityIndexShadowed(this)
            ) {
                shadowedAffIndex = (GridH2IndexBase)affIdx;

                idxs.remove(idxs.size() - 1);
            }
            else
                shadowedAffIndex = null;
        }

        sysIdxsCnt = idxs.size();

        lock = new ReentrantReadWriteLock();

        if (cacheInfo.affinityNode()) {
            long totalTblSize = cacheSize(CachePeekMode.PRIMARY, CachePeekMode.BACKUP);

            size.add(totalTblSize);
        }

        // Init stats with the default values.
        tblStats = new TableStatistics(10_000, 10_000);

        if (desc != null && desc.context() != null) {
            GridKernalContext ctx = desc.context().kernalContext();

            log = ctx.log(getClass());
        }
    }

    /**
     * @return Information about all indexes related to the table.
     */
    public List<IndexInformation> indexesInformation() {
        List<IndexInformation> res = new ArrayList<>();

        IndexColumn keyCol = indexColumn(QueryUtils.KEY_COL, SortOrder.ASCENDING);

        List<IndexColumn> wrappedKeyCols = H2Utils.treeIndexColumns(rowDescriptor(),
            new ArrayList<>(2), keyCol, affKeyCol);

        //explicit add HASH index, due to we know all their parameters and it doesn't created on non afinity nodes.
        res.add(
            new IndexInformation(false,
                true, PK_HASH_IDX_NAME,
                H2IndexType.HASH,
                H2Utils.indexColumnsSql(H2Utils.unwrapKeyColumns(this, wrappedKeyCols.toArray(H2Utils.EMPTY_COLUMNS))),
            null));

        //explicit add SCAN index, due to we know all their parameters and it depends on affinity node or not.
        res.add(new IndexInformation(false, false, SCAN_INDEX_NAME_SUFFIX, H2IndexType.SCAN, null, null));

        for (Index idx : idxs) {
            if (idx instanceof H2TreeIndexBase) {
                res.add(new IndexInformation(
                    idx.getIndexType().isPrimaryKey(),
                    idx.getIndexType().isUnique(),
                    idx.getName(),
                    H2IndexType.BTREE,
                    H2Utils.indexColumnsSql(H2Utils.unwrapKeyColumns(this, idx.getIndexColumns())),
                    ((H2TreeIndexBase)idx).inlineSize()
                ));
            }
            else if (idx.getIndexType().isSpatial()) {
                res.add(
                    new IndexInformation(
                        false,
                        false,
                        idx.getName(),
                        H2IndexType.SPATIAL,
                        H2Utils.indexColumnsSql(idx.getIndexColumns()),
                        null)
                );
            }
        }

        return res;
    }

    /**
     * Calculate affinity key column which will be used for partition pruning and distributed joins.
     *
     * @return Affinity column or {@code null} if none can be used.
     */
    private IndexColumn calculateAffinityKeyColumn() {
        // If custome affinity key mapper is set, we do not know how to convert _KEY to partition, return null.
        if (desc.type().customAffinityKeyMapper())
            return null;

        String affKeyFieldName = desc.type().affinityKey();

        // If explicit affinity key field is not set, then use _KEY.
        if (affKeyFieldName == null)
            return indexColumn(QueryUtils.KEY_COL, SortOrder.ASCENDING);

        // If explicit affinity key field is set, but is not found in the table, do not use anything.
        if (!doesColumnExist(affKeyFieldName))
            return null;

        int colId = getColumn(affKeyFieldName).getColumnId();

        // If affinity key column is either _KEY or it's alias (QueryEntity.keyFieldName), normalize it to _KEY.
        if (desc.isKeyColumn(colId))
            return indexColumn(QueryUtils.KEY_COL, SortOrder.ASCENDING);

        // Otherwise use column as is.
        return indexColumn(colId, SortOrder.ASCENDING);
    }

    /**
     * @return {@code true} If this is a partitioned table.
     */
    public boolean isPartitioned() {
        return desc != null && desc.cacheInfo().config().getCacheMode() == PARTITIONED;
    }

    /**
     * @return Affinity key column or {@code null} if not available.
     */
    @Nullable public IndexColumn getAffinityKeyColumn() {
        return affKeyCol;
    }

    /**
     * @return Explicit affinity key column or {@code null} if not available (skip _KEY column or it's alias).
     */
    @Nullable public IndexColumn getExplicitAffinityKeyColumn() {
        // Only explicit affinity column should be shown. Do not do this for _KEY or it's alias.
        if (affKeyCol == null || affKeyColIsKey)
            return null;

        return affKeyCol;
    }

    /**
     * Check whether passed column can be used for partition pruning.
     *
     * @param col Column.
     * @return {@code True} if affinity key column.
     */
    public boolean isColumnForPartitionPruning(Column col) {
        return isColumnForPartitionPruning0(col, false);
    }

    /**
     * Check whether passed column could be used for partition transfer during partition pruning on joined tables and
     * for external affinity calculation (e.g. on thin clients).
     * <p>
     * Note that it is different from {@link #isColumnForPartitionPruning(Column)} method in that not every column
     * which qualifies for partition pruning can be used by thin clients or join partition pruning logic.
     * <p>
     * Consider the following schema:
     * <pre>
     * CREATE TABLE dept (id PRIMARY KEY);
     * CREATE TABLE emp (id, dept_id AFFINITY KEY, PRIMARY KEY(id, dept_id));
     * </pre>
     * For expression-based partition pruning on "emp" table on the <b>server side</b> we may use both "_KEY" and
     * "dept_id" columns, as passing them through standard affinity workflow will yield the same result:
     * dept_id -> part
     * _KEY -> dept_id -> part
     * <p>
     * But we cannot use "_KEY" on thin client side, as it doesn't know how to extract affinity key field properly.
     * Neither we can perform partition transfer in JOINs when "_KEY" is used.
     * <p>
     * This is OK as data is collocated, so we can merge partitions extracted from both tables:
     * <pre>
     * SELECT * FROM dept d INNER JOIN emp e ON d.id = e.dept_id WHERE e.dept_id=? AND d.id=?
     * </pre>
     * But this is not OK as joined data is not collocated, and tables form distinct collocation groups:
     * <pre>
     * SELECT * FROM dept d INNER JOIN emp e ON d.id = e._KEY WHERE e.dept_id=? AND d.id=?
     * </pre>
     * NB: The last query is not logically correct and will produce empty result. However, it is correct from SQL
     * perspective, so we should make incorrect assumptions about partitions as it may make situation even worse.
     *
     * @param col Column.
     * @return {@code True} if column could be used for partition extraction on both server and client sides and for
     *     partition transfer in joins.
     */
    public boolean isColumnForPartitionPruningStrict(Column col) {
        return isColumnForPartitionPruning0(col, true);
    }

    /**
     * Internal logic to check whether column qualifies for partition extraction or not.
     *
     * @param col Column.
     * @param strict Strict flag.
     * @return {@code True} if column could be used for partition.
     */
    private boolean isColumnForPartitionPruning0(Column col, boolean strict) {
        if (affKeyCol == null)
            return false;

        int colId = col.getColumnId();

        if (colId == affKeyCol.column.getColumnId())
            return true;

        return (affKeyColIsKey || !strict) && desc.isKeyColumn(colId);
    }

    /**
     * @return Whether custom affintiy mapper is used.
     */
    public boolean isCustomAffinityMapper() {
        return desc.type().customAffinityKeyMapper();
    }

    /** {@inheritDoc} */
    @Override public long getDiskSpaceUsed() {
        return 0;
    }

    /**
     * @return Row descriptor.
     */
    public GridH2RowDescriptor rowDescriptor() {
        return desc;
    }

    /**
     * @return Cache name.
     */
    public String cacheName() {
        return cacheInfo.name();
    }

    /**
     * @return Cache ID.
     */
    public int cacheId() {
        return cacheInfo.cacheId();
    }

    /**
     * @return Cache context info.
     */
    public GridCacheContextInfo cacheInfo() {
        return cacheInfo;
    }

    /**
     * @return {@code true} If Cache is lazy (not full inited).
     */
    public boolean isCacheLazy() {
        return cacheInfo.cacheContext() == null;
    }

    /**
     * Get actual table statistics if exists.
     *
     * @return Table statistics or {@code null} if there is no statistics available.
     */
    public ObjectStatistics tableStatistics() {
        GridCacheContext cacheContext = cacheInfo.cacheContext();

        if (cacheContext == null)
            return null;

        IgniteH2Indexing indexing = (IgniteH2Indexing)cacheContext.kernalContext().query().getIndexing();

        return indexing.statsManager().getLocalStatistics(new StatisticsKey(identifier.schema(), identifier.table()));
    }

    /**
     * @return Cache context.
     */
    @Nullable public GridCacheContext cacheContext() {
        return cacheInfo.cacheContext();
    }

    /** {@inheritDoc} */
    @Override public boolean lock(Session ses, boolean exclusive, boolean force) {
        // In accordance with base method semantics, we'll return true if we were already exclusively locked.
        SessionLock sesLock = sessions.get(ses);

        if (sesLock != null) {
            if (sesLock.isExclusive())
                return true;

            if (ver.get() != sesLock.version())
                throw new QueryRetryException(getName());

            return false;
        }

        // Acquire the lock.
        lock(exclusive, true);

        if (destroyed) {
            unlock(exclusive);

            throw new IllegalStateException("Table " + identifierString() + " already destroyed.");
        }

        // Mutate state.
        sessions.put(ses, exclusive ? SessionLock.exclusiveLock() : SessionLock.sharedLock(ver.longValue()));

        ses.addLock(this);

        return false;
    }

    /** {@inheritDoc} */
    @Override public void unlock(Session ses) {
        SessionLock sesLock = sessions.remove(ses);

        if (sesLock == null) {
            log.warning("Requested session ses=[" + ses + "] was already removed from active sessions list.");

            return;
        }

        if (sesLock.locked)
            unlock(sesLock.isExclusive());
    }

    /**
     * @param ses H2 session.
     */
    private void readLockInternal(Session ses) {
        SessionLock sesLock = sessions.get(ses);

        assert sesLock != null && !sesLock.isExclusive()
            : "Invalid table lock [name=" + getName() + ", lock=" + sesLock == null ? "null" : sesLock.ver + ']';

        if (!sesLock.locked) {
            lock(false);

            sesLock.locked = true;
        }
    }

    /**
     * Release table lock.
     *
     * @param ses H2 session.
     */
    private void unlockReadInternal(Session ses) {
        SessionLock sesLock = sessions.get(ses);

        assert sesLock != null && !sesLock.isExclusive()
            : "Invalid table unlock [name=" + getName() + ", lock=" + sesLock == null ? "null" : sesLock.ver + ']';

        if (sesLock.locked) {
            sesLock.locked = false;

            unlock(false);
        }
    }

    /**
     * Acquire table lock.
     *
     * @param exclusive Exclusive flag.
     */
    private void lock(boolean exclusive) {
        lock(exclusive, false);
    }

    /**
     * Acquire table lock.
     *
     * @param exclusive Exclusive flag.
     * @param interruptibly Acquires interruptibly lock or not interruplible lock flag.
     */
    @SuppressWarnings({"LockAcquiredButNotSafelyReleased", "CallToThreadYield"})
    private void lock(boolean exclusive, boolean interruptibly) {
        Lock l = exclusive ? lock.writeLock() : lock.readLock();

        try {
            if (!exclusive) {
                if (interruptibly)
                    l.lockInterruptibly();
                else
                    l.lock();
            }
            else {
                for (;;) {
                    if (l.tryLock(200, TimeUnit.MILLISECONDS))
                        break;
                    else
                        Thread.yield();
                }

                ver.incrementAndGet();
            }
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();

            throw new IgniteInterruptedException("Thread got interrupted while trying to acquire table lock.", e);
        }
    }

    /**
     * Release table lock.
     *
     * @param exclusive Exclusive flag.
     */
    private void unlock(boolean exclusive) {
        Lock l = exclusive ? lock.writeLock() : lock.readLock();

        l.unlock();
    }

    /**
     * @param ses H2 session.
     */
    private void checkVersion(Session ses) {
        SessionLock sesLock = sessions.get(ses);

        assert sesLock != null && !sesLock.isExclusive()
            : "Invalid table check version  [name=" + getName() + ", lock=" + sesLock.ver + ']';

        if (ver.longValue() != sesLock.version())
            throw new QueryRetryException(getName());
    }

    /**
     * @return Table identifier.
     */
    public QueryTable identifier() {
        return identifier;
    }

    /**
     * @return Table identifier as string.
     */
    public String identifierString() {
        return identifierStr;
    }

    /**
     * Check if table is not destroyed.
     */
    private void ensureNotDestroyed() {
        if (destroyed)
            throw new IllegalStateException("Table " + identifierString() + " already destroyed.");
    }

    /** {@inheritDoc} */
    @Override public void close(Session ses) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void removeChildrenAndResources(Session ses) {
        lock(true);

        try {
            super.removeChildrenAndResources(ses);

            // Clear all user indexes registered in schema.
            while (idxs.size() > sysIdxsCnt) {
                Index idx = idxs.get(sysIdxsCnt);

                if (idx.getName() != null && idx.getSchema().findIndex(ses, idx.getName()) == idx) {
                    // This call implicitly removes both idx and its proxy, if any, from idxs.
                    database.removeSchemaObject(ses, idx);

                    // We have to call destroy here if we are who has removed this index from the table.
                    if (idx instanceof GridH2IndexBase)
                        ((GridH2IndexBase)idx).destroy(rmIndex);
                }
            }

            if (SysProperties.CHECK) {
                for (SchemaObject obj : database.getAllSchemaObjects(DbObject.INDEX)) {
                    Index idx = (Index) obj;
                    if (idx.getTable() == this)
                        DbException.throwInternalError("index not dropped: " + idx.getName());
                }
            }

            database.removeMeta(ses, getId());
            invalidate();
        }
        finally {
            unlock(true);
        }
    }

    /**
     * Destroy the table.
     */
    public void destroy() {
        lock(true);

        try {
            ensureNotDestroyed();

            destroyed = true;

            if (shadowedAffIndex != null)
                shadowedAffIndex.destroy(rmIndex);

            for (int i = 1, len = idxs.size(); i < len; i++)
                if (idxs.get(i) instanceof GridH2IndexBase)
                    index(i).destroy(rmIndex);
        }
        finally {
            unlock(true);
        }
    }

    /**
     * If flag {@code True}, index will be destroyed when table {@link #destroy()}.
     *
     * @param rmIndex Flag indicate remove index on destroy or not.
     */
    public void setRemoveIndexOnDestroy(boolean rmIndex) {
        this.rmIndex = rmIndex;
    }

    /**
     * Gets index by index.
     *
     * @param idx Index in list.
     * @return Index.
     */
    private GridH2IndexBase index(int idx) {
        return (GridH2IndexBase)idxs.get(idx);
    }

    /**
     * Gets primary key.
     *
     * @return Primary key.
     */
    private GridH2IndexBase pk() {
        return (GridH2IndexBase)idxs.get(2);
    }

    /**
     * Updates table for given key. If value is null then row with given key will be removed from table,
     * otherwise value and expiration time will be updated or new row will be added.
     *
     * @param row Row to be updated.
     * @param prevRow Previous row.
     * @param prevRowAvailable Whether previous row is available.
     * @throws IgniteCheckedException If failed.
     */
    public void update(CacheDataRow row, @Nullable CacheDataRow prevRow, boolean prevRowAvailable) throws IgniteCheckedException {
        assert desc != null;

        H2CacheRow row0 = desc.createRow(row);
        H2CacheRow prevRow0 = prevRow != null ? desc.createRow(prevRow) : null;

        row0.prepareValuesCache();

        if (prevRow0 != null)
            prevRow0.prepareValuesCache();

        IgniteCheckedException err = null;

        try {
            lock(false);

            try {
                ensureNotDestroyed();

                boolean replaced;

                if (prevRowAvailable && rebuildFromHashInProgress == FALSE)
                    replaced = pk().putx(row0);
                else {
                    prevRow0 = pk().put(row0);

                    replaced = prevRow0 != null;
                }

                if (!replaced)
                    size.increment();

                for (int i = pkIndexPos + 1, len = idxs.size(); i < len; i++) {
                    Index idx = idxs.get(i);

                    if (idx instanceof GridH2IndexBase)
                        err = addToIndex((GridH2IndexBase)idx, row0, prevRow0, err);

                }

                if (!tmpIdxs.isEmpty()) {
                    for (GridH2IndexBase idx : tmpIdxs.values())
                        err = addToIndex(idx, row0, prevRow0, err);
                }
            }
            finally {
                unlock(false);
            }
        }
        finally {
            updateStatistics(row0.key());

            row0.clearValuesCache();

            if (prevRow0 != null)
                prevRow0.clearValuesCache();
        }

        if (err != null)
            throw err;
    }

    /**
     * Remove row.
     *
     * @param row Row.
     * @return {@code True} if was removed.
     * @throws IgniteCheckedException If failed.
     */
    public boolean remove(CacheDataRow row) throws IgniteCheckedException {
        H2CacheRow row0 = desc.createRow(row);

        boolean res = false;

        lock(false);

        try {
            ensureNotDestroyed();

            boolean pkRmv = pk().removex(row0);

            for (int i = pkIndexPos + 1, len = idxs.size(); i < len; i++) {
                Index idx = idxs.get(i);

                if (idx instanceof GridH2IndexBase) {
                    boolean scndRmv = ((GridH2IndexBase)idx).removex(row0);

                    if (scndRmv != pkRmv) {
                        String rowKeyHex = null;
                        String rowValueHex = null;

                        GridToStringBuilder.SensitiveDataLogging sensitiveDataLogging = getSensitiveDataLogging();

                        switch (sensitiveDataLogging) {
                            case PLAIN: {
                                byte[] rowBytes = rowKeyBytes(row0);
                                rowKeyHex = rowBytes != null ? "0x" + byteArray2HexString(rowBytes) : null;

                                rowBytes = rowValueBytes(row0);
                                rowValueHex = rowBytes != null ? "0x" + byteArray2HexString(rowBytes) : null;
                                break;
                            }
                            case HASH: {
                                byte[] rowBytes = rowKeyBytes(row0);
                                if (rowBytes != null)
                                    rowKeyHex = "0x" + toHexString(Arrays.hashCode(rowBytes)).toUpperCase();

                                rowBytes = rowValueBytes(row0);
                                if (rowBytes != null)
                                    rowValueHex = "0x" + toHexString(Arrays.hashCode(rowBytes)).toUpperCase();

                                break;
                            }
                            case NONE: {
                                rowKeyHex = "hidden data";
                                rowValueHex = "hidden data";
                                break;
                            }
                        }

                        log.warning(
                            "SQL index inconsistency detected:" + nl() +
                            "wasInPk=" + pkRmv + ',' + nl() +
                            "wasInSecIdx=" + scndRmv + ',' + nl() +
                            "tblName=" + getName() + ',' + nl() +
                            "secIdxName=" + idx.getName() + ',' + nl() +
                            "row=" + row0 + ',' + nl() +
                            "rowKeyHex=" + rowKeyHex + ',' + nl() +
                            "rowValueHex=" + rowValueHex + ',' + nl() +
                            "sensitiveDataLoggingMode=" + sensitiveDataLogging
                        );
                    }
                }
            }

            if (!tmpIdxs.isEmpty()) {
                for (GridH2IndexBase idx : tmpIdxs.values())
                    idx.removex(row0);
            }

            if (pkRmv)
                size.decrement();

            res = pkRmv;
        }
        finally {
            unlock(false);
        }

        updateStatistics(row0.key());

        return res;
    }

    /**
     * Add row to index.
     * @param idx Index to add row to.
     * @param row Row to add to index.
     * @param prevRow Previous row state, if any.
     * @param err Error on index add
     */
    private IgniteCheckedException addToIndex(
        GridH2IndexBase idx,
        H2CacheRow row,
        H2CacheRow prevRow,
        IgniteCheckedException err
    ) {
        try {
            boolean replaced = idx.putx(row);

            // Row was not replaced, need to remove manually.
            if (!replaced && prevRow != null)
                idx.removex(prevRow);

            return err;
        }
        catch (Throwable t) {
            IgniteSQLException ex = X.cause(t, IgniteSQLException.class);

            if (ex != null && ex.statusCode() == IgniteQueryErrorCode.FIELD_TYPE_MISMATCH) {
                if (err != null) {
                    err.addSuppressed(t);

                    return err;
                }
                else
                    return new IgniteCheckedException("Error on add row to index '" + getName() + '\'', t);
            }
            else
                throw t;
        }
    }

    /**
     * Collect indexes for rebuild.
     *
     * @param clo Closure.
     * @param force Force rebuild indexes.
     */
    public void collectIndexesForPartialRebuild(IndexRebuildPartialClosure clo, boolean force) {
        for (int i = 0; i < idxs.size(); i++) {
            Index idx = idxs.get(i);

            if (idx instanceof H2TreeIndex) {
                H2TreeIndex idx0 = (H2TreeIndex)idx;

                if (force || idx0.rebuildRequired())
                    clo.addIndex(this, idx0);
            }
        }
    }

    /** */
    public boolean checkIfIndexesRebuildRequired() {
        for (int i = 0; i < idxs.size(); i++) {
            Index idx = idxs.get(i);

            if (idx instanceof H2TreeIndex) {
                H2TreeIndex idx0 = (H2TreeIndex)idx;

                if (idx0.rebuildRequired())
                    return true;
            }
        }

        return false;
    }

    /**
     * Mark or unmark index rebuild state.
     */
    public void markRebuildFromHashInProgress(boolean value) {
        assert !value || (idxs.size() >= 2 && index(1).getIndexType().isHash()) : "Table has no hash index.";

        if (rebuildFromHashInProgressFieldUpdater.compareAndSet(this, value ? FALSE : TRUE, value ? TRUE : FALSE)) {
            lock.writeLock().lock();

            try {
                incrementModificationCounter();
            }
            finally {
                lock.writeLock().unlock();
            }
        }
    }

    /**
     *
     */
    public boolean rebuildFromHashInProgress() {
        return rebuildFromHashInProgress == TRUE;
    }

    /** {@inheritDoc} */
    @Override public Index addIndex(Session ses, String idxName, int idxId, IndexColumn[] cols, IndexType idxType,
        boolean create, String idxComment) {
        return commitUserIndex(ses, idxName);
    }

    /**
     * Checks that equivalent fields collection index already present.
     *
     * @param curIdx Index to check.
     */
    private void checkEquivalentFieldsIndexIsPresent(Index curIdx) {
        IndexColumn[] curColumns = curIdx.getIndexColumns();

        Index registredIdx = null;

        for (Index idx : idxs) {
            if (!(idx instanceof H2TreeIndex))
                continue;

            IndexColumn[] idxColumns = idx.getIndexColumns();

            for (int i = 0; i < Math.min(idxColumns.length, curColumns.length); ++i) {
                IndexColumn idxCol = idxColumns[i];
                IndexColumn curCol = curColumns[i];

                // pk attach at the end of listed fields.
                if (curCol.column.getColumnId() == 0 && registredIdx != null)
                    continue;

                if (H2Utils.equals(idxCol, curCol) && idxCol.sortType == curCol.sortType)
                    registredIdx = idx;
                else {
                    registredIdx = null;

                    break;
                }
            }

            if (registredIdx != null) {
                String idxCols = Stream.of(registredIdx.getIndexColumns())
                    .map(k -> k.columnName).collect(Collectors.joining(", "));

                U.warn(log, "Index with the given set or subset of columns already exists " +
                    "(consider dropping either new or existing index) [cacheName=" + cacheInfo.name() + ", " +
                    "schemaName=" + getSchema().getName() + ", tableName=" + getName() +
                    ", newIndexName=" + curIdx.getName() + ", existingIndexName=" + registredIdx.getName() +
                    ", existingIndexColumns=[" + idxCols + "]]");
            }
        }
    }

    /**
     * Add index that is in an intermediate state and is still being built, thus is not used in queries until it is
     * promoted.
     *
     * @param idx Index to add.
     * @throws IgniteCheckedException If failed.
     */
    public void proposeUserIndex(Index idx) throws IgniteCheckedException {
        assert idx instanceof GridH2IndexBase;

        lock(true);

        try {
            ensureNotDestroyed();

            for (Index idx0 : idxs) {
                if (F.eq(idx.getName(), idx0.getName()))
                    throw new SchemaOperationException(CODE_INDEX_EXISTS, idx.getName());
            }

            checkEquivalentFieldsIndexIsPresent(idx);

            Index oldTmpIdx = tmpIdxs.put(idx.getName(), (GridH2IndexBase)idx);

            assert oldTmpIdx == null;
        }
        finally {
            unlock(true);
        }
    }

    /**
     * Promote temporary index to make it usable in queries.
     *
     * @param ses H2 session.
     * @param idxName Index name.
     * @return Temporary index with given name.
     */
    private Index commitUserIndex(Session ses, String idxName) {
        lock(true);

        try {
            ensureNotDestroyed();

            Index idx = tmpIdxs.remove(idxName);

            assert idx != null;

            Index cloneIdx = createDuplicateIndexIfNeeded(idx);

            ArrayList<Index> newIdxs = new ArrayList<>(
                idxs.size() + ((cloneIdx == null) ? 1 : 2));

            newIdxs.addAll(idxs);

            newIdxs.add(idx);

            if (cloneIdx != null)
                newIdxs.add(cloneIdx);

            idxs = newIdxs;

            database.addSchemaObject(ses, idx);

            if (cloneIdx != null)
                database.addSchemaObject(ses, cloneIdx);

            incrementModificationCounter();

            return idx;
        }
        finally {
            unlock(true);
        }
    }

    /**
     * Remove user index without promoting it.
     *
     * @param idxName Index name.
     */
    public void rollbackUserIndex(String idxName) {
        lock(true);

        try {
            ensureNotDestroyed();

            GridH2IndexBase rmvIdx = tmpIdxs.remove(idxName);

            assert rmvIdx != null;

            rmvIdx.destroy(true);
        }
        finally {
            unlock(true);
        }
    }

    /**
     * Get user index with provided name.
     *
     * @param idxName Index name.
     * @return User index if exists and {@code null} othwerwise.
     */
    @Nullable public Index userIndex(String idxName) {
        for (int i = 2; i < idxs.size(); i++) {
            Index idx = idxs.get(i);

            if (idx.getName().equalsIgnoreCase(idxName))
                return idx;
        }

        return null;
    }

    /** {@inheritDoc} */
    @Override public void removeIndex(Index h2Idx) {
        throw DbException.getUnsupportedException("must use removeIndex(session, idx)");
    }

    /**
     * Remove the given index from the list.
     *
     * @param h2Idx the index to remove
     */
    public void removeIndex(Session session, Index h2Idx) {
        lock(true);

        try {
            ensureNotDestroyed();

            ArrayList<Index> idxs = new ArrayList<>(this.idxs);

            Index targetIdx = (h2Idx instanceof GridH2ProxyIndex) ?
                ((GridH2ProxyIndex)h2Idx).underlyingIndex() : h2Idx;

            for (int i = pkIndexPos; i < idxs.size();) {
                Index idx = idxs.get(i);

                if (idx == targetIdx || (idx instanceof GridH2ProxyIndex &&
                    ((GridH2ProxyIndex)idx).underlyingIndex() == targetIdx)) {

                    Index idx0 = idxs.remove(i);

                    if (idx0 instanceof GridH2ProxyIndex &&
                        idx.getSchema().findIndex(session, idx.getName()) != null)
                        database.removeSchemaObject(session, idx);

                    GridCacheContext cctx0 = cacheInfo.cacheContext();

                    if (cctx0 != null && idx0 instanceof GridH2IndexBase)
                        ((GridH2IndexBase)idx0).destroy(rmIndex);

                    continue;
                }

                i++;
            }

            this.idxs = idxs;
        }
        finally {
            unlock(true);
        }
    }

    /** {@inheritDoc} */
    @Override public void removeRow(Session ses, Row row) {
        throw DbException.getUnsupportedException("removeRow");
    }

    /** {@inheritDoc} */
    @Override public void truncate(Session ses) {
        throw DbException.getUnsupportedException("truncate");
    }

    /** {@inheritDoc} */
    @Override public void addRow(Session ses, Row row) {
        throw DbException.getUnsupportedException("addRow");
    }

    /** {@inheritDoc} */
    @Override public void checkSupportAlter() {
        throw DbException.getUnsupportedException("alter");
    }

    /** {@inheritDoc} */
    @Override public TableType getTableType() {
        return TableType.TABLE;
    }

    /** {@inheritDoc} */
    @Override public Index getScanIndex(Session ses) {
        return getIndexes().get(0); // Scan must be always first index.
    }

    /** {@inheritDoc} */
    @Override public Index getUniqueIndex() {
        if (rebuildFromHashInProgress == TRUE)
            return index(1);
        else
            return index(2);
    }

    /** {@inheritDoc} */
    @Override public ArrayList<Index> getIndexes() {
        if (rebuildFromHashInProgress == FALSE)
            return idxs;

        ArrayList<Index> idxs = new ArrayList<>(2);

        idxs.add(this.idxs.get(0));

        if (hasHashIndex)
            idxs.add(this.idxs.get(1));

        return idxs;
    }

    /** {@inheritDoc} */
    @Override public boolean isLockedExclusively() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean isLockedExclusivelyBy(Session ses) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public long getMaxDataModificationId() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public boolean isDeterministic() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean canGetRowCount() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean canDrop() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public long getRowCount(@Nullable Session ses) {
        return getUniqueIndex().getRowCount(ses);
    }

    /** {@inheritDoc} */
    @Override public long getRowCountApproximation(Session ses) {
        if (!localQuery(H2Utils.context(ses)))
            return 10_000; // Fallback to the previous behaviour.

        refreshStatsIfNeeded();

        return tblStats.primaryRowCount();
    }

    /**
     * Destroys the old data and recreate the index.
     *
     * @param session Session.
     * @throws IgniteCheckedException In case we were unable to destroy the data.
     */
    public void prepareIndexesForRebuild(Session session) throws IgniteCheckedException {
        lock(true);

        try {
            ArrayList<Index> newIdxs = new ArrayList<>(idxs.size());

            for (int i = 0; i < sysIdxsCnt; i++) {
                Index idx = idxs.get(i);

                if (idx instanceof GridH2ProxyIndex)
                    break;

                Index newIdx = idx instanceof H2TreeIndex ? recreateIndex((H2TreeIndex) idx) : idx;

                newIdxs.add(newIdx);
            }

            for (int i = 1; i < sysIdxsCnt; i++) {
                Index clone = createDuplicateIndexIfNeeded(newIdxs.get(i));

                if (clone != null)
                    newIdxs.add(clone);
            }

            List<IgniteBiTuple<Index, Index>> toReplace = new ArrayList<>();

            for (int i = sysIdxsCnt; i < idxs.size(); i++) {
                Index idx = idxs.get(i);

                if (idx instanceof GridH2ProxyIndex)
                    continue;

                if (idx instanceof H2TreeIndex) {
                    Index newIdx = recreateIndex((H2TreeIndex) idx);

                    newIdxs.add(newIdx);

                    toReplace.add(new IgniteBiTuple<>(idx, newIdx));

                    Index clone = createDuplicateIndexIfNeeded(newIdx);

                    if (clone != null) {
                        newIdxs.add(clone);

                        toReplace.add(new IgniteBiTuple<>(null, clone));
                    }
                }
                else {
                    newIdxs.add(idx);

                    if (idxs.get(i + 1) instanceof GridH2ProxyIndex)
                        newIdxs.add(idxs.get(++i));
                }
            }

            for (IgniteBiTuple<Index, Index> oldToNew : toReplace)
                replaceSchemaObject(session, oldToNew.get1(), oldToNew.get2());

            if (hasHashIndex)
                newIdxs.set(0, new H2TableScanIndex(this, (GridH2IndexBase) newIdxs.get(2), (GridH2IndexBase) newIdxs.get(1)));
            else
                newIdxs.set(0, new H2TableScanIndex(this, (GridH2IndexBase) newIdxs.get(1), null));

            idxs = newIdxs;

            incrementModificationCounter();
        }
        finally {
            unlock(true);
        }
    }

    /** */
    private H2TreeIndex recreateIndex(H2TreeIndex treeIdx) throws IgniteCheckedException {
        treeIdx.destroy0(true, true);

        GridCacheContext<?, ?> cctx = cacheContext();

        assert cctx != null;

        return treeIdx.createCopy(cctx.dataRegion().pageMemory(), cctx.offheap());
    }

    /**
     * Replaces the object in the schema managed by H2.
     *
     * <p>Invocation of the method should be guarded by exclusive lock.
     * @param session Session.
     * @param oldObj Object to remove. Do nothing if null.
     * @param newObj Object to add. Do nothing if null.
     * @see #lock(boolean)
     */
    private void replaceSchemaObject(
            Session session,
            @Nullable SchemaObject oldObj,
            @Nullable SchemaObject newObj
    ) {
        assert lock.writeLock().isHeldByCurrentThread() : lock.writeLock();

        if (oldObj != null)
            database.removeSchemaObject(session, oldObj);

        if (newObj != null)
            database.addSchemaObject(session, newObj);
    }

    /**
     * @param qctx Context.
     *
     * @return {@code True} if the current query is a local query.
     */
    private boolean localQuery(QueryContext qctx) {
        assert qctx != null;

        return qctx.local();
    }

    /**
     * Refreshes table stats if they are outdated.
     */
    private void refreshStatsIfNeeded() {
        TableStatistics stats = tblStats;

        long statsTotalRowCnt = stats.totalRowCount();
        long curTotalRowCnt = size.sum();

        // Update stats if total table size changed significantly since the last stats update.
        if (needRefreshStats(statsTotalRowCnt, curTotalRowCnt) && cacheInfo.affinityNode()) {
            CacheConfiguration ccfg = cacheContext().config();

            int backups = ccfg.getCacheMode() == CacheMode.REPLICATED ? 0 : cacheContext().config().getBackups();

            // After restart of node with persistence and before affinity exchange - PRIMARY partitions are empty.
            // Assume that data divides equally between PRIMARY and BACKUP.
            long localOwnerRowCnt = cacheSize(CachePeekMode.PRIMARY, CachePeekMode.BACKUP) / (backups + 1);

            int owners = cacheContext().discovery().cacheNodes(cacheContext().name(), NONE).size();

            long totalRowCnt = owners * localOwnerRowCnt;

            size.reset();
            size.add(totalRowCnt);

            tblStats = new TableStatistics(totalRowCnt, localOwnerRowCnt);
        }
    }

    /**
     * @param statsRowCnt Row count from statistics.
     * @param actualRowCnt Actual row count.
     * @return {@code True} if actual table size has changed more than the threshold since last stats update.
     */
    private static boolean needRefreshStats(long statsRowCnt, long actualRowCnt) {
        double delta = U.safeAbs(statsRowCnt - actualRowCnt);

        double relativeChange = delta / (statsRowCnt + 1); // Add 1 to avoid division by zero.

        // Return true if an actual table size has changed more than the threshold since the last stats update.
        return relativeChange > STATS_UPDATE_THRESHOLD;
    }

    /**
     * Retrieves partitions size.
     *
     * @return Rows count.
     */
    private long cacheSize(CachePeekMode... modes) {
        try {
            return cacheInfo.cacheContext().cache().localSizeLong(modes);
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }

    /**
     * @return Backup filter for the current topology.
     */
    @Nullable private IndexingQueryCacheFilter backupFilter() {
        IgniteH2Indexing indexing = rowDescriptor().indexing();

        AffinityTopologyVersion topVer = indexing.readyTopologyVersion();

        IndexingQueryFilter filter = indexing.backupFilter(topVer, null);

        return filter.forCache(cacheName());
    }

    /** {@inheritDoc} */
    @Override public void checkRename() {
        throw DbException.getUnsupportedException("rename");
    }

    /**
     * Creates index column for table.
     *
     * @param col Column index.
     * @param sorting Sorting order {@link SortOrder}
     * @return Created index column.
     */
    public IndexColumn indexColumn(int col, int sorting) {
        IndexColumn res = new IndexColumn();

        res.column = getColumn(col);
        res.columnName = res.column.getName();
        res.sortType = sorting;

        return res;
    }

    /**
     * Update key statistics.
     *
     * @param key Updated key.
     */
    private void updateStatistics(KeyCacheObject key) {
        GridCacheContext cacheCtx = cacheInfo.cacheContext();
        if (cacheCtx == null)
            return;

        IgniteH2Indexing indexing = (IgniteH2Indexing)cacheCtx.kernalContext().query().getIndexing();
        try {
            indexing.statsManager().onRowUpdated(this.identifier().schema(),
                this.identifier.table(), key.partition(), key.valueBytes(this.cacheContext().cacheObjectContext()));
        }
        catch (IgniteCheckedException e) {
            if (log.isDebugEnabled())
                log.debug("Error while updating statistics obsolescence due to " + e.getMessage());
        }
    }

    /**
     * Creates proxy index for given target index.
     * Proxy index refers to alternative key and val columns.
     *
     * @param target Index to clone.
     * @return Proxy index.
     */
    private Index createDuplicateIndexIfNeeded(Index target) {
        if (!(target instanceof H2TreeIndexBase) && !(target instanceof SpatialIndex))
            return null;

        IndexColumn[] cols = target.getIndexColumns();

        List<IndexColumn> proxyCols = new ArrayList<>(cols.length);

        boolean modified = false;

        for (IndexColumn col : cols) {
            IndexColumn proxyCol = new IndexColumn();

            proxyCol.columnName = col.columnName;
            proxyCol.column = col.column;
            proxyCol.sortType = col.sortType;

            int altColId = desc.getAlternativeColumnId(proxyCol.column.getColumnId());

            if (altColId != proxyCol.column.getColumnId()) {
                proxyCol.column = getColumn(altColId);

                proxyCol.columnName = proxyCol.column.getName();

                modified = true;
            }

            proxyCols.add(proxyCol);
        }

        if (modified) {
            String proxyName = target.getName() + "_proxy";

            if (target.getIndexType().isSpatial())
                return new GridH2ProxySpatialIndex(this, proxyName, proxyCols, target);

            return new GridH2ProxyIndex(this, proxyName, proxyCols, target);
        }

        return null;
    }

    /**
     * Add new columns to this table.
     *
     * @param cols Columns to add.
     * @param ifNotExists Ignore this command if {@code cols} has size of 1 and column with given name already exists.
     */
    public void addColumns(List<QueryField> cols, boolean ifNotExists) {
        assert !ifNotExists || cols.size() == 1;

        lock(true);

        try {
            Column[] safeColumns0 = safeColumns;

            int pos = safeColumns0.length;

            Column[] newCols = new Column[safeColumns0.length + cols.size()];

            // First, let's copy existing columns to new array
            System.arraycopy(safeColumns0, 0, newCols, 0, safeColumns0.length);

            // And now, let's add new columns
            for (QueryField col : cols) {
                if (doesColumnExist(col.name())) {
                    if (ifNotExists && cols.size() == 1)
                        return;
                    else
                        throw new IgniteSQLException("Column already exists [tblName=" + getName() +
                            ", colName=" + col.name() + ']');
                }

                try {
                    Column c = new Column(col.name(), H2Utils.getTypeFromClass(Class.forName(col.typeName())));

                    c.setNullable(col.isNullable());

                    newCols[pos++] = c;
                }
                catch (ClassNotFoundException e) {
                    throw new IgniteSQLException("H2 data type not found for class: " + col.typeName(), e);
                }
            }

            setColumns(newCols);

            desc.refreshMetadataFromTypeDescriptor();

            incrementModificationCounter();
        }
        finally {
            unlock(true);
        }
    }

    /**
     * Drop columns.
     *
     * @param cols Columns.
     * @param ifExists If EXISTS flag.
     */
    @SuppressWarnings("ForLoopReplaceableByForEach")
    public void dropColumns(List<String> cols, boolean ifExists) {
        assert !ifExists || cols.size() == 1;

        lock(true);

        try {
            Column[] safeColumns0 = safeColumns;

            int size = safeColumns0.length;

            for (String name : cols) {
                if (!doesColumnExist(name)) {
                    if (ifExists && cols.size() == 1)
                        return;
                    else
                        throw new IgniteSQLException("Column does not exist [tblName=" + getName() +
                            ", colName=" + name + ']');
                }

                size--;
            }

            assert size > QueryUtils.DEFAULT_COLUMNS_COUNT;

            Column[] newCols = new Column[size];

            int dst = 0;

            for (int i = 0; i < safeColumns0.length; i++) {
                Column column = safeColumns0[i];

                for (String name : cols) {
                    if (F.eq(name, column.getName())) {
                        column = null;

                        break;
                    }
                }

                if (column != null)
                    newCols[dst++] = column;
            }

            setColumns(newCols);

            desc.refreshMetadataFromTypeDescriptor();

            for (Index idx : getIndexes()) {
                if (idx instanceof GridH2IndexBase)
                    ((GridH2IndexBase)idx).refreshColumnIds();
            }

            incrementModificationCounter();
        }
        finally {
            unlock(true);
        }
    }

    /** {@inheritDoc} */
    @Override protected void setColumns(Column[] columns) {
        this.safeColumns = columns;

        super.setColumns(columns);
    }

    /** {@inheritDoc} */
    @Override public Column[] getColumns() {
        return safeColumns;
    }

    /**
     * Increment modification counter to force recompilation of existing prepared statements.
     */
    private void incrementModificationCounter() {
        assert lock.isWriteLockedByCurrentThread();

        setModified();
    }

    /**
     * @param s H2 session.
     */
    public static void unlockTables(Session s) {
        for (Table t : s.getLocks()) {
            if (t instanceof GridH2Table)
                ((GridH2Table)t).unlockReadInternal(s);
        }
    }

    /**
     * @param s H2 session.
     */
    public static void readLockTables(Session s) {
        for (Table t : s.getLocks()) {
            if (t instanceof GridH2Table)
                ((GridH2Table)t).readLockInternal(s);
        }
    }

    /**
     * @param s H2 session.
     */
    public static void checkTablesVersions(Session s) {
        for (Table t : s.getLocks()) {
            if (t instanceof GridH2Table)
                ((GridH2Table)t).checkVersion(s);
        }
    }

    /**
     *
     */
    private static class SessionLock {
        /** Version. */
        final long ver;

        /** Locked by current thread flag. */
        boolean locked;

        /**
         * Constructor for shared lock.
         *
         * @param ver Table version.
         */
        private SessionLock(long ver) {
            this.ver = ver;
            locked = true;
        }

        /**
         * @param ver Locked table version.
         * @return Shared lock instance.
         */
        static SessionLock sharedLock(long ver) {
            return new SessionLock(ver);
        }

        /**
         * @return Exclusive lock instance.
         */
        static SessionLock exclusiveLock() {
            return new SessionLock(EXCLUSIVE_LOCK);
        }

        /**
         * @return {@code true} if exclusive lock.
         */
        boolean isExclusive() {
            return ver == EXCLUSIVE_LOCK;
        }

        /**
         * @return Table version of the first lock.
         */
        long version() {
            return ver;
        }
    }

    /** */
    private byte[] rowValueBytes(Object row) {
        if (row instanceof H2CacheRow) {
            try {
                return ((H2CacheRow)row).value().valueBytes(cacheContext().cacheObjectContext());
            }
            catch (Exception ex) {
                // NO-OP
            }
        }

        return "<UNAVAILABLE>".getBytes(UTF_8);
    }

    /** */
    private byte[] rowKeyBytes(Object row) {
        if (row instanceof H2CacheRow) {
            try {
                return ((H2CacheRow)row).key().valueBytes(cacheContext().cacheObjectContext());
            }
            catch (Exception ex) {
                // NO-OP
            }
        }

        return "<UNAVAILABLE>".getBytes(UTF_8);
    }

    /**
     * Get an index by name.
     *
     * @param indexName Index name to search for.
     * @return Found index, {@code null} if not found.
     */
    public @Nullable Index getIndexSafe(String indexName) {
        ArrayList<Index> indexes = getIndexes();

        if (HashJoinIndex.HASH_JOIN_IDX.equalsIgnoreCase(indexName))
            return new HashJoinIndex(this);

        if (indexes != null) {
            for (Index index : indexes) {
                if (index.getName().equals(indexName))
                    return index;
            }
        }

        return null;
    }
}
