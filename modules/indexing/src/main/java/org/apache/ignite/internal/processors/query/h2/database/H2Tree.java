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

package org.apache.ignite.internal.processors.query.h2.database;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.SystemProperty;
import org.apache.ignite.failure.FailureType;
import org.apache.ignite.internal.metric.IoStatisticsHolder;
import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.pagemem.PageIdAllocator;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.pagemem.wal.record.PageSnapshot;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.mvcc.MvccUtils;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRowAdapter;
import org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.PageLockTrackerManager;
import org.apache.ignite.internal.processors.cache.persistence.tree.BPlusTree;
import org.apache.ignite.internal.processors.cache.persistence.tree.CorruptedTreeException;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusMetaIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIoResolver;
import org.apache.ignite.internal.processors.cache.persistence.tree.reuse.ReuseList;
import org.apache.ignite.internal.processors.cache.tree.mvcc.data.MvccDataRow;
import org.apache.ignite.internal.processors.failure.FailureProcessor;
import org.apache.ignite.internal.processors.query.h2.H2RowCache;
import org.apache.ignite.internal.processors.query.h2.H2Utils;
import org.apache.ignite.internal.processors.query.h2.database.inlinecolumn.InlineIndexColumnFactory;
import org.apache.ignite.internal.processors.query.h2.database.io.H2ExtrasInnerIO;
import org.apache.ignite.internal.processors.query.h2.database.io.H2ExtrasLeafIO;
import org.apache.ignite.internal.processors.query.h2.database.io.H2RowLinkIO;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Table;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2ValueCacheObject;
import org.apache.ignite.internal.processors.query.h2.opt.H2CacheRow;
import org.apache.ignite.internal.processors.query.h2.opt.H2Row;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteProductVersion;
import org.apache.ignite.maintenance.MaintenanceTask;
import org.gridgain.internal.h2.message.DbException;
import org.gridgain.internal.h2.result.SearchRow;
import org.gridgain.internal.h2.result.SortOrder;
import org.gridgain.internal.h2.table.IndexColumn;
import org.gridgain.internal.h2.value.Value;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.query.h2.database.H2TreeIndexBase.computeInlineSize;
import static org.apache.ignite.internal.processors.query.h2.database.H2TreeIndexBase.getAvailableInlineColumns;
import static org.apache.ignite.internal.processors.query.h2.database.inlinecolumn.AbstractInlineIndexColumn.CANT_BE_COMPARE;
import static org.apache.ignite.internal.processors.query.h2.maintenance.MaintenanceRebuildIndexUtils.mergeTasks;
import static org.apache.ignite.internal.processors.query.h2.maintenance.MaintenanceRebuildIndexUtils.toMaintenanceTask;

/**
 * H2 tree index implementation.
 */
public class H2Tree extends BPlusTree<H2Row, H2Row> {
    /** @see #IGNITE_THROTTLE_INLINE_SIZE_CALCULATION */
    public static final int DFLT_THROTTLE_INLINE_SIZE_CALCULATION = 1_000;

    /** */
    @SystemProperty(value = "How often real invocation of inline size calculation will be skipped.", type = Long.class,
        defaults = "" + DFLT_THROTTLE_INLINE_SIZE_CALCULATION)
    public static final String IGNITE_THROTTLE_INLINE_SIZE_CALCULATION = "IGNITE_THROTTLE_INLINE_SIZE_CALCULATION";

    /** Cache context. */
    private final GridCacheContext<?, ?> cctx;

    /** Owning table. */
    private final GridH2Table table;

    /** */
    private final int inlineSize;

    /** List of helpers to work with inline values on the page. */
    private final List<InlineIndexColumn> inlineIdxs;

    /** Actual columns that current index is consist from. */
    private final IndexColumn[] cols;

    /**
     * Columns that will be used for inlining.
     * Could differ from actual columns {@link #cols} in case of
     * meta page were upgraded from older version.
     */
    private final IndexColumn[] inlineCols;

    /** */
    private final boolean mvccEnabled;

    /** */
    private final boolean pk;

    /** */
    private final boolean affinityKey;

    /** */
    private final String cacheName;

    /** */
    private final String tblName;

    /** */
    private final String idxName;

    /** */
    @Nullable private final IoStatisticsHolder stats;

    /** */
    private final Comparator<Value> comp = this::compareValues;

    /** Row cache. */
    private final H2RowCache rowCache;

    /** How often real invocation of inline size calculation will be skipped. */
    private final int THROTTLE_INLINE_SIZE_CALCULATION =
        IgniteSystemProperties.getInteger(IGNITE_THROTTLE_INLINE_SIZE_CALCULATION,
            DFLT_THROTTLE_INLINE_SIZE_CALCULATION);

    /** Counter of inline size calculation for throttling real invocations. */
    private final ThreadLocal<Long> inlineSizeCalculationCntr = ThreadLocal.withInitial(() -> 0L);

    /** Keep max calculated inline size for current index. */
    private final AtomicInteger maxCalculatedInlineSize;

    /** */
    private final IgniteLogger log;

    /** Whether PK is stored in unwrapped form. */
    private final boolean unwrappedPk;

    /** Whether index was created from scratch during owning node lifecycle. */
    private final boolean created;

    /** Stub flag, check {@link GridH2ValueCacheObject#useLegacyComparator()} description. */
    private final boolean useLegacyComparator;

    /**
     * Constructor.
     *
     * @param cctx Cache context.
     * @param table Owning table.
     * @param name Name of the tree.
     * @param idxName Name of the index.
     * @param cacheName Name of the cache.
     * @param tblName Name of the table.
     * @param reuseList Reuse list.
     * @param grpId Cache group ID.
     * @param grpName Name of the cache group.
     * @param pageMem Page memory.
     * @param wal Write ahead log manager.
     * @param globalRmvId Global remove ID counter.
     * @param metaPageId Meta page ID.
     * @param initNew if {@code true} new tree will be initialized,
     * else meta page info will be read.
     * @param unwrappedCols Unwrapped indexed columns.
     * @param wrappedCols Original indexed columns.
     * @param maxCalculatedInlineSize Keep max calculated inline size
     * for current index.
     * @param pk {@code true} for primary key.
     * @param affinityKey {@code true} for affinity key.
     * @param mvccEnabled Mvcc flag.
     * @param rowCache Row cache.
     * @param failureProcessor if the tree is corrupted.
     * @param pageLockTrackerManager Page lock tracker manager.
     * @param log Logger.
     * @param stats Statistics holder.
     * @param factory Inline helper factory.
     * @param configuredInlineSize Size that has been set by user during
     * index creation.
     * @throws IgniteCheckedException If failed.
     */
    public H2Tree(
        @Nullable GridCacheContext<?, ?> cctx,
        GridH2Table table,
        String name,
        String idxName,
        String cacheName,
        String tblName,
        ReuseList reuseList,
        int grpId,
        String grpName,
        PageMemory pageMem,
        IgniteWriteAheadLogManager wal,
        AtomicLong globalRmvId,
        long metaPageId,
        boolean initNew,
        List<IndexColumn> unwrappedCols,
        List<IndexColumn> wrappedCols,
        AtomicInteger maxCalculatedInlineSize,
        boolean pk,
        boolean affinityKey,
        boolean mvccEnabled,
        @Nullable H2RowCache rowCache,
        @Nullable FailureProcessor failureProcessor,
        PageLockTrackerManager pageLockTrackerManager,
        IgniteLogger log,
        @Nullable IoStatisticsHolder stats,
        InlineIndexColumnFactory factory,
        int configuredInlineSize,
        PageIoResolver pageIoRslvr
    ) throws IgniteCheckedException {
        super(
            name,
            grpId,
            grpName,
            pageMem,
            wal,
            globalRmvId,
            metaPageId,
            reuseList,
            PageIdAllocator.FLAG_IDX,
            failureProcessor,
            pageLockTrackerManager,
            pageIoRslvr
        );

        this.cctx = cctx;
        this.table = table;
        this.stats = stats;
        this.log = log;
        this.rowCache = rowCache;
        this.idxName = idxName;
        this.cacheName = cacheName;
        this.tblName = tblName;
        this.maxCalculatedInlineSize = maxCalculatedInlineSize;
        this.pk = pk;
        this.affinityKey = affinityKey;
        this.mvccEnabled = mvccEnabled;

        if (!initNew) {
            // Page is ready - read meta information.
            MetaPageInfo metaInfo = getMetaInfo();

            IgniteProductVersion ver_8_7_40 = IgniteProductVersion.fromString("8.7.40");
            IgniteProductVersion ver_8_8_11 = IgniteProductVersion.fromString("8.8.11");

            IgniteProductVersion idxCreateVer = metaInfo.createdVersion();

            if (idxCreateVer == null /* too old version */ ||
                idxCreateVer.compareTo(ver_8_7_40) < 0 ||
                idxCreateVer.minor() != 7 && idxCreateVer.compareTo(ver_8_8_11) < 0)
                useLegacyComparator = true; // Fallback to legacy comparator due to compatibility reasons.
            else
                useLegacyComparator = false;

            unwrappedPk = metaInfo.useUnwrappedPk();

            cols = (unwrappedPk ? unwrappedCols : wrappedCols).toArray(H2Utils.EMPTY_COLUMNS);

            inlineSize = metaInfo.inlineSize();

            List<InlineIndexColumn> inlineIdxs0 = getAvailableInlineColumns(affinityKey, cacheName, idxName, log, pk,
                table, cols, factory, metaInfo.inlineObjectHash());

            // IOs must be set before calling inlineObjectSupported(),
            // because IOs will be used to traverse the tree.
            setIos(
                H2ExtrasInnerIO.getVersions(inlineSize, mvccEnabled),
                H2ExtrasLeafIO.getVersions(inlineSize, mvccEnabled)
            );

            boolean inlineObjSupported = inlineSize > 0 && inlineObjectSupported(metaInfo, inlineIdxs0);

            inlineIdxs = inlineObjSupported ? inlineIdxs0 : inlineIdxs0.stream()
                .filter(ih -> ih.type() != Value.JAVA_OBJECT)
                .collect(Collectors.toList());

            inlineCols = new IndexColumn[inlineIdxs.size()];

            for (int i = 0, j = 0; i < cols.length && j < inlineIdxs.size(); i++) {
                if (cols[i].column.getColumnId() == inlineIdxs.get(j).columnIndex())
                    inlineCols[j++] = cols[i];
            }

            boolean inlineDecimalSupported = inlineSize > 0 && metaInfo.inlineDecimalSupported();

            // remove tail if inlining of decimals is not supported
            if (!inlineDecimalSupported) {
                boolean decimal = false;

                Iterator<InlineIndexColumn> it = inlineIdxs.iterator();
                while (it.hasNext()) {
                    InlineIndexColumn ih = it.next();

                    if (ih.type() == Value.DECIMAL)
                        decimal = true;

                    if (decimal)
                        it.remove();
                }
            }

            if (!metaInfo.flagsSupported())
                upgradeMetaPage(inlineObjSupported);
        }
        else {
            unwrappedPk = true;

            useLegacyComparator = false;

            cols = unwrappedCols.toArray(H2Utils.EMPTY_COLUMNS);
            inlineCols = cols;

            inlineIdxs = getAvailableInlineColumns(affinityKey, cacheName, idxName, log, pk,
                table, cols, factory, true);

            inlineSize = computeInlineSize(idxName, inlineIdxs, configuredInlineSize,
                    cctx.config().getSqlIndexMaxInlineSize(), log);

            setIos(
                H2ExtrasInnerIO.getVersions(inlineSize, mvccEnabled),
                H2ExtrasLeafIO.getVersions(inlineSize, mvccEnabled)
            );

            initTree(true, inlineSize);
        }

        created = initNew;
    }

    /**
     * @param metaInfo Metapage info.
     * @param inlineIdxs Base collection of index helpers.
     * @return {@code true} if inline object is supported by exists tree.
     */
    private boolean inlineObjectSupported(MetaPageInfo metaInfo, List<InlineIndexColumn> inlineIdxs) {
        if (metaInfo.flagsSupported())
            return metaInfo.inlineObjectSupported();
        else {
            try {
                if (H2TreeInlineObjectDetector.objectMayBeInlined(inlineSize, inlineIdxs)) {
                    H2TreeInlineObjectDetector inlineObjDetector = new H2TreeInlineObjectDetector(
                        inlineSize, inlineIdxs, tblName, idxName, log);

                    findFirst(inlineObjDetector);

                    return inlineObjDetector.inlineObjectSupported();
                }
                else
                    return false;
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException("Unexpected exception on detect inline object", e);
            }
        }
    }

    /**
     * Return columns of the index.
     *
     * @return Indexed columns.
     */
    public IndexColumn[] cols() {
        return cols;
    }

    /**
     * Create row from link.
     *
     * @param link Link.
     * @return Row.
     * @throws IgniteCheckedException if failed.
     */
    public H2Row createRow(long link) throws IgniteCheckedException {
        return createRow(link, true);
    }

    public H2Row createRow(long link, boolean follow) throws IgniteCheckedException {
        if (rowCache != null) {
            H2CacheRow row = rowCache.get(link);

            if (row == null) {
                row = createRow0(link, follow);

                rowCache.put(row);
            }

            return row;
        }
        else
            return createRow0(link, follow);
    }

    /**
     * !!! This method must be invoked in read or write lock of referring index page. It is needed to
     * !!! make sure that row at this link will be invisible, when the link will be removed from
     * !!! from all the index pages, so that row can be safely erased from the data page.
     *
     * @param link Link.
     * @return Row.
     * @throws IgniteCheckedException If failed.
     */
    private H2CacheRow createRow0(long link, boolean follow) throws IgniteCheckedException {
        CacheDataRowAdapter row = new CacheDataRowAdapter(link);

        if (follow) {
            row.initFromLink(
                cctx.group(),
                CacheDataRowAdapter.RowData.FULL,
                true
            );
        }

        return table.rowDescriptor().createRow(row);
    }

    /**
     * Create row from link.
     *
     * @param link Link.
     * @param mvccOpCntr MVCC operation counter.
     * @return Row.
     * @throws IgniteCheckedException if failed.
     */
    public H2Row createMvccRow(
        long link,
        long mvccCrdVer,
        long mvccCntr,
        int mvccOpCntr
    ) throws IgniteCheckedException {
        return createMvccRow(link, mvccCrdVer, mvccCntr, mvccOpCntr, null);
    }

    /**
     * Create row from link.
     *
     * @param link Link.
     * @param mvccOpCntr MVCC operation counter.
     * @return Row.
     * @throws IgniteCheckedException if failed.
     */
    public H2Row createMvccRow(
        long link,
        long mvccCrdVer,
        long mvccCntr,
        int mvccOpCntr,
        CacheDataRowAdapter.RowData rowData
    ) throws IgniteCheckedException {
        if (rowCache != null) {
            H2CacheRow row = rowCache.get(link);

            if (row == null) {
                row = createMvccRow0(link, mvccCrdVer, mvccCntr, mvccOpCntr, rowData);

                rowCache.put(row);
            }

            return row;
        }
        else
            return createMvccRow0(link, mvccCrdVer, mvccCntr, mvccOpCntr, rowData);
    }

    public boolean getPk() {
        return pk;
    }

    public boolean getAffinityKey() {
        return affinityKey;
    }

    /**
     * @param link Link.
     * @param mvccCrdVer Mvcc coordinator version.
     * @param mvccCntr Mvcc counter.
     * @param mvccOpCntr Mvcc operation counter.
     * @return Row.
     */
    private H2CacheRow createMvccRow0(long link, long mvccCrdVer, long mvccCntr, int mvccOpCntr, CacheDataRowAdapter.RowData rowData)
            throws IgniteCheckedException {
        int partId = PageIdUtils.partId(PageIdUtils.pageId(link));

        MvccDataRow row = new MvccDataRow(
            cctx.group(),
            0,
            link,
            partId,
            rowData,
            mvccCrdVer,
            mvccCntr,
            mvccOpCntr,
            true
        );

        return table.rowDescriptor().createRow(row);
    }

    /** {@inheritDoc} */
    @Override public H2Row getRow(BPlusIO<H2Row> io, long pageAddr, int idx, Object ignore)
        throws IgniteCheckedException {
        return io.getLookupRow(this, pageAddr, idx);
    }

    /**
     * @return Inline size.
     */
    public int inlineSize() {
        return inlineSize;
    }

    /**
     * @return Inline size.
     * @throws IgniteCheckedException If failed.
     */
    public MetaPageInfo getMetaInfo() throws IgniteCheckedException {
        final long metaPage = acquirePage(metaPageId);

        try {
            long pageAddr = readLock(metaPageId, metaPage); // Meta can't be removed.

            assert pageAddr != 0 : "Failed to read lock meta page [metaPageId=" +
                U.hexLong(metaPageId) + ']';

            try {
                BPlusMetaIO io = BPlusMetaIO.VERSIONS.forPage(pageAddr);

                return new MetaPageInfo(io, pageAddr);
            }
            finally {
                readUnlock(metaPageId, metaPage, pageAddr);
            }
        }
        finally {
            releasePage(metaPageId, metaPage);
        }
    }

    /**
     * Update root meta page if need (previous version not supported features flags
     * and created product version on root meta page).
     *
     * @param inlineObjSupported inline POJO by created tree flag.
     * @throws IgniteCheckedException On error.
     */
    private void upgradeMetaPage(boolean inlineObjSupported) throws IgniteCheckedException {
        final long metaPage = acquirePage(metaPageId);

        try {
            long pageAddr = writeLock(metaPageId, metaPage); // Meta can't be removed.

            assert pageAddr != 0 : "Failed to read lock meta page [metaPageId=" +
                U.hexLong(metaPageId) + ']';

            try {
                BPlusMetaIO.upgradePageVersion(pageAddr, inlineObjSupported, false, pageSize());

                if (wal != null)
                    wal.log(new PageSnapshot(new FullPageId(metaPageId, grpId),
                        pageAddr, pageMem.pageSize(), pageMem.realPageSize(grpId)));
            }
            finally {
                writeUnlock(metaPageId, metaPage, pageAddr, true);
            }
        }
        finally {
            releasePage(metaPageId, metaPage);
        }
    }

    /**
     * Copy info from another meta page.
     * @param info Meta page info.
     * @throws IgniteCheckedException If failed.
     */
    public void copyMetaInfo(MetaPageInfo info) throws IgniteCheckedException {
        final long metaPage = acquirePage(metaPageId);

        try {
            long pageAddr = writeLock(metaPageId, metaPage); // Meta can't be removed.

            assert pageAddr != 0 : "Failed to read lock meta page [metaPageId=" +
                U.hexLong(metaPageId) + ']';

            try {
                BPlusMetaIO.setValues(
                    pageAddr,
                    info.inlineSize,
                    info.useUnwrappedPk,
                    info.inlineObjSupported,
                    info.inlineObjHash
                );
            }
            finally {
                writeUnlock(metaPageId, metaPage, pageAddr, true);
            }
        }
        finally {
            releasePage(metaPageId, metaPage);
        }
    }


    /** {@inheritDoc} */
    @SuppressWarnings("ForLoopReplaceableByForEach")
    @Override protected int compare(BPlusIO<H2Row> io, long pageAddr, int idx,
        H2Row row) throws IgniteCheckedException {
        try {
            if (inlineSize() == 0)
                return compareRows(getRow(io, pageAddr, idx), row);
            else {
                int off = io.offset(idx);

                int fieldOff = 0;

                int lastIdxUsed = 0;

                for (int i = 0; i < inlineIdxs.size(); i++) {
                    InlineIndexColumn inlineIdx = inlineIdxs.get(i);
                    Value v2 = row.getValue(inlineIdx.columnIndex());

                    if (v2 == null)
                        return 0;

                    int c = inlineIdx.compare(pageAddr, off + fieldOff, inlineSize() - fieldOff, v2, comp);

                    if (c == CANT_BE_COMPARE)
                        break;

                    lastIdxUsed++;

                    if (c != 0)
                        return fixSort(c, inlineCols[i].sortType);

                    fieldOff += inlineIdx.fullSize(pageAddr, off + fieldOff);

                    if (fieldOff > inlineSize())
                        break;
                }

                if (lastIdxUsed == cols.length)
                    return mvccCompare((H2RowLinkIO)io, pageAddr, idx, row);

                inlineSizeRecomendation(row);

                SearchRow rowData = getRow(io, pageAddr, idx);

                for (int i = lastIdxUsed, len = cols.length; i < len; i++) {
                    IndexColumn col = cols[i];
                    int idx0 = col.column.getColumnId();

                    Value v2 = row.getValue(idx0);

                    if (v2 == null) {
                        // Can't compare further.
                        return mvccCompare((H2RowLinkIO)io, pageAddr, idx, row);
                    }

                    Value v1 = rowData.getValue(idx0);

                    /**
                     * Stub GG-33962, GG-34893 (new comparator) workaround.
                     * Versions < 8.7.40 and < 8.8.11 are used incorrect comparator and a simple comparator change
                     * leads to the tree corruprion. Thus for "old" versions is used appropriate old comparator,
                     * we take into account that later all indexes will be rebuild and only one ver of comparator
                     * will be used.
                     */
                    if (useLegacyComparator && v1 instanceof GridH2ValueCacheObject)
                        ((GridH2ValueCacheObject)v1).useLegacyComparator();

                    int c = compareValues(v1, v2);

                    if (c != 0)
                        return fixSort(c, col.sortType);
                }

                return mvccCompare((H2RowLinkIO)io, pageAddr, idx, row);
            }
        }
        catch (DbException ex) {
            throw new IgniteCheckedException("Rows cannot be compared", ex);
        }
    }

    /**
     * Perform sort order correction.
     *
     * @param c Compare result.
     * @param sortType Sort type.
     * @return Fixed compare result.
     */
    private static int fixSort(int c, int sortType) {
        return sortType == SortOrder.ASCENDING ? c : -c;
    }

    /**
     * Compares two H2 rows.
     *
     * @param r1 Row 1.
     * @param r2 Row 2.
     * @return Compare result: see {@link Comparator#compare(Object, Object)} for values.
     */
    public int compareRows(H2Row r1, H2Row r2) {
        assert !mvccEnabled || r2.indexSearchRow() || MvccUtils.mvccVersionIsValid(r2.mvccCoordinatorVersion(), r2.mvccCounter()) : r2;
        if (r1 == r2)
            return 0;

        for (int i = 0, len = cols.length; i < len; i++) {
            IndexColumn idxCol = cols[i];

            int idx = idxCol.column.getColumnId();

            Value v1 = r1.getValue(idx);
            Value v2 = r2.getValue(idx);

            if (v1 == null || v2 == null) {
                // Can't compare further.
                return mvccCompare(r1, r2);
            }

            int c = compareValues(v1, v2);

            if (c != 0)
                return fixSort(c, idxCol.sortType);
        }

        return mvccCompare(r1, r2);
    }

    /**
     * Checks both rows are the same. <p/>
     * Primarily used to verify both search rows are the same and we can apply
     * the single row lookup optimization.
     *
     * @param r1 The first row.
     * @param r2 Another row.
     * @return {@code true} in case both rows are efficiently the same, {@code false} otherwise.
     */
    boolean checkRowsTheSame(H2Row r1, H2Row r2) {
        if (r1 == r2)
            return true;

        for (int i = 0, len = cols.length; i < len; i++) {
            IndexColumn idxCol = cols[i];

            int idx = idxCol.column.getColumnId();

            Value v1 = r1.getValue(idx);
            Value v2 = r2.getValue(idx);

            if (v1 == null && v2 == null)
                continue;

            if (v1 == null || v2 == null)
                return false;

            if (compareValues(v1, v2) != 0)
                return false;
        }

        return true;
    }

    /**
     * @param io IO.
     * @param pageAddr Page address.
     * @param idx Item index.
     * @param r2 Search row.
     * @return Comparison result.
     */
    private int mvccCompare(H2RowLinkIO io, long pageAddr, int idx, H2Row r2) {
        if (!mvccEnabled || r2.indexSearchRow())
            return 0;

        long crd = io.getMvccCoordinatorVersion(pageAddr, idx);
        long cntr = io.getMvccCounter(pageAddr, idx);
        int opCntr = io.getMvccOperationCounter(pageAddr, idx);

        assert MvccUtils.mvccVersionIsValid(crd, cntr, opCntr);

        return -MvccUtils.compare(crd, cntr, opCntr, r2);  // descending order
    }

    /**
     * @param r1 First row.
     * @param r2 Second row.
     * @return Comparison result.
     */
    private int mvccCompare(H2Row r1, H2Row r2) {
        if (!mvccEnabled || r2.indexSearchRow())
            return 0;

        long crdVer1 = r1.mvccCoordinatorVersion();
        long crdVer2 = r2.mvccCoordinatorVersion();

        int c = -Long.compare(crdVer1, crdVer2);

        if (c != 0)
            return c;

        return -Long.compare(r1.mvccCounter(), r2.mvccCounter());
    }

    /**
     * Calculate aggregate inline size for given indexes and log recommendation in case calculated size more than
     * current inline size.
     *
     * @param row Grid H2 row related to given inline indexes.
     */
    @SuppressWarnings({"ConditionalBreakInInfiniteLoop", "IfMayBeConditional"})
    private void inlineSizeRecomendation(SearchRow row) {
        //Do the check only for put operations.
        if (!(row instanceof H2CacheRow))
            return;

        Long invokeCnt = inlineSizeCalculationCntr.get();

        inlineSizeCalculationCntr.set(++invokeCnt);

        boolean throttle = invokeCnt % THROTTLE_INLINE_SIZE_CALCULATION != 0;

        if (throttle)
            return;

        int newSize = 0;

        InlineIndexColumn idx;

        List<String> colNames = new ArrayList<>();

        for (InlineIndexColumn index : inlineIdxs) {
            idx = index;

            newSize += idx.inlineSizeOf(row.getValue(idx.columnIndex()));

            colNames.add(index.columnName());
        }

        if (newSize > inlineSize()) {
            int oldSize;

            while (true) {
                oldSize = maxCalculatedInlineSize.get();

                if (oldSize >= newSize)
                    return;

                if (maxCalculatedInlineSize.compareAndSet(oldSize, newSize))
                    break;
            }

            String cols = colNames.stream().collect(Collectors.joining(", ", "(", ")"));

            String idxType = pk ? "PRIMARY KEY" : affinityKey ? "AFFINITY KEY (implicit)" : "SECONDARY";

            String recommendation;

            if (pk || affinityKey) {
                recommendation = "for sorted indexes on primary key and affinity field use 'PK_INLINE_SIZE' and " +
                    "'AFFINITY_INDEX_INLINE_SIZE' properties for CREATE TABLE command";
            }
            else {
                recommendation = "use INLINE_SIZE option for CREATE INDEX command, " +
                    "QuerySqlField.inlineSize for annotated classes, or QueryIndex.inlineSize for explicit " +
                    "QueryEntity configuration";
            }

            String warn = "Indexed columns of a row cannot be fully inlined into index " +
                "what may lead to slowdown due to additional data page reads, increase index inline size if needed " +
                "(" + recommendation + ") " +
                "[cacheName=" + cacheName +
                ", tableName=" + tblName +
                ", idxName=" + idxName +
                ", idxCols=" + cols +
                ", idxType=" + idxType +
                ", curSize=" + inlineSize() +
                ", recommendedInlineSize=" + newSize + "]";

            U.warn(log, warn);
        }
    }

    /** {@inheritDoc} */
    @Override protected IoStatisticsHolder statisticsHolder() {
        return stats != null ? stats : super.statisticsHolder();
    }

    /**
     * @return {@code true} In case use unwrapped columns for PK
     */
    public boolean unwrappedPk() {
        return unwrappedPk;
    }

    /**
     * @return Inline indexes for the segment.
     */
    public List<InlineIndexColumn> inlineIndexes() {
        return inlineIdxs;
    }

    /**
     *
     */
    public static class MetaPageInfo {
        /** */
        int inlineSize;

        /** */
        boolean useUnwrappedPk;

        /** */
        boolean flagsSupported;

        /** */
        boolean inlineObjSupported;

        /** */
        boolean inlineObjHash;

        /** */
        boolean inlineDecimalSupported;

        /** */
        IgniteProductVersion createdVer;

        /**
         * @param io Metapage IO.
         * @param pageAddr Page address.
         */
        public MetaPageInfo(BPlusMetaIO io, long pageAddr) {
            inlineSize = io.getInlineSize(pageAddr);
            useUnwrappedPk = io.unwrappedPk(pageAddr);
            flagsSupported = io.supportFlags();

            if (flagsSupported) {
                inlineObjSupported = io.inlineObjectSupported(pageAddr);
                inlineObjHash = io.inlineObjectHash(pageAddr);
                inlineDecimalSupported = io.inlineDecimalSupported(pageAddr);
            }

            createdVer = io.createdVersion(pageAddr);
        }

        /**
         * @return Inline size.
         */
        public int inlineSize() {
            return inlineSize;
        }

        /**
         * @return {@code true} In case use unwrapped PK for indexes.
         */
        public boolean useUnwrappedPk() {
            return useUnwrappedPk;
        }

        /**
         * @return {@code true} In case metapage contains flags.
         */
        public boolean flagsSupported() {
            return flagsSupported;
        }

        /**
         * @return {@code true} In case inline object is supported.
         */
        public boolean inlineObjectSupported() {
            return inlineObjSupported;
        }

        /**
         * @return {@code true} In case inline object is supported.
         */
        public boolean inlineObjectHash() {
            return inlineObjHash;
        }

        /**
         * @return {@code true} In case inline decimal is supported.
         */
        public boolean inlineDecimalSupported() {
            return inlineDecimalSupported;
        }

        /**
         * @return Created version.
         */
        public IgniteProductVersion createdVersion() {
            return createdVer;
        }
    }

    /**
     * @param v1 First value.
     * @param v2 Second value.
     * @return Comparison result.
     */
    public int compareValues(Value v1, Value v2) {
        return v1 == v2 ? 0 : table.compareValues(v1, v2);
    }

    /**
     * @return {@code True} if index was created during curren node's lifetime, {@code False} if it was restored from
     * disk.
     */
    public boolean created() {
        return created;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(H2Tree.class, this, "super", super.toString());
    }

    /**
     * Construct the exception and invoke failure processor.
     *
     * @param msg Message.
     * @param cause Cause.
     * @param grpId Group id.
     * @param pageIds Pages ids.
     * @return New CorruptedTreeException instance.
     */
    @Override protected CorruptedTreeException corruptedTreeException(String msg, Throwable cause, int grpId, long... pageIds) {
        CorruptedTreeException e = new CorruptedTreeException(msg, cause, grpName, cacheName, idxName, grpId, pageIds);

        String errorMsg = "Index " + idxName + " of the table " + tblName + " (cache " + cacheName + ") is " +
            "corrupted, to fix this issue a rebuild is required. On the next restart, node will enter the " +
            "maintenance mode and rebuild corrupted indexes.";

        log.warning(errorMsg);

        int cacheId = table.cacheId();

        try {
            MaintenanceTask task = toMaintenanceTask(cacheId, idxName);

            cctx.kernalContext().maintenanceRegistry().registerMaintenanceTask(
                task,
                oldTask -> mergeTasks(oldTask, task)
            );
        }
        catch (IgniteCheckedException ex) {
            log.warning("Failed to register maintenance record for corrupted partition files.", ex);
        }

        processFailure(FailureType.CRITICAL_ERROR, e);

        return e;
    }

    /** {@inheritDoc} */
    @Override protected void temporaryReleaseLock() {
        cctx.kernalContext().cache().context().database().checkpointReadUnlock();
        cctx.kernalContext().cache().context().database().checkpointReadLock();
    }

    /** {@inheritDoc} */
    @Override protected long maxLockHoldTime() {
        long sysWorkerBlockedTimeout = cctx.kernalContext().workersRegistry().getSystemWorkerBlockedTimeout();

        // Using timeout value reduced by 10 times to increase possibility of lock releasing before timeout.
        return sysWorkerBlockedTimeout == 0 ? Long.MAX_VALUE : (sysWorkerBlockedTimeout / 10);
    }

    /** {@inheritDoc} */
    @Override protected String lockRetryErrorMessage(String op) {
        return super.lockRetryErrorMessage(op) + " Problem with the index [cacheName=" +
            cacheName + ", tblName=" + tblName + ", idxName=" + idxName + ']';
    }

    /**
     * @return Table.
     */
    public GridH2Table table() {
        return table;
    }
}
