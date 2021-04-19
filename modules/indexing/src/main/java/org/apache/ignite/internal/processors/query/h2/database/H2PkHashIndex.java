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

import java.util.Iterator;
import java.util.List;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.IgniteCacheOffheapManager;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.mvcc.MvccSnapshot;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRowAdapter;
import org.apache.ignite.internal.processors.cache.tree.CacheDataRowStore;
import org.apache.ignite.internal.processors.query.GridQueryTypeDescriptor;
import org.apache.ignite.internal.processors.query.h2.H2Utils;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2IndexBase;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2RowDescriptor;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Table;
import org.apache.ignite.internal.processors.query.h2.opt.H2CacheRow;
import org.apache.ignite.internal.processors.query.h2.opt.QueryContext;
import org.apache.ignite.internal.util.lang.GridCursor;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.indexing.IndexingQueryCacheFilter;
import org.apache.ignite.spi.indexing.IndexingQueryFilter;
import org.gridgain.internal.h2.command.dml.AllColumnsForPlan;
import org.gridgain.internal.h2.engine.Session;
import org.gridgain.internal.h2.index.Cursor;
import org.gridgain.internal.h2.index.IndexCondition;
import org.gridgain.internal.h2.index.IndexType;
import org.gridgain.internal.h2.message.DbException;
import org.gridgain.internal.h2.result.Row;
import org.gridgain.internal.h2.result.SearchRow;
import org.gridgain.internal.h2.result.SortOrder;
import org.gridgain.internal.h2.table.Column;
import org.gridgain.internal.h2.table.IndexColumn;
import org.gridgain.internal.h2.table.TableFilter;

import static org.apache.ignite.internal.processors.cache.IgniteCacheOffheapManager.DATA;

/**
 *
 */
public class H2PkHashIndex extends GridH2IndexBase {
    /** */
    private final GridCacheContext cctx;

    /** */
    private final int segments;

    /**
     * @param cctx Cache context.
     * @param tbl Table.
     * @param name Index name.
     * @param colsList Index columns.
     * @param segments Segments.
     */
    @SuppressWarnings("ZeroLengthArrayAllocation")
    public H2PkHashIndex(
        GridCacheContext<?, ?> cctx,
        GridH2Table tbl,
        String name,
        List<IndexColumn> colsList,
        int segments
    ) {
        super(
            tbl,
            name,
            GridH2IndexBase.columnsArray(tbl, colsList),
            IndexType.createPrimaryKey(false, true));

        assert segments > 0 : segments;

        this.cctx = cctx;
        this.segments = segments;
    }

    /** {@inheritDoc} */
    @Override public int segmentsCount() {
        return segments;
    }

    /** {@inheritDoc} */
    @Override public Cursor find(Session ses, final SearchRow lower, final SearchRow upper) {
        IndexingQueryCacheFilter filter = null;
        MvccSnapshot mvccSnapshot = null;

        QueryContext qctx = H2Utils.context(ses);

        int seg = 0;

        if (qctx != null) {
            IndexingQueryFilter f = qctx.filter();
            filter = f != null ? f.forCache(getTable().cacheName()) : null;
            mvccSnapshot = qctx.mvccSnapshot();

            seg = segment(qctx);
        }

        assert !cctx.mvccEnabled() || mvccSnapshot != null;

        KeyCacheObject lowerObj = lower != null ? cctx.toCacheKeyObject(lower.getValue(0).getObject()) : null;
        KeyCacheObject upperObj = upper != null ? cctx.toCacheKeyObject(upper.getValue(0).getObject()) : null;

        final Iterator<IgniteCacheOffheapManager.CacheDataStore> it = cctx.offheap().cacheDataStores().iterator();

        final H2PkHashIndexCursor cur = new H2PkHashIndexCursor(it, lowerObj, upperObj, filter, seg, mvccSnapshot);

        return cur;
    }

    /** {@inheritDoc} */
    @Override public boolean canScan() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public H2CacheRow put(H2CacheRow row) {
        // Should not be called directly. Rows are inserted into underlying cache data stores.
        assert false;

        throw DbException.getUnsupportedException("put");
    }

    /** {@inheritDoc} */
    @Override public boolean putx(H2CacheRow row) {
        // Should not be called directly. Rows are inserted into underlying cache data stores.
        assert false;

        throw DbException.getUnsupportedException("putx");
    }

    /** {@inheritDoc} */
    @Override public boolean removex(SearchRow row) {
        // Should not be called directly. Rows are removed from underlying cache data stores.
        assert false;

        throw DbException.getUnsupportedException("removex");
    }

    /** {@inheritDoc} */
    @Override public double getCost(Session ses, int[] masks, TableFilter[] filters, int filter,
        SortOrder sortOrder, AllColumnsForPlan allColsSet) {
        if (masks == null)
            return Long.MAX_VALUE;

        for (Column column : columns) {
            int index = column.getColumnId();
            int mask = masks[index];

            if ((mask & IndexCondition.EQUALITY) != IndexCondition.EQUALITY)
                return Long.MAX_VALUE;
        }

        return 2;
    }

    /** {@inheritDoc} */
    @Override public long getRowCount(Session ses) {
        Cursor cursor = find(ses, null, null);

        long res = 0;

        while (cursor.next())
            res++;

        return res;
    }

    /** {@inheritDoc} */
    @Override public boolean canGetFirstOrLast() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public Cursor findFirstOrLast(Session ses, boolean b) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public long totalRowCount(IndexingQueryCacheFilter partsFilter) {
        H2PkHashIndexCursor pkHashCursor = new H2PkHashIndexCursor(
            cctx.offheap().cacheDataStores().iterator(),
            null,
            null,
            null,
            0,
            null);

        long res = 0;

        while (pkHashCursor.next())
            res++;

        return res;
    }

    /**
     * Cursor.
     */
    private class H2PkHashIndexCursor implements Cursor {
        /** */
        private final GridH2RowDescriptor desc;

        /** */
        private final Iterator<IgniteCacheOffheapManager.CacheDataStore> itStore;

        /** */
        private final KeyCacheObject lowerObj;

        /** */
        private final KeyCacheObject upperObj;

        /** */
        private final IndexingQueryCacheFilter filter;

        /** */
        private final int seg;

        /** */
        private final MvccSnapshot mvccSnapshot;

        /** */
        private GridCursor<? extends CacheDataRow> curr;

        /** Creation time of this cursor to check if some row is expired. */
        private final long time;

        /**
         */
        private H2PkHashIndexCursor(
            Iterator<IgniteCacheOffheapManager.CacheDataStore> itStore,
            KeyCacheObject lowerObj,
            KeyCacheObject upperObj,
            IndexingQueryCacheFilter filter,
            int seg,
            MvccSnapshot mvccSnapshot) {
            this.itStore = itStore;
            this.lowerObj = lowerObj;
            this.upperObj = upperObj;
            this.filter = filter;
            this.seg = seg;
            this.mvccSnapshot = mvccSnapshot;

            desc = rowDescriptor();

            time = U.currentTimeMillis();
        }

        /** {@inheritDoc} */
        @Override public Row get() {
            try {
                return desc.createRow(curr.get());
            }
            catch (IgniteCheckedException e) {
                throw DbException.convert(e);
            }
        }

        /** {@inheritDoc} */
        @Override public SearchRow getSearchRow() {
            return get();
        }

        /** {@inheritDoc} */
        @Override public boolean next() {
            try {
                CacheDataRowStore.setSkipVersion(true);

                GridQueryTypeDescriptor type = desc.type();

                for (;;) {
                    if (curr != null) {
                        while (curr.next()) {
                            CacheDataRow row = curr.get();
                            // Need to filter rows by value type because in a single cache
                            // we can have multiple indexed types.
                            // Also need to skip expired rows.
                            if (type.matchType(row.value()) && !wasExpired(row))
                                return true;
                        }
                    }

                    curr = nextCursor();

                    if (curr == null)
                        return false;
                }
            }
            catch (IgniteCheckedException e) {
                throw DbException.convert(e);
            }
            finally {
                CacheDataRowStore.setSkipVersion(false);
            }
        }

        /** */
        private GridCursor<? extends CacheDataRow> nextCursor() throws IgniteCheckedException {
            while (itStore.hasNext()) {
                IgniteCacheOffheapManager.CacheDataStore store = itStore.next();

                int part = store.partId();

                if (segmentForPartition(part) != seg)
                    continue;

                if (filter == null || filter.applyPartition(part)) {
                    return store.cursor(
                            cctx.cacheId(),
                            lowerObj,
                            upperObj,
                            CacheDataRowAdapter.RowData.FULL,
                            mvccSnapshot,
                            DATA
                    );
                }
            }

            return null;
        }

        /**
         * @param row to check.
         * @return {@code true} if row was expired at the moment this cursor was created; {@code false} if not or if
         * expire time is not set for this cursor.
         */
        private boolean wasExpired(CacheDataRow row) {
            return row.expireTime() > 0 && row.expireTime() <= time;
        }

        /** {@inheritDoc} */
        @Override public boolean previous() {
            throw DbException.getUnsupportedException("previous");
        }
    }
}
