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

package org.apache.ignite.internal.processors.query.h2.twostep;

import java.util.Collection;
import java.util.Set;
import java.util.UUID;
import javax.cache.CacheException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.GridKernalContext;
import org.h2.engine.Session;
import org.h2.index.BaseIndex;
import org.h2.index.Cursor;
import org.h2.index.IndexType;
import org.h2.message.DbException;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.table.IndexColumn;
import org.h2.table.Table;

/**
 * Merge index.
 */
public abstract class AbstractReduceIndex extends BaseIndex implements Reducer {
    /**
     * @param ctx  Context.
     * @param tbl  Table.
     * @param name Index name.
     * @param type Type.
     * @param cols Columns.
     */
    protected AbstractReduceIndex(GridKernalContext ctx,
        Table tbl,
        String name,
        IndexType type,
        IndexColumn[] cols
    ) {
        super(tbl, 0, name, cols, type);
    }

    /**
     * @param ctx Context.
     * @param tbl Fake reduce table.
     */
    protected AbstractReduceIndex(GridKernalContext ctx, Table tbl) {
        this(ctx, tbl, null, IndexType.createScan(false), null);
    }

    /**
     * @return Index reducer.
     */
    protected abstract AbstractReducer reducer();

    /** {@inheritDoc} */
    @Override public Set<UUID> sources() {
        return reducer().sources();
    }

    /** {@inheritDoc} */
    @Override public boolean hasSource(UUID nodeId) {
        return reducer().hasSource(nodeId);
    }

    /** {@inheritDoc} */
    @Override public long getRowCount(Session ses) {
        Cursor c = find(ses, null, null);

        long cnt = 0;

        while (c.next())
            cnt++;

        return cnt;
    }

    /** {@inheritDoc} */
    @Override public long getRowCountApproximation(Session ses) {
        return 10_000;
    }

    /** {@inheritDoc} */
    @Override public void setSources(Collection<ClusterNode> nodes, int segmentsCnt) {
        reducer().setSources(nodes, segmentsCnt);
    }

    /** {@inheritDoc} */
    @Override public void onFailure(UUID nodeId, final CacheException e) {
        reducer().onFailure(nodeId, e);
    }

    /** {@inheritDoc} */
    @Override public final void addPage(ReduceResultPage page) {
        reducer().addPage(page);
    }

    /** {@inheritDoc} */
    @Override public void setPageSize(int pageSize) {
        reducer().setPageSize(pageSize);
    }

    /** {@inheritDoc} */
    @Override public final Cursor find(Session ses, SearchRow first, SearchRow last) {
        return find(first, last);
    }

    /** {@inheritDoc} */
    @Override public Cursor find(SearchRow first, SearchRow last) {
        return reducer().find(first, last);
    }

    /** {@inheritDoc} */
    @Override public boolean fetchedAll() {
        return reducer().fetchedAll();
    }

    /** {@inheritDoc} */
    @Override public void checkRename() {
        throw DbException.getUnsupportedException("rename");
    }

    /** {@inheritDoc} */
    @Override public void close(Session ses) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void add(Session ses, Row row) {
        throw DbException.getUnsupportedException("add");
    }

    /** {@inheritDoc} */
    @Override public void remove(Session ses, Row row) {
        throw DbException.getUnsupportedException("remove row");
    }

    /** {@inheritDoc} */
    @Override public void remove(Session ses) {
        throw DbException.getUnsupportedException("remove index");
    }

    /** {@inheritDoc} */
    @Override public void truncate(Session ses) {
        throw DbException.getUnsupportedException("truncate");
    }

    /** {@inheritDoc} */
    @Override public boolean canGetFirstOrLast() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public Cursor findFirstOrLast(Session ses, boolean first) {
        throw DbException.getUnsupportedException("findFirstOrLast");
    }

    /** {@inheritDoc} */
    @Override public boolean needRebuild() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public long getDiskSpaceUsed() {
        return 0;
    }
}