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

import org.apache.ignite.internal.GridKernalContext;
import org.gridgain.internal.h2.command.dml.AllColumnsForPlan;
import org.gridgain.internal.h2.engine.Session;
import org.gridgain.internal.h2.index.Index;
import org.gridgain.internal.h2.index.IndexType;
import org.gridgain.internal.h2.result.SortOrder;
import org.gridgain.internal.h2.table.IndexColumn;
import org.gridgain.internal.h2.table.TableFilter;

/**
 * H2 {@link Index} adapter for {@link UnsortedReducer}.
 */
public final class UnsortedReduceIndexAdapter extends AbstractReduceIndexAdapter {
    /** Index type. */
    private static final IndexType TYPE = IndexType.createScan(false);

    /** */
    private final UnsortedReducer delegate;

    /**
     * @param ctx Context.
     * @param tbl Table.
     * @param name Index name.
     */
    public UnsortedReduceIndexAdapter(GridKernalContext ctx, ReduceTable tbl, String name) {
        super(ctx, tbl, name, TYPE, IndexColumn.wrap(tbl.getColumns()));

        delegate = new UnsortedReducer(ctx);
    }

    /** {@inheritDoc} */
    @Override protected UnsortedReducer reducer() {
        return delegate;
    }

    /** {@inheritDoc} */
    @Override public double getCost(Session ses, int[] masks, TableFilter[] filters, int filter, SortOrder sortOrder,
        AllColumnsForPlan allColumnsSet) {
        return getCostRangeIndex(masks, getRowCountApproximation(ses), filters, filter, sortOrder, true, allColumnsSet);
    }
}
