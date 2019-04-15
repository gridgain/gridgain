/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 * 
 * Commons Clause Restriction
 * 
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 * 
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 * 
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.processors.query.h2.database;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2IndexBase;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Table;
import org.apache.ignite.internal.util.typedef.F;
import org.h2.engine.Session;
import org.h2.result.SortOrder;
import org.h2.table.Column;
import org.h2.table.IndexColumn;
import org.h2.table.TableFilter;

/**
 * H2 tree index base.
 */
public abstract class H2TreeIndexBase extends GridH2IndexBase {
    /** Default value for {@code IGNITE_MAX_INDEX_PAYLOAD_SIZE} */
    public static final int IGNITE_MAX_INDEX_PAYLOAD_SIZE_DEFAULT = 10;

    /**
     * Constructor.
     *
     * @param tbl Table.
     */
    protected H2TreeIndexBase(GridH2Table tbl) {
        super(tbl);
    }

    /**
     * @return Inline size.
     */
    public abstract int inlineSize();

    /** {@inheritDoc} */
    @Override public double getCost(Session ses, int[] masks, TableFilter[] filters, int filter, SortOrder sortOrder,
        HashSet<Column> allColumnsSet) {
        long rowCnt = getRowCountApproximation();

        double baseCost = getCostRangeIndex(masks, rowCnt, filters, filter, sortOrder, false, allColumnsSet);

        int mul = getDistributedMultiplier(ses, filters, filter);

        return mul * baseCost;
    }

    /**
     * @param cols Columns array.
     * @return List of {@link InlineIndexHelper} objects.
     */
    protected List<InlineIndexHelper> getAvailableInlineColumns(IndexColumn[] cols) {
        List<InlineIndexHelper> res = new ArrayList<>();

        for (IndexColumn col : cols) {
            if (!InlineIndexHelper.AVAILABLE_TYPES.contains(col.column.getType())) {
                warnCantBeInlined(col);

                break;
            }

            InlineIndexHelper idx = new InlineIndexHelper(
                col.columnName,
                col.column.getType(),
                col.column.getColumnId(),
                col.sortType,
                table.getCompareMode());

            res.add(idx);
        }

        return res;
    }

    /**
     * @param col Indexed column which can't be inlined.
     */
    protected void warnCantBeInlined(IndexColumn col) {
        // No-op.
    }

    /**
     * @param inlineIdxs Inline index helpers.
     * @param cfgInlineSize Inline size from cache config.
     * @param cacheConf Cache configuration.
     * @return Inline size.
     */
    protected int computeInlineSize(List<InlineIndexHelper> inlineIdxs, int cfgInlineSize,
        CacheConfiguration<?, ?> cacheConf) {
        int confSize = cacheConf.getSqlIndexMaxInlineSize();

        int propSize = confSize == -1
            ? IgniteSystemProperties.getInteger(IgniteSystemProperties.IGNITE_MAX_INDEX_PAYLOAD_SIZE, IGNITE_MAX_INDEX_PAYLOAD_SIZE_DEFAULT)
            : confSize;

        if (cfgInlineSize == 0)
            return 0;

        if (F.isEmpty(inlineIdxs))
            return 0;

        if (cfgInlineSize == -1) {
            if (propSize == 0)
                return 0;

            int size = 0;

            for (InlineIndexHelper idxHelper : inlineIdxs) {
                if (idxHelper.size() <= 0) {
                    size = propSize;
                    break;
                }
                // 1 byte type + size
                size += idxHelper.size() + 1;
            }

            return Math.min(PageIO.MAX_PAYLOAD_SIZE, size);
        }
        else
            return Math.min(PageIO.MAX_PAYLOAD_SIZE, cfgInlineSize);
    }

    /** {@inheritDoc} */
    @Override public boolean canGetFirstOrLast() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public long getRowCountApproximation() {
        return 10_000; // TODO
    }
}
