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
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.internal.processors.query.h2.database.inlinecolumn.BytesInlineIndexColumn;
import org.apache.ignite.internal.processors.query.h2.database.inlinecolumn.InlineIndexColumnFactory;
import org.apache.ignite.internal.processors.query.h2.database.inlinecolumn.StringInlineIndexColumn;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2IndexBase;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Table;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.gridgain.internal.h2.command.dml.AllColumnsForPlan;
import org.gridgain.internal.h2.engine.Session;
import org.gridgain.internal.h2.index.IndexType;
import org.gridgain.internal.h2.result.SortOrder;
import org.gridgain.internal.h2.table.IndexColumn;
import org.gridgain.internal.h2.table.Table;
import org.gridgain.internal.h2.table.TableFilter;

/**
 * H2 tree index base.
 */
public abstract class H2TreeIndexBase extends GridH2IndexBase {
    /**
     * Default sql index size for types with variable length (such as String or byte[]).
     * Note that effective length will be lower, because 3 bytes will be taken for the inner representation of variable type.
     */
    static final int IGNITE_VARIABLE_TYPE_DEFAULT_INDEX_SIZE = 10;

    /** SQL pattern for the String with defined length. */
    static final Pattern STRING_WITH_LENGTH_SQL_PATTERN = Pattern.compile("\\w+\\((\\d+)\\)");

    /**
     * To avoid tree corruption, at least two items should fit into one page.
     * So maximum payload size equals: P = (PS - H - 3L) / 2 - X , where P - Payload size, PS - page size, H - page
     * header size, L - size of the child link, X - overhead per item. Calculated for pageSize = 1KB with
     * KeystoreEncryptionSpi and MVCC enabled.
     */
    public static final int MAX_INLINE_SIZE = 427;

    /**
     * Constructor.
     *
     * @param tbl Table.
     * @param name Index name.
     * @param cols Indexed columns.
     * @param type Index type.
     */
    protected H2TreeIndexBase(GridH2Table tbl, String name, IndexColumn[] cols, IndexType type) {
        super(tbl, name, cols, type);
    }

    /**
     * @return Inline size.
     */
    public abstract int inlineSize();

    /** {@inheritDoc} */
    @Override public double getCost(Session ses, int[] masks, TableFilter[] filters, int filter,
        SortOrder sortOrder, AllColumnsForPlan allColumnsSet) {

        long rowCnt = getRowCountApproximation(ses);

        double baseCost = costRangeIndex(ses, masks, rowCnt, filters, filter, sortOrder, false, allColumnsSet);

        int mul = getDistributedMultiplier(ses, filters, filter);

        return mul * baseCost;
    }

    /** {@inheritDoc} */
    @Override public boolean canGetFirstOrLast() {
        return true;
    }

    /**
     * @param inlineIdxs Inline index helpers.
     * @param cfgInlineSize Inline size from cache config.
     * @param cfgMaxInlineSize Max inline size from cache config.
     * @return Inline size.
     */
    static int computeInlineSize(
            String name,
            List<InlineIndexColumn> inlineIdxs,
            int cfgInlineSize,
            int cfgMaxInlineSize,
            IgniteLogger log) {
        if (cfgInlineSize == 0)
            return 0;

        if (F.isEmpty(inlineIdxs))
            return 0;

        boolean fixedSize = true;

        int maxSize = cfgMaxInlineSize;

        if (cfgMaxInlineSize > MAX_INLINE_SIZE) {
            log.warning("Cache sqlIdxMaxInlineSize exceeds maximum allowed size. Ignoring" +
                    "[index=" + name + ", maxInlineSize=" + maxSize + ", maxAllowedInlineSize=" + MAX_INLINE_SIZE + ']');
            maxSize = MAX_INLINE_SIZE;
        }

        if (cfgMaxInlineSize == -1) {
            int propSize = IgniteSystemProperties.getInteger(IgniteSystemProperties.IGNITE_MAX_INDEX_PAYLOAD_SIZE,
                    MAX_INLINE_SIZE);

            if (propSize > MAX_INLINE_SIZE)
                log.warning("System property IGNITE_MAX_INDEX_PAYLOAD_SIZE exceeds maximum allowed size. Ignoring" +
                        "[index=" + name + ", propertySize=" + propSize + ", maxAllowedInlineSize=" + MAX_INLINE_SIZE + ']');

            maxSize = Math.min(propSize, MAX_INLINE_SIZE);
        }

        if (maxSize == 0)
            return 0;

        int size = 0;

        for (InlineIndexColumn idxHelper : inlineIdxs) {
            // for variable types - default variable size, for other types - type's size + type marker
            int sizeInc = idxHelper.size() < 0 ? IGNITE_VARIABLE_TYPE_DEFAULT_INDEX_SIZE : idxHelper.size() + 1;

            fixedSize &= idxHelper.size() != -1;

            if (idxHelper instanceof StringInlineIndexColumn || idxHelper instanceof BytesInlineIndexColumn) {
                String sql = idxHelper.columnSql();

                if (sql != null) {
                    Matcher m = STRING_WITH_LENGTH_SQL_PATTERN.matcher(sql);

                    if (m.find())
                        // if column has defined length we use it as default + 3 bytes for the inner info of the variable type
                        sizeInc = Integer.parseInt(m.group(1)) + 3;
                }
            }

            size += sizeInc;
        }

        if (cfgInlineSize != -1) {
            size = Math.min(size, maxSize);

            if (fixedSize && size < cfgInlineSize) {
                log.warning("Explicit INLINE_SIZE for fixed size index item is too big. " +
                        "This will lead to wasting of space inside index pages. Ignoring " +
                        "[index=" + name + ", explicitInlineSize=" + cfgInlineSize + ", realInlineSize=" + size + ']');

                return size;
            }

            if (cfgInlineSize > maxSize)
                log.warning("Explicit INLINE_SIZE exceeds maximum size. Ignoring " +
                        "[index=" + name + ", explicitInlineSize=" + cfgInlineSize + ", maxInlineSize=" + maxSize + ']');

            return Math.min(cfgInlineSize, maxSize);
        }

        if (size > maxSize) {
            log.warning("Calculated inline size exceeds maximum size. Ignoring " +
                    "[index=" + name + ", calculatedInlineSize=" + size + ", maxInlineSize=" + maxSize + ']');

            return maxSize;
        }

        return size;
    }

    /**
     * Creates inline helper list for provided column list.
     *
     * @param affinityKey Affinity key.
     * @param cacheName Cache name.
     * @param idxName Index name.
     * @param log Logger.
     * @param pk Pk.
     * @param tbl Table.
     * @param cols Columns.
     * @param factory Factory.
     * @param inlineObjHashSupported Whether hash inlining is supported or not.
     * @return List of {@link InlineIndexColumn} objects.
     */
    static List<InlineIndexColumn> getAvailableInlineColumns(boolean affinityKey, String cacheName,
        String idxName, IgniteLogger log, boolean pk, Table tbl, IndexColumn[] cols,
        InlineIndexColumnFactory factory, boolean inlineObjHashSupported) {
        ArrayList<InlineIndexColumn> res = new ArrayList<>(cols.length);

        for (IndexColumn col : cols) {
            if (!InlineIndexColumnFactory.typeSupported(col.column.getType().getValueType())) {
                String idxType = pk ? "PRIMARY KEY" : affinityKey ? "AFFINITY KEY (implicit)" : "SECONDARY";

                U.warn(log, "Column cannot be inlined into the index because it's type doesn't support inlining, " +
                    "index access may be slow due to additional page reads (change column type if possible) " +
                    "[cacheName=" + cacheName +
                    ", tableName=" + tbl.getName() +
                    ", idxName=" + idxName +
                    ", idxType=" + idxType +
                    ", colName=" + col.columnName +
                    ", columnType=" + InlineIndexColumnFactory.nameTypeByCode(col.column.getType().getValueType()) + ']'
                );

                res.trimToSize();

                break;
            }

            res.add(factory.createInlineHelper(col.column, inlineObjHashSupported));
        }

        return res;
    }
}
