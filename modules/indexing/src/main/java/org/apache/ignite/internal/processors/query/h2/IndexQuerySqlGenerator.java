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

package org.apache.ignite.internal.processors.query.h2;

import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.query.IndexQuery;
import org.apache.ignite.cache.query.IndexQueryCriterion;
import org.apache.ignite.internal.cache.query.SqlBuilderContext;
import org.apache.ignite.internal.cache.query.SqlIndexQueryCriterion;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Table;
import org.apache.ignite.internal.util.typedef.internal.SB;
import org.gridgain.internal.h2.index.Index;
import org.gridgain.internal.h2.result.SortOrder;
import org.gridgain.internal.h2.table.IndexColumn;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.query.QueryUtils.KEY_FIELD_NAME;
import static org.apache.ignite.internal.processors.query.QueryUtils.VAL_FIELD_NAME;

/** TODO blah-blah. */
public class IndexQuerySqlGenerator {
    private final IndexQuery<?, ?> qry;
    private final String sql;
    private List<Object> args;

    public IndexQuerySqlGenerator(IndexQuery<?, ?> qry, H2TableDescriptor tblDesc) {
        this.qry = qry;

        sql = generateSQL(tblDesc);
    }

    public String sql() {
        return sql;
    }

    public @Nullable Object[] arguments() {
        return args == null ? null : args.toArray();
    }

    private String generateSQL(H2TableDescriptor tblDesc) {
        SB sql = new SB();

        sql.a("SELECT ").a(KEY_FIELD_NAME).a(", ").a(VAL_FIELD_NAME);
        sql.a(" FROM ").a(tblDesc.fullTableName());

        Index index = getIndex(qry.getIndexName(), tblDesc.table());

        if (index != null) {
            sql.a(" USE INDEX (\"").a(index.getName()).a("\")");
        }

        if (qry.getCriteria() != null) {
            List<IndexQueryCriterion> criteria = qry.getCriteria();
            args = new ArrayList<>();

            for (int i = 0; i < criteria.size(); i++) {
                IndexQueryCriterion criterion = criteria.get(i);

                if (i == 0) {
                    sql.a(" WHERE ");
                }
                else {
                    sql.a(" AND ");
                }

                // Ignite's IndexQuery allows to compare with NULL and treats it as the smallest value.
                // While SQL doesn't allow this, we mimic Ignite's behavior for compatibility.
                SqlBuilderContext ctx = new SqlFromIndexQueryBuilderContext(tblDesc, criterion.field());

                if (criterion instanceof SqlIndexQueryCriterion) {
                    sql.a(((SqlIndexQueryCriterion)criterion).toSQL(ctx, args));
                }
                else {
                    // Mimic Ignite error.
                    throw new IllegalArgumentException(
                        String.format("Unknown IndexQuery criterion type [%s]", criterion.getClass().getSimpleName())
                    );
                }
            }
        }

        if (index != null) {
            sql.a(" ORDER BY ");
            // Just insert natural index order.
            IndexColumn[] idxColumns = index.getIndexColumns();

            for (int i = 0; i < idxColumns.length; i++) {
                if (i > 0) {
                    sql.a(", ");
                }

                IndexColumn idxCol = idxColumns[i];

                sql.a('"').a(idxCol.columnName).a('"');

                if (idxCol.sortType == SortOrder.DESCENDING)
                    sql.a(" DESC");
            }
        }

        if (qry.getLimit() != 0) {
            sql.a(" LIMIT ").a(qry.getLimit());
        }

        return sql.toString();
    }

    private @Nullable Index getIndex(@Nullable String idxName, GridH2Table table) {
        if (idxName == null) {
            return null;
        }

        ArrayList<Index> indexes = table.getIndexes();
        String upperCaseIdxName = idxName.toUpperCase();

        for (Index idx : indexes) {
            if (idx.getName().equals(upperCaseIdxName))
                return idx;

            if (idx.getName().equals(idxName))
                return idx;
        }

        throw new IgniteException("Index \"" + upperCaseIdxName + "\" not found.");
    }
}
