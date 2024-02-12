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
import org.apache.ignite.internal.cache.query.SqlIndexQueryCriterion;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Table;
import org.apache.ignite.internal.util.typedef.internal.SB;
import org.gridgain.internal.h2.index.Index;
import org.gridgain.internal.h2.result.SortOrder;
import org.gridgain.internal.h2.table.IndexColumn;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.query.QueryUtils.KEY_FIELD_NAME;
import static org.apache.ignite.internal.processors.query.QueryUtils.VAL_FIELD_NAME;

/**
 * Utility class to generate SQL query from {@link IndexQuery}.
 */
public class IndexQuerySqlGenerator {
    /**
     * Generates SQL query from the provided {@link IndexQuery}.
     *
     * @param qry Index query.
     * @param tblDesc Target table descriptor.
     * @return SQL query text.
     */
    public static SqlGeneratorResult generate(IndexQuery<?, ?> qry, H2TableDescriptor tblDesc) {
        SB buffer = new SB();

        buffer.a("SELECT ").a(KEY_FIELD_NAME).a(", ").a(VAL_FIELD_NAME);
        buffer.a(" FROM ").a(tblDesc.fullTableName());

        Index index = getIndex(qry.getIndexName(), tblDesc.table());

        if (index != null) {
            buffer.a(" USE INDEX (\"").a(index.getName()).a("\")");
        }

        List<Object> args = null;

        if (qry.getCriteria() != null) {
            List<IndexQueryCriterion> criteria = qry.getCriteria();
            args = new ArrayList<>();

            for (int i = 0; i < criteria.size(); i++) {
                IndexQueryCriterion criterion = criteria.get(i);

                if (i == 0) {
                    buffer.a(" WHERE ");
                }
                else {
                    buffer.a(" AND ");
                }

                if (criterion instanceof SqlIndexQueryCriterion) {
                    // Ignite's IndexQuery allows to compare with NULL and treats it as the smallest value.
                    // While SQL doesn't allow this, we mimic Ignite's behavior for compatibility.
                    String condition = ((SqlIndexQueryCriterion)criterion)
                        .toSql(new IndexQuerySqlBuilderContext(tblDesc.table(), args));

                    buffer.a(condition);
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
            buffer.a(" ORDER BY ");
            // Just insert natural index order.
            IndexColumn[] idxColumns = index.getIndexColumns();

            for (int i = 0; i < idxColumns.length; i++) {
                if (i > 0) {
                    buffer.a(", ");
                }

                IndexColumn idxCol = idxColumns[i];

                buffer.a('"').a(idxCol.columnName).a('"');

                if (idxCol.sortType == SortOrder.DESCENDING)
                    buffer.a(" DESC");
            }
        }

        if (qry.getLimit() != 0) {
            buffer.a(" LIMIT ").a(qry.getLimit());
        }

        return new SqlGeneratorResult(buffer.toString(), args);
    }

    private static @Nullable Index getIndex(@Nullable String idxName, GridH2Table table) {
        if (idxName == null) {
            return null;
        }

        ArrayList<Index> indexes = table.getIndexes();
        String upperCaseIdxName = idxName.toUpperCase();

        for (Index idx : indexes) {
            if (idx.getName().equals(idxName))
                return idx;

            if (idx.getName().equals(upperCaseIdxName))
                return idx;
        }

        throw new IgniteException("Index \"" + upperCaseIdxName + "\" not found.");
    }

    static class SqlGeneratorResult {
        private final String sql;

        private final @Nullable List<Object> arguments;

        public SqlGeneratorResult(String sql, @Nullable List<Object> arguments) {
            this.sql = sql;
            this.arguments = arguments;
        }

        public String sql() {
            return sql;
        }

        public @Nullable Object[] arguments() {
            return arguments == null ? null : arguments.toArray();
        }
    }
}
