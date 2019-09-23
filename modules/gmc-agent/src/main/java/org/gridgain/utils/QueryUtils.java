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

package org.gridgain.utils;

import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.processors.query.GridQueryFieldMetadata;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.visor.query.VisorQueryUtils;
import org.gridgain.dto.action.query.QueryArgument;
import org.gridgain.dto.action.query.QueryField;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * SQL query utils.
 */
public class QueryUtils {
    /**
     * @param meta Meta.
     * @return List of columns.
     */
    public static List<QueryField> getColumns(List<GridQueryFieldMetadata> meta) {
        List<QueryField> res = new ArrayList<>(meta.size());

        for (GridQueryFieldMetadata col : meta) {
            res.add(
                    new QueryField()
                            .setSchemaName(col.schemaName())
                            .setTypeName(col.typeName())
                            .setFieldName(col.fieldName())
                            .setFieldTypeName(col.fieldTypeName())
            );
        }

        return res;
    }

    /**
     * @param arg Argument.
     * @return Prepared query.
     */
    public static SqlFieldsQuery prepareQuery(QueryArgument arg) {
        SqlFieldsQuery qry = new SqlFieldsQuery(arg.getQueryText());

        qry.setPageSize(arg.getPageSize());
        qry.setLocal(arg.isLocal());
        qry.setDistributedJoins(arg.isDistributedJoins());
        qry.setCollocated(arg.isCollocated());
        qry.setEnforceJoinOrder(arg.isEnforceJoinOrder());
        qry.setLazy(arg.isLazy());

        if (!F.isEmpty(arg.getCacheName()))
            qry.setSchema(arg.getCacheName());

        return qry;
    }

    /**
     * Collects rows from sql query future, first time creates meta and column names arrays.
     *
     * @param itr Result set iterator.
     * @param pageSize Number of rows to fetch.
     * @return Fetched rows.
     */
    public static List<Object[]> fetchSqlQueryRows(Iterator itr, int pageSize) {
        List<Object[]> rows = new ArrayList<>();

        int cnt = 0;

        Iterator<List<?>> sqlItr = (Iterator<List<?>>)itr;

        while (itr.hasNext() && cnt < pageSize) {
            List<?> next = sqlItr.next();

            int sz = next.size();

            Object[] row = new Object[sz];

            for (int i = 0; i < sz; i++)
                row[i] = VisorQueryUtils.convertValue(next.get(i));

            rows.add(row);

            cnt++;
        }

        return rows;
    }
}
