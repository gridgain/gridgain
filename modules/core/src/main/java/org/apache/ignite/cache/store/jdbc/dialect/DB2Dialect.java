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

package org.apache.ignite.cache.store.jdbc.dialect;

import java.util.Collection;
import org.apache.ignite.internal.util.typedef.C1;
import org.apache.ignite.internal.util.typedef.F;

/**
 * A dialect compatible with the IBM DB2 database.
 */
public class DB2Dialect extends BasicJdbcDialect {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override public String loadCacheSelectRangeQuery(String fullTblName, Collection<String> keyCols) {
        String cols = mkString(keyCols, ",");

        return String.format("SELECT %1$s FROM (SELECT %1$s, ROW_NUMBER() OVER(ORDER BY %1$s) AS rn FROM %2$s) WHERE mod(rn, ?) = 0 ORDER BY %1$s",
            cols, fullTblName);
    }

    /** {@inheritDoc} */
    @Override public boolean hasMerge() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public String mergeQuery(String fullTblName, Collection<String> keyCols, Collection<String> uniqCols) {
        Collection<String> cols = F.concat(false, keyCols, uniqCols);

        String colsLst = mkString(cols, ", ");

        String match = mkString(keyCols, new C1<String, String>() {
            @Override public String apply(String col) {
                return String.format("t.%s=v.%s", col, col);
            }
        }, "", " AND ", "");

        String setCols = mkString(uniqCols, new C1<String, String>() {
            @Override public String apply(String col) {
                return String.format("t.%s = v.%s", col, col);
            }
        }, "", ", ", "");

        String valuesCols = mkString(cols, new C1<String, String>() {
            @Override public String apply(String col) {
                return "v." + col;
            }
        }, "", ", ", "");

        return String.format("MERGE INTO %s t" +
                " USING (VALUES(%s)) AS v (%s)" +
                "  ON %s" +
                " WHEN MATCHED THEN" +
                "  UPDATE SET %s" +
                " WHEN NOT MATCHED THEN" +
                "  INSERT (%s) VALUES (%s)", fullTblName, repeat("?", cols.size(), "", ",", ""), colsLst,
            match, setCols, colsLst, valuesCols);
    }
}
