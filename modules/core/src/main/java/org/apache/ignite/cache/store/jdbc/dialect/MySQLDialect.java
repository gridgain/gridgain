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
 * A dialect compatible with the MySQL database.
 */
public class MySQLDialect extends BasicJdbcDialect {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    public MySQLDialect() {
        // Workaround for known issue with MySQL large result set.
        // See: http://dev.mysql.com/doc/connector-j/5.1/en/connector-j-reference-implementation-notes.html
        fetchSize = Integer.MIN_VALUE;
    }

    /** {@inheritDoc} */
    @Override public String escape(String ident) {
        return '`' + ident + '`';
    }

    /** {@inheritDoc} */
    @Override public String loadCacheSelectRangeQuery(String fullTblName, Collection<String> keyCols) {
        String cols = mkString(keyCols, ",");

        return String.format("SELECT %s " +
            "FROM (SELECT %s, @rownum := @rownum + 1 AS rn FROM %s, (SELECT @rownum := 0) r ORDER BY %s) as r " +
            "WHERE mod(rn, ?) = 0", cols, cols, fullTblName, cols);
    }

    /** {@inheritDoc} */
    @Override public boolean hasMerge() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public String mergeQuery(String fullTblName, Collection<String> keyCols, Collection<String> uniqCols) {
        Collection<String> cols = F.concat(false, keyCols, uniqCols);

        String updPart = mkString(uniqCols, new C1<String, String>() {
            @Override public String apply(String col) {
                return String.format("%s = VALUES(%s)", col, col);
            }
        }, "", ", ", "");

        return String.format("INSERT INTO %s (%s) VALUES (%s) ON DUPLICATE KEY UPDATE %s", fullTblName,
            mkString(cols, ", "), repeat("?", cols.size(), "", ",", ""), updPart);
    }
}
