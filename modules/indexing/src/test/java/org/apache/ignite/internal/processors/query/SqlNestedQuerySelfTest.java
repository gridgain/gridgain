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

package org.apache.ignite.internal.processors.query;

import java.util.List;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.index.AbstractIndexingCommonTest;
import org.junit.Test;

/**
 * Tests for schemas.
 */
public class SqlNestedQuerySelfTest extends AbstractIndexingCommonTest {
    /** Node. */
    private IgniteEx node;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        node = (IgniteEx)startGrid();

        startGrid(2);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     */
    @Test
    public void testNestingQuery() {
        sql("CREATE TABLE txs(txId INTEGER PRIMARY KEY, created INTEGER)");
        sql("CREATE TABLE ops(id INTEGER PRIMARY KEY, txId INTEGER, stage VARCHAR, tStamp INTEGER)");

        sql("INSERT INTO txs(txId, created) VALUES (1, 599000), (2, 599111), (3, 599234)");
        sql("INSERT INTO ops(id, txId, stage, tStamp) VALUES" +
            " (1, 1, 'NEW', 599686), (2, 1, 'OLD', 599722), (3, 1, 'OLD', 599736), (4, 2, 'NEW', 599767)");

        sql("WITH cacheJoin (txId, stage, tStamp)" +
              " AS (SELECT t.txId, o.stage, o.tStamp FROM txs t INNER JOIN ops o ON t.txId = o.txId)" +
            " SELECT ou.stage, COUNT(*) as cou, SUM(CASE WHEN ou.stage = in.stage THEN 1 ELSE 0 END) AS ttl" +
              " FROM (SELECT txId, stage FROM cacheJoin cte GROUP BY txId, stage) ou" +
                " INNER JOIN (SELECT mx.txId, mx.stage FROM (SELECT txId, tStamp, stage FROM cacheJoin cte) mx" +
                  " INNER JOIN (SELECT txId, MAX(tStamp) AS maxTStamp FROM cacheJoin cte GROUP BY txId) mix" +
                    " ON mx.txId = mix.txId AND mx.tStamp = mix.maxTStamp) in ON ou.txId = in.txId" +
            " GROUP BY ou.stage");
    }

    /**
     * @param sql SQL query.
     * @return Results.
     */
    private List<List<?>> sql(String sql) {
        GridQueryProcessor qryProc = node.context().query();

        SqlFieldsQuery qry = new SqlFieldsQuery(sql).setSchema("PUBLIC");

        return qryProc.querySqlFields(qry, true).getAll();
    }
}
