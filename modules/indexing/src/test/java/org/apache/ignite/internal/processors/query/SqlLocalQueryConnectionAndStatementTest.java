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

import java.util.Iterator;
import java.util.List;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.processors.cache.index.AbstractIndexingCommonTest;
import org.junit.Test;

/**
 * Test for statement reuse.
 */
public class SqlLocalQueryConnectionAndStatementTest extends AbstractIndexingCommonTest {
    /** {@inheritDoc} */
    @Override public void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrids(1);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        super.afterTestsStopped();
    }

    /**
     */
    @Test
    public void testReplicated() {
        sql("CREATE TABLE repl_tbl (id LONG PRIMARY KEY, val LONG) WITH \"template=replicated\"").getAll();

        try {
            for (int i = 0; i < 10; i++)
                sql("insert into repl_tbl(id,val) VALUES(" + i + "," + i + ")").getAll();

            Iterator<List<?>> it0 = sql(new SqlFieldsQuery("SELECT * FROM repl_tbl where id > ?").setArgs(1)).iterator();

            it0.next();

            sql(new SqlFieldsQuery("SELECT * FROM repl_tbl where id > ?").setArgs(1)).getAll();

            it0.next();
        }
        finally {
            sql("DROP TABLE repl_tbl").getAll();
        }
    }

    /**
     */
    @Test
    public void testLocalQuery() {
        sql("CREATE TABLE tbl (id LONG PRIMARY KEY, val LONG)").getAll();

        try {
            for (int i = 0; i < 10; i++)
                sql("insert into tbl(id,val) VALUES(" + i + "," + i + ")").getAll();

            Iterator<List<?>> it0 = sql(
                new SqlFieldsQuery("SELECT * FROM tbl where id > ?")
                    .setArgs(1)
                    .setLocal(true))
                .iterator();

            it0.next();

            sql(new SqlFieldsQuery("SELECT * FROM tbl where id > ?").setArgs(1).setLocal(true)).getAll();

            it0.next();
        }
        finally {
            sql("DROP TABLE tbl").getAll();
        }
    }

    /**
     * @param sql SQL query.
     * @return Results.
     */
    private FieldsQueryCursor<List<?>> sql(String sql) {
        return sql(new SqlFieldsQuery(sql));
    }

    /**
     * @param qry SQL query.
     * @return Results.
     */
    private FieldsQueryCursor<List<?>> sql(SqlFieldsQuery qry) {
        GridQueryProcessor qryProc = grid(0).context().query();

        return qryProc.querySqlFields(qry, true);
    }
}
