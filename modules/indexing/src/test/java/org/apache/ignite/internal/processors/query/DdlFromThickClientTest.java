/*
 * Copyright 2024 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.internal.processors.query;

import java.util.List;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.index.AbstractIndexingCommonTest;
import org.junit.Test;

/**
 * Tests execution of DDL queries from client node.
 */
public class DdlFromThickClientTest extends AbstractIndexingCommonTest {

    private static final String TABLE = "table_foo";

    private static final String CREATE = "CREATE TABLE IF NOT EXISTS " + TABLE + " (\n" +
        "\tid VARCHAR PRIMARY KEY,\n" +
        "\ttitle VARCHAR\n" +
        ")";

    private static final String DROP_COLUMN = "ALTER TABLE " + TABLE + " DROP COLUMN IF EXISTS title";

    /**
     * Make sure that thick client is able to run DDLs when no cache instance has been initiated.
     * I.e. without explicit calls to <code>ignite.cache(..)</code> before running DDLs,
     * client node has nullable cache contexts, that might trigger NPE.
     */
    @Test
    public void testDropColumn() throws Exception {
        IgniteEx srv = startGrid("srv");
        sql(srv, CREATE);

        IgniteEx client = startClientGrid("client");
        sql(client, DROP_COLUMN);
    }

    /**
     * @param ign  Node.
     * @param sql  SQL query.
     * @param args Query parameters.
     * @return Results cursor.
     */
    protected FieldsQueryCursor<List<?>> sql(IgniteEx ign, String sql, Object... args) {
        return ign.context().query().querySqlFields(new SqlFieldsQuery(sql)
            .setLazy(true)
            .setArgs(args), false);
    }
}
