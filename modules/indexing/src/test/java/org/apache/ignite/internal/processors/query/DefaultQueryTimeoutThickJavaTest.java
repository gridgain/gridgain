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

package org.apache.ignite.internal.processors.query;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.GridTestUtils;

public class DefaultQueryTimeoutThickJavaTest extends DefaultQueryTimeoutTest {
    private final boolean lazy;

    public DefaultQueryTimeoutThickJavaTest() {
        this(false, false);
    }

    protected DefaultQueryTimeoutThickJavaTest(boolean updateQuery, boolean lazy) {
        super(updateQuery);

        this.lazy = lazy;
    }

    @Override protected void prepareQueryExecution() throws Exception {
        super.prepareQueryExecution();

        startClientGrid(10);
    }

    @Override protected void executeQuery(String sql) throws Exception {
        executeQuery0(new SqlFieldsQuery(sql).setLazy(lazy));
    }

    @Override protected void executeQuery(String sql, long timeout) throws Exception {
        executeQuery0(new SqlFieldsQuery(sql)
            .setLazy(lazy)
            .setTimeout((int)timeout, TimeUnit.MILLISECONDS));
    }

    private void executeQuery0(SqlFieldsQuery qry) throws Exception {
        IgniteEx cli = grid(10);

        cli.context().query().querySqlFields(qry, false).getAll();
    }

    @Override protected void assertQueryCancelled(Callable<?> c) {
        // t0d0 check thrown exception in lazy mode
        GridTestUtils.assertThrows(log, c, Exception.class, "cancel");
    }
}
