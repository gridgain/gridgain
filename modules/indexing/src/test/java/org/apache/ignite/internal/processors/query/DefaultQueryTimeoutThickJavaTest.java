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
import org.apache.ignite.cache.query.QueryCancelledException;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.testframework.GridTestUtils;

public class DefaultQueryTimeoutThickJavaTest extends DefaultQueryTimeoutTest {
    @Override protected void executeQuery(String sql) {
        SqlFieldsQuery qry = new SqlFieldsQuery(sql);

        grid(0).context().query().querySqlFields(qry, false).getAll();
    }

    @Override protected void executeQuery(String sql, long timeout) {
        SqlFieldsQuery qry = new SqlFieldsQuery(sql).setTimeout((int)timeout, TimeUnit.MILLISECONDS);

        grid(0).context().query().querySqlFields(qry, false).getAll();
    }

    @Override protected void assertQueryCancelled(Callable<?> c) {
        GridTestUtils.assertThrowsWithCause(c, QueryCancelledException.class);
    }
}
