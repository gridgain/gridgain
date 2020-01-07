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

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class DefaultQueryTimeoutThickJavaTest extends DefaultQueryTimeoutTest {
    @Parameterized.Parameters(name = "Lazy={0}")
    public static List<Object[]> params() {
        return Arrays.asList(new Object[][] {
            {false},
            {true}
        });
    }

    @Parameterized.Parameter
    public boolean lazy;

    @Override protected void executeQuery(String sql) {
        SqlFieldsQuery qry = new SqlFieldsQuery(sql).setLazy(lazy);

        grid(0).context().query().querySqlFields(qry, false).getAll();
    }

    @Override protected void executeQuery(String sql, long timeout) {
        SqlFieldsQuery qry = new SqlFieldsQuery(sql).setTimeout((int)timeout, TimeUnit.MILLISECONDS);

        grid(0).context().query().querySqlFields(qry, false).getAll();
    }

    @Override protected void assertQueryCancelled(Callable<?> c) {
        // t0d0 check thrown exception in lazy mode
        GridTestUtils.assertThrows(log, c, Exception.class, "cancel");
    }
}
