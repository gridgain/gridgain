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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

// t0d0 search existing tests
public class SqlExplicitTimeoutValidationTest extends GridCommonAbstractTest {
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    @Test
    public void testJavaApi() throws Exception {
        // Applicable for thick and thin java clients.
        GridTestUtils.assertThrowsWithCause(
            () -> new SqlFieldsQuery("select * from test").setTimeout(-1, TimeUnit.MILLISECONDS),
            IllegalArgumentException.class);
    }

    @Test
    public void testThinJdbcApi() throws Exception {
        startGrid(0);

        GridTestUtils.assertThrowsWithCause(() -> {
            try (Connection conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1")) {
                conn.createStatement().setQueryTimeout(-1);
            }

            return null;
        }, SQLException.class);
    }
}
