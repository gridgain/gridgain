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

package org.apache.ignite.jdbc.thin;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.SqlConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/** */
public class JdbcThinDefaultTimeoutTest extends GridCommonAbstractTest {
    /** */
    public static final int ROW_COUNT = 200;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        CacheConfiguration<Object, Object> ccfg = new CacheConfiguration<>(DEFAULT_CACHE_NAME)
            .setIndexedTypes(Integer.class, Integer.class)
            .setSqlSchema("PUBLIC")
            .setSqlFunctionClasses(GridTestUtils.SqlTestFunctions.class);

        return super.getConfiguration(igniteInstanceName)
            .setCacheConfiguration(ccfg)
            .setSqlConfiguration(new SqlConfiguration().setDefaultQueryTimeout(100));
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        IgniteEx ign = startGrid(0);

        Map<Integer, Integer> vals = IntStream.range(0, ROW_COUNT)
            .boxed()
            .collect(Collectors.toMap(Function.identity(), Function.identity()));

        // We need to fill cache with many rows because server-side timeout checks for timeout periodically after
        // loading several rows.
        ign.cache(DEFAULT_CACHE_NAME).putAll(vals);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        super.afterTestsStopped();
    }

    /**
     * Check JDBC query timeout.
     * Steps:
     * - execute query with zero explicit timeout (timeout disabled);
     * - check that query successful.
     */
    @Test
    public void testDefaultTimeoutIgnored() throws Exception {
        try (Connection conn = DriverManager.getConnection("jdbc:ignite:thin://localhost")) {
            Statement stmt = conn.createStatement();

            // Set infinite timeout explicitly.
            stmt.setQueryTimeout(0);

            ResultSet rs = stmt.executeQuery("select _key, _val, delay(5) from Integer");

            int cnt = 0;
            while (rs.next())
                cnt++;

            assertEquals(ROW_COUNT, cnt);

            // assert no exception
        }
    }

    /**
     * Check JDBC query timeout.
     * Steps:
     * - set default timeout to 100 ms;
     * - execute query without explicit timeout;
     * - check that query fails by timeout.
     */
    @Test
    public void testDefaultTimeout() throws Exception {
        try (Connection conn = DriverManager.getConnection("jdbc:ignite:thin://localhost")) {
            Statement stmt = conn.createStatement();

            GridTestUtils.assertThrows(log, () -> {
                    ResultSet rs = stmt.executeQuery("select _key, _val, delay(5) from Integer");

                    int cnt = 0;
                    while (rs.next())
                        cnt++;

                    return null;
                },
                SQLException.class, "The query was cancelled while executing");
        }
    }
}
