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
import java.sql.Statement;
import java.util.Collections;
import java.util.List;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.jdbc.thin.JdbcThinConnection;
import org.apache.ignite.internal.jdbc.thin.JdbcThinTcpIo;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Tests for cancel multiple statement DML queries on client disconnect.
 */
public class JdbcThinMultiStatementDmlCancelOnDisconnectTest extends GridCommonAbstractTest {
    /** Statements count. */
    private static final int STMTS_CNT = 1000;

    /** {@inheritDoc} */
    @Override public void beforeTestsStarted() throws Exception {
        startGrids(2);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setCacheConfiguration(
                new CacheConfiguration<>()
                    .setName("TEST")
                    .setQueryEntities(Collections.singleton(
                        new QueryEntity(Integer.class.getName(), "VAL_TYPE")
                            .setTableName("TEST")
                            .addQueryField("ID", Integer.class.getName(), null)
                            .addQueryField("VAL_INT", Integer.class.getName(), null)
                            .addQueryField("VAL_STR", String.class.getName(), null)
                            .setKeyFieldName("ID")
                    ))
                    .setSqlSchema("PUBLIC")
                    .setSqlFunctionClasses(GridTestUtils.SqlTestFunctions.class)
            );
    }

    /**
     * Assert that script containing both h2 and non h2 (native) sql statements is handled correctly.
     */
    @Test
    public void test() throws Exception {
        final String sql = buildQuery(100_000);

        Connection c = GridTestUtils.connect(grid(0), null);
        final Statement stmt = c.createStatement();

        IgniteInternalFuture f = GridTestUtils.runAsync(() -> stmt.executeUpdate(sql));

        assertTrue(GridTestUtils.waitForCondition(() -> !sql("select * from test").isEmpty(), 5000));

        JdbcThinTcpIo io = GridTestUtils.getFieldValue(c, "singleIo");

        io.close();

        U.sleep(1000);

        int size0 = sql("select * from test").size();

        U.sleep(1000);

        assertTrue("The DML isn't stopped", sql("select * from test").size() == size0);
    }

    /**
     * @param duration
     * @return
     */
    private String buildQuery(int duration) {
        double dDelay = (double)duration / STMTS_CNT;

        int oneSavePeriod = (int)(1.0 / (dDelay - Math.floor(dDelay)));
        int delay = (int)dDelay;

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < STMTS_CNT; ++i) {
            int stmtDelay = delay + (oneSavePeriod < STMTS_CNT && i % oneSavePeriod == 0 ? 1 : 0);

            sb.append("MERGE INTO TEST (ID, VAL_INT, VAL_STR) VALUES (")
                .append(i)
                .append(", delay(")
                .append(stmtDelay)
                .append("), 'val_")
                .append(i)
                .append("');");
        }

        return sb.toString();
    }

    /**
     * Execute sql script using thin driver.
     */
    private void executeUpdate(String sql) throws Exception {
        try (Connection c = GridTestUtils.connect(grid(0), null)) {
            try (Statement stmt = c.createStatement()) {
                stmt.executeUpdate(sql);
            }
        }
    }

    /**
     * Execute sql script using thin driver.
     */
    private List<List<?>> sql(String sql) {
        return grid(0).context().query().querySqlFields(new SqlFieldsQuery(sql), false).getAll();
    }
}
