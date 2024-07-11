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

package org.apache.ignite.internal.processors.query.oom;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.SqlConfiguration;
import org.apache.ignite.failure.StopNodeFailureHandler;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.multijvm.IgniteProcessProxy;
import org.junit.Test;

import static org.apache.ignite.testframework.GridTestUtils.assertThrows;

/** Out of memory processing. */
public class OOMLeadsTest extends GridCommonAbstractTest {
    /** Statement. */
    private Statement stmt;

    /** Connection. */
    private Connection conn;

    /** Connection string. */
    private static final String URL = "jdbc:ignite:thin://127.0.0.1:10800..10850/";

    /** {@inheritDoc} */
    @Override protected List<String> additionalRemoteJvmArgs() {
        return Arrays.asList("-Xmx64m", "-Xms64m");
    }

    /** {@inheritDoc} */
    @Override protected boolean isMultiJvm() {
        return true;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        startGrids(2);

        // stop local jvm node
        stopGrid(0);

        connect(URL);

        assert stmt != null;
        assert !stmt.isClosed();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        if (stmt != null && !stmt.isClosed()) {
            stmt.close();

            assert stmt.isClosed();
        }

        conn.close();

        assert stmt.isClosed();
        assert conn.isClosed();

        stopAllGrids();

        IgniteProcessProxy.killAll();

        super.afterTest();
    }

    /** */
    private void connect(String url) throws Exception {
        if (stmt != null)
            stmt.close();

        if (conn != null)
            conn.close();

        conn = DriverManager.getConnection(url);
        conn.setSchema("PUBLIC");

        stmt = conn.createStatement();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setFailureHandler(new StopNodeFailureHandler());

        cfg.setSqlConfiguration(new SqlConfiguration()
                .setSqlOffloadingEnabled(false)
                .setSqlGlobalMemoryQuota("0"));

        return cfg;
    }

    /** Check correct handling for Out of memory. */
    @Test
    public void testOOMQueryHandling() throws Exception {
        // check non oom sql processed correctly
        stmt.execute("select x, space(100+x) as av from system_range(1, 1) group by av");

        // oom lead sql
        assertThrows(null, () ->
                        stmt.execute("select x, space(10000000+x) as av from system_range(1, 1000) group by av"),
                SQLException.class, "Out of memory");

        assertTrue(((IgniteProcessProxy)grid(1)).getProcess().getProcess().isAlive());

        stmt.execute("select x, space(100+x) as av from system_range(1, 1) group by av");
        stmt.execute("create table t(ID INT PRIMARY KEY, VAL INT)");
        stmt.execute("insert into t values(1, 1)");
        ResultSet res = stmt.executeQuery("select count(*) from t");
        assertTrue(res.next());
        assertEquals(1, res.getInt(1));
    }
}
