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
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.Callable;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ClientConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.testframework.GridTestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

/**
 * JDBC driver transparent reconnection test with single address.
 */
public class JdbcThinConnectionFailoverTest extends JdbcThinAbstractSelfTest {
    /** Nodes count. */
    private static final int NODES_CNT = 2;

    /** */
    private static final String CONNECT_URL = "jdbc:ignite:thin://127.0.0.1:"
        + ClientConnectorConfiguration.DFLT_PORT;

    /** {@inheritDoc} */
    @SuppressWarnings("deprecation")
    @Override protected IgniteConfiguration getConfiguration(String name) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(name);

        cfg.setCacheConfiguration(cacheConfiguration(DEFAULT_CACHE_NAME));

        cfg.setMarshaller(new BinaryMarshaller());

        cfg.setClientConnectorConfiguration(new ClientConnectorConfiguration());

        return cfg;
    }

    /**
     * @param name Cache name.
     * @return Cache configuration.
     * @throws Exception In case of error.
     */
    private CacheConfiguration cacheConfiguration(@NotNull String name) throws Exception {
        CacheConfiguration cfg = defaultCacheConfiguration();

        cfg.setName(name);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        startGrids(NODES_CNT);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    @Test
    public void testReconnectAfterRestart() throws Exception {
        try (Connection conn = DriverManager.getConnection(CONNECT_URL)) {
            Statement stmt1 = conn.createStatement();

            stmt1.execute("CREATE TABLE TEST (ID INT, NAME VARCHAR, PRIMARY KEY(ID));");

            stopGrid(0);
            startGrid(0);

            Statement stmt2 = conn.createStatement();

            stmt2.execute("INSERT INTO TEST VALUES (1, 1)");
            stmt2.execute("SELECT 1");

            GridTestUtils.assertThrows(log, new Callable<Object>() {
                @Override public Object call() throws Exception {
                    stmt1.execute("SELECT 1");

                    return null;
                }
            }, SQLException.class, "Statement was closed due to a disconnection from the server");
        }
    }

    @Test
    public void testReconnectDuringRestart() throws Exception {
        try (Connection conn = DriverManager.getConnection(CONNECT_URL)) {
            Statement stmt1 = conn.createStatement();

            stmt1.execute("CREATE TABLE TEST (ID INT, NAME VARCHAR, PRIMARY KEY(ID));");

            stopGrid(0);

            Statement stmt3 = conn.createStatement();

            GridTestUtils.assertThrows(log, new Callable<Object>() {
                @Override public Object call() throws Exception {
                    stmt3.execute("INSERT INTO TEST VALUES(1, 1)");

                    return null;
                }
            }, SQLException.class, "Failed to connect to server");

            GridTestUtils.assertThrows(log, new Callable<Object>() {
                @Override public Object call() throws Exception {
                    stmt3.execute("SELECT 1");

                    return null;
                }
            }, SQLException.class, "Failed to connect to server");

            startGrid(0);

            stmt3.executeUpdate("INSERT INTO TEST VALUES(1, 1)");

            stmt3.execute("SELECT 1");

            Statement stmt2 = conn.createStatement();

            stmt2.execute("SELECT 1");
            stmt2.execute("SELECT 1");

            GridTestUtils.assertThrows(log, new Callable<Object>() {
                @Override public Object call() throws Exception {
                    stmt1.execute("SELECT 1");

                    return null;
                }
            }, SQLException.class, "Statement was closed due to a disconnection from the server");
        }
    }
}
