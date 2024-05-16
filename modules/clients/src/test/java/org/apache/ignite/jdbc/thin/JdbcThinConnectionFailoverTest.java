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
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.apache.ignite.configuration.ClientConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;

/**
 * JDBC driver transparent reconnection test with single address.
 */
public class JdbcThinConnectionFailoverTest extends JdbcThinAbstractSelfTest {
    /** Nodes count. */
    private static final int NODES_CNT = 2;

    /** */
    private static final String STATEMENT_CLOSED_ON_DISCONNECT_MESSAGE =
        "Statement was closed due to a disconnection from the server";

    /** */
    private static final String CONNECT_URL = "jdbc:ignite:thin://127.0.0.1:"
        + ClientConnectorConfiguration.DFLT_PORT;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String name) throws Exception {
        return super.getConfiguration(name)
            .setClientConnectorConfiguration(new ClientConnectorConfiguration());
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
            Statement stmt1 = createTestTable(conn);

            {
                restartGrid(0);

                Statement stmt = conn.createStatement();

                assertEquals(1, stmt.executeUpdate("INSERT INTO TEST VALUES (1, 1)"));
                stmt.execute("SELECT 1");
            }

            {
                PreparedStatement pstmt = conn.prepareStatement("INSERT INTO TEST VALUES (?, ?)");

                pstmt.setInt(1, 2);
                pstmt.setInt(2, 2);

                restartGrid(0);

                assertEquals(1, pstmt.executeUpdate());

                restartGrid(0);

                pstmt.setInt(1, 3);
                pstmt.setInt(2, 3);
                pstmt.addBatch();

                // Batch cannot be re-used after connection reset.
                assertThrowsSql(pstmt::executeBatch, "Failed to communicate with Ignite cluster");
                assertThrowsSql(() -> pstmt.setInt(1, 5), STATEMENT_CLOSED_ON_DISCONNECT_MESSAGE);

                restartGrid(0);

                PreparedStatement pstmt0 = conn.prepareStatement("INSERT INTO TEST VALUES (?, ?)");

                pstmt0.setInt(1, 5);
                pstmt0.setInt(2, 5);
                assertEquals(1, pstmt0.executeUpdate());
                assertEquals(1, pstmt0.getUpdateCount());

                restartGrid(0);

                try (Statement stmt = conn.createStatement()) {
                    try (ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM TEST")) {
                        assertTrue(rs.next());

                        assertEquals(3, rs.getLong(1));
                    }
                }

                // After another statement is executed, the results of the previously created
                // statement cannot be reused, so the statement must be closed.
                assertTrue(pstmt.isClosed());
            }

            assertThrowsSql(() -> stmt1.execute("SELECT 1"), STATEMENT_CLOSED_ON_DISCONNECT_MESSAGE);
        }
    }

    @Test
    public void testReconnectDuringRestart() throws Exception {
        String expErrMessage = "Failed to connect to server";

        try (Connection conn = DriverManager.getConnection(CONNECT_URL)) {
            Statement stmt1 = createTestTable(conn);

            // Basic statements.
            {
                stopGrid(0);

                Statement stmt = conn.createStatement();

                assertThrowsSql(() -> stmt.execute("INSERT INTO TEST VALUES(1, 1)"), expErrMessage);
                assertThrowsSql(() -> stmt.execute("SELECT 1"), expErrMessage);

                startGrid(0);

                stmt.executeUpdate("INSERT INTO TEST VALUES(1, 1)");
                stmt.execute("SELECT 1");

                Statement stmt2 = conn.createStatement();

                stmt2.execute("SELECT 1");
                stmt2.execute("SELECT 1");
            }

            // Prepared statement.
            {
                PreparedStatement pstmt = conn.prepareStatement("INSERT INTO TEST VALUES (?, ?)");

                {
                    pstmt.setInt(1, 2);
                    pstmt.setInt(2, 2);

                    stopGrid(0);

                    assertThrowsSql(pstmt::executeUpdate, expErrMessage);

                    startGrid(0);

                    assertEquals(1, pstmt.executeUpdate());

                }

                {
                    stopGrid(0);

                    pstmt.setInt(1, 3);
                    pstmt.setInt(2, 3);
                    pstmt.addBatch();

                    // Batch query does not support reconnection.
                    assertThrowsSql(pstmt::executeBatch, "Failed to communicate with Ignite cluster");
                    assertTrue(pstmt.isClosed());

                    startGrid(0);

                    pstmt = conn.prepareStatement("INSERT INTO TEST VALUES (?, ?)");

                    pstmt.setInt(1, 3);
                    pstmt.setInt(2, 3);
                    pstmt.addBatch();

                    assertArrayEquals(new int[] {1}, pstmt.executeBatch());
                }

                try (Statement stmt = conn.createStatement()) {
                    try (ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM TEST")) {
                        assertTrue(rs.next());

                        assertEquals(3, rs.getLong(1));
                    }
                }
            }

            assertThrowsSql(() -> stmt1.execute("SELECT 1"), STATEMENT_CLOSED_ON_DISCONNECT_MESSAGE);
        }
    }

    @Test
    public void testReconnectWithTxContextIsNotAllowed() throws Exception {
        try (Connection conn = DriverManager.getConnection(CONNECT_URL)) {
            conn.setAutoCommit(false);

            Statement stmt1 = createTestTable(conn);

            stmt1.execute("CREATE TABLE TEST_MVCC (ID INT, NAME VARCHAR, PRIMARY KEY(ID))" +
                " WITH \"cache_name=TEST_MVCC,template=replicated,atomicity=transactional_snapshot\"");

            stmt1.executeUpdate("INSERT INTO TEST VALUES (1, 1)");
            stmt1.executeUpdate("INSERT INTO TEST_MVCC VALUES (1, 1)");

            restartGrid(0);

            assertThrowsSql(() -> stmt1.execute("SELECT 1"), "Failed to communicate with Ignite cluster");
            assertThrowsSql(() -> stmt1.execute("SELECT 1"), STATEMENT_CLOSED_ON_DISCONNECT_MESSAGE);

            assertTrue(conn.isClosed());

            assertThrowsSql(conn::createStatement, "Connection is closed");
        }
    }

    private void restartGrid(int idx) throws Exception {
        stopGrid(idx);
        startGrid(idx);
    }

    private static void assertThrowsSql(GridTestUtils.RunnableX run, String message) {
        GridTestUtils.assertThrows(log, () -> {
            run.runx();

            return null;
        }, SQLException.class, message);
    }

    private static Statement createTestTable(Connection conn) throws SQLException {
        Statement stmt = conn.createStatement();

        stmt.execute("CREATE TABLE TEST (ID INT, NAME VARCHAR, PRIMARY KEY(ID))" +
            " WITH \"cache_name=TEST,template=replicated\"");

        return stmt;
    }
}
