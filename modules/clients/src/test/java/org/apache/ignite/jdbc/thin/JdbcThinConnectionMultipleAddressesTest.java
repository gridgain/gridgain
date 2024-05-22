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

import java.lang.management.ManagementFactory;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import javax.management.MBeanServer;
import javax.management.MBeanServerInvocationHandler;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ClientConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.internal.jdbc.thin.JdbcThinConnection;
import org.apache.ignite.internal.jdbc.thin.JdbcThinDisconnectException;
import org.apache.ignite.internal.processors.odbc.ClientListenerProcessor;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.mxbean.ClientProcessorMXBean;
import org.apache.ignite.testframework.GridTestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;

/**
 * JDBC driver reconnect test with multiple addresses.
 */
public class JdbcThinConnectionMultipleAddressesTest extends JdbcThinAbstractSelfTest {
    /** Nodes count. */
    private static final int NODES_CNT = 3;

    /** */
    private static final String URL_PORT_RANGE = "jdbc:ignite:thin://127.0.0.1:"
        + ClientConnectorConfiguration.DFLT_PORT + ".." + (ClientConnectorConfiguration.DFLT_PORT + 10);

    /** */
    private static final String URL_SINGLE_PORT = "jdbc:ignite:thin://127.0.0.1:"
        + ClientConnectorConfiguration.DFLT_PORT;

    /** */
    private static final String STATEMENT_CLOSED_ON_DISCONNECT_MESSAGE =
        "Statement was closed due to a disconnection from the server";

    /** */
    private static final String FAILED_CONNECT_TO_CLUSTER_MESSAGE =
        "Failed to communicate with Ignite cluster";

    /** Jdbc ports. */
    private static final ArrayList<Integer> jdbcPorts = new ArrayList<>();

    /**
     * @return JDBC URL.
     */
    private static String url() {
        StringBuilder sb = new StringBuilder("jdbc:ignite:thin://");

        for (int i = 0; i < NODES_CNT; i++)
            sb.append("127.0.0.1:").append(jdbcPorts.get(i)).append(',');

        return sb.toString();
    }

    /** {@inheritDoc} */
    @SuppressWarnings("deprecation")
    @Override protected IgniteConfiguration getConfiguration(String name) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(name);

        cfg.setCacheConfiguration(cacheConfiguration(DEFAULT_CACHE_NAME));

        cfg.setMarshaller(new BinaryMarshaller());

        cfg.setClientConnectorConfiguration(
            new ClientConnectorConfiguration()
                .setPort(jdbcPorts.get(getTestIgniteInstanceIndex(name))));

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

        jdbcPorts.clear();

        for (int i = 0; i < NODES_CNT; i++)
            jdbcPorts.add(ClientConnectorConfiguration.DFLT_PORT + i);

        startGrids(NODES_CNT);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testMultipleAddressesConnect() throws Exception {
        try (Connection conn = DriverManager.getConnection(url())) {
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("SELECT 1");

                ResultSet rs = stmt.getResultSet();

                assertTrue(rs.next());

                assertEquals(1, rs.getInt(1));
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPortRangeConnect() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL_PORT_RANGE)) {
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("SELECT 1");

                ResultSet rs = stmt.getResultSet();

                assertTrue(rs.next());

                assertEquals(1, rs.getInt(1));
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testMultipleAddressesOneNodeFailoverOnStatementExecute() throws Exception {
        checkReconnectOnStatementExecute(url(), false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testMultipleAddressesAllNodesFailoverOnStatementExecute() throws Exception {
        checkReconnectOnStatementExecute(url(), true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPortRangeAllNodesFailoverOnStatementExecute() throws Exception {
        checkReconnectOnStatementExecute(URL_PORT_RANGE, true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testMultipleAddressesOneNodeFailoverOnResultSet() throws Exception {
        checkReconnectOnResultSet(url(), false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testMultipleAddressesAllNodesFailoverOnResultSet() throws Exception {
        checkReconnectOnResultSet(url(), true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPortRangeAllNodesFailoverOnResultSet() throws Exception {
        checkReconnectOnResultSet(URL_PORT_RANGE, true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testMultipleAddressesOneNodeFailoverOnMeta() throws Exception {
        checkReconnectOnMeta(url(), false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testMultipleAddressesAllNodesFailoverOnMeta() throws Exception {
        checkReconnectOnMeta(url(), true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPortRangeAllNodesFailoverOnMeta() throws Exception {
        checkReconnectOnMeta(URL_PORT_RANGE, true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testMultipleAddressesOneNodeFailoverOnStreaming() throws Exception {
        checkReconnectOnStreaming(url(), false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testClientConnectionMXBean() throws Exception {
        Connection conn = DriverManager.getConnection(URL_PORT_RANGE);

        try {
            final Statement stmt0 = conn.createStatement();

            stmt0.execute("SELECT 1");

            ResultSet rs0 = stmt0.getResultSet();

            ClientProcessorMXBean serverMxBean = null;

            // Find node which client is connected to.
            for (int i = 0; i < NODES_CNT; i++) {
                serverMxBean = clientProcessorBean(i);

                if (!serverMxBean.getConnections().isEmpty())
                    break;
            }

            assertNotNull("No ClientConnections MXBean found.", serverMxBean);

            serverMxBean.dropAllConnections();

            // Simple select should silently close opened result set and re-establish connection trnsparently.
            stmt0.execute("SELECT 1");
            assertTrue(rs0.isClosed());

            serverMxBean.dropAllConnections();

            Statement stmt1 = conn.createStatement();

            // Multistatement query cannot re-establish connection transparently.
            assertThrowsSql(() -> stmt1.execute("SELECT 1; SELECT 1"), FAILED_CONNECT_TO_CLUSTER_MESSAGE, true);
            assertTrue(stmt1.isClosed());

            assertTrue(getActiveClients().isEmpty());

            final Statement stmt2 = conn.createStatement();

            stmt2.execute("SELECT 1");

            ResultSet rs1 = stmt2.getResultSet();

            // Check active clients.
            List<String> activeClients = getActiveClients();

            assertEquals(1, activeClients.size());

            assertTrue(rs1.next());
            assertEquals(1, rs1.getInt(1));

            rs1.close();
            stmt2.close();
        }
        finally {
            conn.close();
        }

        boolean allClosed = GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                return getActiveClients().isEmpty();
            }
        }, 10_000);

        assertTrue(allClosed);
    }

    /** Ensures that {@link JdbcThinDisconnectException} doesn't throw on the initial connection. */
    @Test
    public void testConnectionFailure() {
        assertThrowsSql(
            () -> DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1:12345"),
            "Failed to connect to server",
            false
        );

        assertThrowsSql(
            () -> DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1:12345?PartitionAwareness=true"),
            "Failed to connect to server",
            false
        );
    }

    @Test
    public void testSingleAddressReconnectAfterRestart() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL_SINGLE_PORT)) {
            Statement stmt1 = createTestTable(conn);

            // Simple select.
            {
                // Statement is empty when disconnect detected.
                try (Statement stmt = conn.createStatement()) {
                    stopGrid(0);
                    startGrid(0);

                    stmt.execute("SELECT 1");
                    stmt.execute("SELECT 1");
                }

                // Statement has closed resultset when disconnect detected.
                try (Statement stmt = conn.createStatement()) {
                    try (ResultSet rs = stmt.executeQuery("SELECT 1")) {
                        assertTrue(rs.next());
                    }

                    stopGrid(0);
                    startGrid(0);

                    stmt.execute("SELECT 1");
                    stmt.execute("SELECT 1");
                }

                // Statement has opened resultset when disconnect detected.
                {
                    Statement stmt = conn.createStatement();

                    stmt.execute("SELECT 1");

                    stopGrid(0);
                    startGrid(0);

                    // The exception when closing an open result set should be ignored
                    // since the client cannot do anything with it.
                    stmt.close();
                    assertTrue(stmt.isClosed());
                }

                // The same case, but with transparent re-connection.
                try (Statement stmt = conn.createStatement()) {
                    stmt.execute("SELECT 1");

                    stopGrid(0);
                    startGrid(0);

                    stmt.execute("SELECT 1");
                    stmt.execute("SELECT 1");
                }

                // Statement created after restart.
                stopGrid(0);
                startGrid(0);

                try (Statement stmt = conn.createStatement()) {
                    stmt.execute("SELECT 1");
                    stmt.execute("SELECT 1; SELECT 1");
                }
            }

            // DML query.
            {
                stopGrid(0);
                startGrid(0);

                try (Statement stmt = conn.createStatement()) {
                    // We cannot automatically retry DML query, because it's possible to duplicate it.
                    assertThrowsSql(() -> stmt.executeUpdate("INSERT INTO TEST VALUES (1, 1)"),
                        FAILED_CONNECT_TO_CLUSTER_MESSAGE,
                        true);

                    assertTrue(stmt.isClosed());
                    assertThrowsSql(() -> stmt.execute("SELECT 1"),
                        STATEMENT_CLOSED_ON_DISCONNECT_MESSAGE,
                        false);
                }

                try (Statement stmt = conn.createStatement()) {
                    // But the second attempt recover connection.
                    assertEquals(1, stmt.executeUpdate("INSERT INTO TEST VALUES (1, 1)"));

                    assertFalse(stmt.isClosed());
                }
            }

            // Prepared statement.
            {
                stopGrid(0);
                startGrid(0);

                try (PreparedStatement stmt = conn.prepareStatement("INSERT INTO TEST VALUES (?, ?)")) {
                    stmt.setInt(1, 2);
                    stmt.setInt(2, 2);

                    // We cannot automatically retry DML query, because it's possible to duplicate it.
                    assertThrowsSql(stmt::executeUpdate, FAILED_CONNECT_TO_CLUSTER_MESSAGE, true);

                    assertTrue(stmt.isClosed());
                }

                try (PreparedStatement stmt = conn.prepareStatement("INSERT INTO TEST VALUES (?, ?)")) {
                    stmt.setInt(1, 2);
                    stmt.setInt(2, 2);
                    // But the second attempt recover connection.
                    assertEquals(1, stmt.executeUpdate());

                    assertFalse(stmt.isClosed());
                }
            }

            // Prepared statement batch.
            {
                stopGrid(0);
                startGrid(0);

                try (PreparedStatement stmt = conn.prepareStatement("INSERT INTO TEST VALUES (?, ?)")) {
                    stmt.setInt(1, 3);
                    stmt.setInt(2, 3);
                    stmt.addBatch();

                    stmt.setInt(1, 4);
                    stmt.setInt(2, 4);
                    stmt.addBatch();

                    assertThrowsSql(stmt::executeBatch, FAILED_CONNECT_TO_CLUSTER_MESSAGE, true);

                    assertTrue(stmt.isClosed());
                }

                try (PreparedStatement stmt = conn.prepareStatement("INSERT INTO TEST VALUES (?, ?)")) {
                    stmt.setInt(1, 3);
                    stmt.setInt(2, 3);
                    stmt.addBatch();

                    stmt.setInt(1, 4);
                    stmt.setInt(2, 4);
                    stmt.addBatch();
                    // But the second attempt recover connection.
                    assertArrayEquals(new int[] {1, 1}, stmt.executeBatch());

                    assertFalse(stmt.isClosed());
                }

                try (Statement stmt = conn.createStatement()) {
                    try (ResultSet rs = stmt.executeQuery("SELECT COUNT(1) FROM TEST")) {
                        assertTrue(rs.next());

                        assertEquals(4, rs.getLong(1));
                    }
                }
            }

            assertTrue(stmt1.isClosed());
        }
    }

    @Test
    public void testSingleAddressReconnectDuringRestart() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL_SINGLE_PORT)) {
            stopGrid(0);

            try (Statement stmt = conn.createStatement()) {
                assertThrowsSql(() -> stmt.execute("SELECT 1"), "Failed to connect to server", true);
                assertTrue(stmt.isClosed());
            }

            startGrid(0);

            try (Statement stmt = conn.createStatement()) {
                stmt.execute("SELECT 1");
            }
        }
    }

    /** Ensures that if a transaction context exists, the connection is closed when a disconnect is detected. */
    @Test
    public void testReconnectWithTxContextIsNotAllowed() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL_SINGLE_PORT)) {
            conn.setAutoCommit(false);

            Statement stmt1 = createTestTable(conn);

            stmt1.execute("CREATE TABLE TEST_MVCC (ID INT, NAME VARCHAR, PRIMARY KEY(ID))" +
                " WITH \"cache_name=TEST_MVCC,template=replicated,atomicity=transactional_snapshot\"");

            stmt1.executeUpdate("INSERT INTO TEST VALUES (1, 1)");

            stopGrid(0);
            startGrid(0);

            // Transaction context does not exist - reconnection must be performed.
            stmt1.execute("SELECT * FROM TEST");

            // Create transaction context.
            stmt1.execute("SELECT * FROM TEST_MVCC");

            stopGrid(0);
            startGrid(0);

            try (Statement stmt = conn.createStatement()) {
                assertThrowsSql(() -> stmt.execute("SELECT 1"), FAILED_CONNECT_TO_CLUSTER_MESSAGE, true);
                assertThrowsSql(() -> stmt.execute("SELECT 1"), STATEMENT_CLOSED_ON_DISCONNECT_MESSAGE, false);
            }

            assertTrue(stmt1.isClosed());
            assertTrue(conn.isClosed());

            assertThrowsSql(conn::createStatement, "Connection is closed", false);
        }
    }

    /**
     * Return active client list.
     *
     * @return clients.
     */
    @NotNull private List<String> getActiveClients() {
        List<String> activeClients = new ArrayList<>(1);

        for (int i = 0; i < NODES_CNT; i++) {
            ClientProcessorMXBean mxBean = clientProcessorBean(i);

            assertNotNull(mxBean);

            activeClients.addAll(mxBean.getConnections());
        }
        return activeClients;
    }

    /**
     * Return ClientProcessorMXBean.
     *
     * @return MBean.
     */
    private ClientProcessorMXBean clientProcessorBean(int igniteInt) {
        ObjectName mbeanName = null;

        try {
            mbeanName = U.makeMBeanName(getTestIgniteInstanceName(igniteInt), "Clients",
                ClientListenerProcessor.class.getSimpleName());
        }
        catch (MalformedObjectNameException e) {
            fail("Failed to register MBean.");
        }

        MBeanServer mbeanSrv = ManagementFactory.getPlatformMBeanServer();

        if (!mbeanSrv.isRegistered(mbeanName))
            fail("MBean is not registered: " + mbeanName.getCanonicalName());

        return MBeanServerInvocationHandler.newProxyInstance(mbeanSrv, mbeanName, ClientProcessorMXBean.class, true);
    }

    /**
     * Check failover on restart cluster or stop one node.
     *
     * @param url Connection URL.
     * @param allNodes Restart all nodes flag.
     * @throws Exception If failed.
     */
    private void checkReconnectOnMeta(String url, boolean allNodes) throws Exception {
        try (Connection conn = DriverManager.getConnection(url)) {
            DatabaseMetaData meta = conn.getMetaData();

            final String[] types = {"TABLES"};

            ResultSet rs0 = meta.getTables(null, null, null, types);

            assertFalse(rs0.next());

            stop(conn, allNodes);

            if (allNodes) {
                assertThrowsSql(() -> meta.getTables(null, null, null, null),
                    "Failed to connect to server", true);

                assertThrowsSql(() -> meta.getTables(null, null, null, null),
                    "Failed to connect to server", true);
            } else {
                // Connection should be transparently re-established to another node.
                rs0 = meta.getTables(null, null, null, types);

                assertFalse(rs0.next());
            }

            restart(allNodes);

            rs0 = meta.getTables(null, null, null, types);
            assertFalse(rs0.next());
        }
    }

    /**
     * Check failover on restart cluster ar stop one node.
     *
     * @param url Connection URL.
     * @param allNodes Restart all nodes flag.
     * @throws Exception If failed.
     */
    private void checkReconnectOnStatementExecute(String url, boolean allNodes) throws Exception {
        try (Connection conn = DriverManager.getConnection(url)) {
            final Statement stmt0 = conn.createStatement();

            stmt0.execute("SELECT 1");

            ResultSet rs0 = stmt0.getResultSet();

            assertTrue(rs0.next());
            assertEquals(1, rs0.getInt(1));
            assertFalse(rs0.isClosed());

            stop(conn, allNodes);
            restart(allNodes);

            // Connection must be re-established transparently.
            stmt0.execute("SELECT 1");
            ResultSet rs1 = stmt0.getResultSet();

            assertTrue(rs1.next());
            assertEquals(1, rs1.getInt(1));
        }
    }

    /**
     * Check failover on restart cluster ar stop one node.
     *
     * @param url Connection URL.
     * @param allNodes Restart all nodes flag.
     * @throws Exception If failed.
     */
    private void checkReconnectOnResultSet(String url, boolean allNodes) throws Exception {
        try (Connection conn = DriverManager.getConnection(url)) {
            final Statement stmt0 = conn.createStatement();

            stmt0.execute("SELECT 1");

            final ResultSet rs0 = stmt0.getResultSet();

            assertTrue(rs0.next());
            assertEquals(1, rs0.getInt(1));
            assertFalse(rs0.isClosed());

            stop(conn, allNodes);

            // Exception raised on sending close request is ignored, because client cannot do anything with it
            rs0.close();

            restart(allNodes);

            final Statement stmt1 = conn.createStatement();

            stmt1.execute("SELECT 1");

            ResultSet rs1 = stmt1.getResultSet();

            assertTrue(rs1.next());
            assertEquals(1, rs1.getInt(1));
        }
    }

    /**
     * Check failover on restart cluster ar stop one node.
     *
     * @param url Connection URL.
     * @param allNodes Restart all nodes flag.
     * @throws Exception If failed.
     */
    private void checkReconnectOnStreaming(String url, boolean allNodes) throws Exception {
        try (Connection conn = DriverManager.getConnection(url)) {
            final Statement stmt0 = conn.createStatement();
            stmt0.execute("CREATE TABLE TEST(id int primary key, val int)");

            stmt0.execute("SET STREAMING 1 BATCH_SIZE 10 ALLOW_OVERWRITE 0 " +
                " PER_NODE_BUFFER_SIZE 1000 FLUSH_FREQUENCY 1000");

            final ResultSet rs0 = stmt0.getResultSet();

            stop(conn, allNodes);

            final int[] id = {0};

            GridTestUtils.assertThrows(log, new Callable<Object>() {
                @Override public Object call() throws Exception {
                    // compiled instead of while (true)
                    while (id[0] >= 0) {
                        stmt0.execute("INSERT INTO TEST(id, val) values (" + id[0] + ", " + id[0] + ")");

                        id[0]++;
                    }

                    return null;
                }
            }, SQLException.class, "Failed to communicate with Ignite cluster on JDBC streaming");

            int minId = id[0];

            restart(allNodes);

            final Statement stmt1 = conn.createStatement();

            stmt1.execute("SET STREAMING 1 BATCH_SIZE 10 ALLOW_OVERWRITE 0 " +
                " PER_NODE_BUFFER_SIZE 1000 FLUSH_FREQUENCY 1000");

            for (int i = 0; i < 10; ++i, id[0]++)
                stmt1.execute("INSERT INTO TEST(id, val) values (" + id[0] + ", " + id[0] + ")");

            stmt1.execute("SET STREAMING 0");

            stmt1.execute("SELECT ID FROM TEST WHERE id < " + minId);

            assertFalse(stmt1.getResultSet().next());

            stmt1.execute("SELECT count(id) FROM TEST WHERE id > " + minId);

            assertTrue(stmt1.getResultSet().next());
        }
    }

    /**
     * @param conn Connection.
     * @param all If {@code true} all nodes will be stopped.
     */
    private void stop(Connection conn, boolean all) {
        if (all)
            stopAllGrids();
        else {

            if (partitionAwareness) {
                for (int i = 0; i < NODES_CNT - 1; i++)
                    stopGrid(i);
            }
            else {
                int idx = ((JdbcThinConnection)conn).serverIndex();

                stopGrid(idx);
            }
        }
    }

    /**
     * @param all If {@code true} all nodes will be started.
     * @throws Exception On error.
     */
    private void restart(boolean all) throws Exception {
        if (all)
            startGrids(NODES_CNT);
    }

    private static void assertThrowsSql(GridTestUtils.RunnableX run, String message, boolean disconnected) {
        SQLException ex = GridTestUtils.assertThrows(log, () -> {
            run.runx();

            return null;
        }, SQLException.class, message);

        assertEquals(String.valueOf(ex), disconnected, ex.getCause() instanceof JdbcThinDisconnectException);
    }

    private static Statement createTestTable(Connection conn) throws SQLException {
        Statement stmt = conn.createStatement();

        stmt.execute("CREATE TABLE TEST (ID INT, NAME VARCHAR, PRIMARY KEY(ID))" +
            " WITH \"cache_name=TEST,template=replicated\"");

        return stmt;
    }
}
