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

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.affinity.AffinityFunction;
import org.apache.ignite.cache.affinity.AffinityFunctionContext;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.jdbc.thin.AffinityCache;
import org.apache.ignite.internal.jdbc.thin.JdbcThinPartitionResultDescriptor;
import org.apache.ignite.internal.jdbc.thin.QualifiedSQLQuery;
import org.apache.ignite.internal.processors.query.QueryHistory;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.apache.ignite.internal.sql.optimizer.affinity.PartitionResult;
import org.apache.ignite.internal.util.GridBoundedLinkedHashMap;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;

/**
 * Jdbc thin affinity awareness test.
 */
@SuppressWarnings({"ThrowableNotThrown"})
public class JdbcThinAffinityAwarenessSelfTest extends JdbcThinAbstractSelfTest {
    /** URL. */
    private static final String URL = "jdbc:ignite:thin://127.0.0.1:10800..10802?affinityAwareness=true";

    /** Nodes count. */
    private static final int NODES_CNT = 3;

    /** Query execution multiplier. */
    private static final int QUERY_EXECUTION_MULTIPLIER = 5;

    /** Rows count. */
    private static final int ROWS_COUNT = 100;

    /** Connection. */
    private Connection conn;

    /** Statement. */
    private Statement stmt;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        CacheConfiguration<?,?> cache = defaultCacheConfiguration();

        cache.setCacheMode(PARTITIONED);
        cache.setBackups(1);
        cache.setIndexedTypes(
            Integer.class, Person.class
        );

        cfg.setCacheConfiguration(cache);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGridsMultiThreaded(NODES_CNT);

        fillCache(DEFAULT_CACHE_NAME);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        conn = DriverManager.getConnection(URL);

        conn.setSchema('"' + DEFAULT_CACHE_NAME + '"');

        stmt = conn.createStatement();

        assert stmt != null;
        assert !stmt.isClosed();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        U.closeQuiet(stmt);

        conn.close();

        assert stmt.isClosed();
        assert conn.isClosed();
    }

    /**
     * Check that queries goes to expected number of nodes.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testExecuteQueries() throws Exception {
        checkNodesUsage(null, "select * from Person where _key = 1", 1, 1,
            false);

        checkNodesUsage(null, "select * from Person where _key = 1 or _key = 2", 2,
            2, false);

        checkNodesUsage(null, "select * from Person where _key in (1, 2)", 2, 2,
            false);
    }

    /**
     * Check that parameterised queries goes to expected number of nodes.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testExecuteParametrizedQueries() throws Exception {
        // Use case 1.
        PreparedStatement ps = conn.prepareStatement("select * from Person where _key = ?");

        ps.setInt(1, 2);

        checkNodesUsage(ps, null, 1, 1, false);

        // Use case 2.
        ps = conn.prepareStatement("select * from Person where _key = ? or _key = ?");

        ps.setInt(1, 1);

        ps.setInt(2, 2);

        checkNodesUsage(ps, null, 2, 2, false);

        // Use case 3.
        ps = conn.prepareStatement("select * from Person where _key in (?, ?)");

        ps.setInt(1, 1);

        ps.setInt(2, 2);

        checkNodesUsage(ps, null, 2, 2, false);
    }

    /**
     * Check that dml queries(updates) goes to expected number of nodes.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testUpdateQueries() throws Exception {
        checkNodesUsage(null, "update Person set firstName = 'TestFirstName' where _key = 1",
            1, 1, true);

        checkNodesUsage(null, "update Person set firstName = 'TestFirstName' where _key = 1 or _key = 2",
            2, 2, true);

        checkNodesUsage(null, "update Person set firstName = 'TestFirstName' where _key in (1, 2)",
            2, 2, true);
    }

    /**
     * Check that parameterised dml queries(updates) goes to expected number of nodes.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testUpdateParametrizedQueries() throws Exception {
        // Use case 1.
        PreparedStatement ps = conn.prepareStatement(
            "update Person set firstName = 'TestFirstName' where _key = ?");

        ps.setInt(1, 2);

        checkNodesUsage(ps, null, 1, 1, true);

        // Use case 2.
        ps = conn.prepareStatement("update Person set firstName = 'TestFirstName' where _key = ? or _key = ?");

        ps.setInt(1, 1);

        ps.setInt(2, 2);

        checkNodesUsage(ps, null, 2, 2, true);

        // Use case 3.
        ps = conn.prepareStatement("update Person set firstName = 'TestFirstName' where _key in (?, ?)");

        ps.setInt(1, 1);

        ps.setInt(2, 2);

        checkNodesUsage(ps, null, 2, 2, true);
    }

    /**
     * Check that dml queries(delete) goes to expected number of nodes.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testDeleteQueries() throws Exception {
        // In case of simple query like "delete from Person where _key = 1" fast update logic is used,
        // so parition result is not calculated on the server side - nothing to check.

        checkNodesUsage(null, "delete from Person where _key = 10000 or _key = 20000",
            2, 0, true);

        checkNodesUsage(null, "delete from Person where _key in (10000, 20000)",
            2, 0, true);
    }

    /**
     * Check that parameterised dml queries(delete) goes to expected number of nodes.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testDeleteParametrizedQueries() throws Exception {
        // In case of simple query like "delete from Person where _key = ?" fast update logic is used,
        // so parition result is not calculated on the server side - nothing to check.

        // Use case 1.
        PreparedStatement ps = conn.prepareStatement("delete from Person where _key = ? or _key = ?");

        ps.setInt(1, 1000);

        ps.setInt(2, 2000);

        checkNodesUsage(ps, null, 2, 0, true);

        // Use case 2.
        ps = conn.prepareStatement("delete from Person where _key in (?, ?)");

        ps.setInt(1, 1000);

        ps.setInt(2, 2000);

        checkNodesUsage(ps, null, 2, 0, true);
    }

    /**
     * Check that request/response functionality works fine if server response lacks partition result,
     * i.e. partitionResult is null. AllNode tes.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testQueryWithNullPartitionResponseBasedOnAllNode() throws Exception {
        verifyPartitionResultIsNull("select * from Person where age > 15", 85);
    }

    /**
     * Check that request/response functionality works fine if server response lacks partition result,
     * i.e. partitionResult is null. NoneNode tes.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testQueryWithNullPartitionResponseBasedOnNoneNode() throws Exception {
        verifyPartitionResultIsNull("select * from Person where _key = 1 and _key = 2", 0);
    }


    /**
     * Check that in case of non-rendezvous affinity function, client side affinity awareness is skipped.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testCacheWithNonRendezvousAffinityFunction() throws Exception {
        final String cacheName = "cacheWithCustomAffinityFunction";

        CacheConfiguration<Object, Object> cache = prepareCacheConfig(cacheName);
        cache.setAffinity(new DummyAffinity());

        ignite(0).createCache(cache);

        fillCache(cacheName);

        verifyPartitionResultIsNull("select * from \"" + cacheName + "\".Person where _key = 1",
            1);
    }

    /**
     * Check that in case of custom filters, client side affinity awareness is skipped.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testCacheWithCustomNodeFilter() throws Exception {
        final String cacheName = "cacheWithCustomNodeFilter";

        CacheConfiguration<Object, Object> cache = prepareCacheConfig(cacheName);
        cache.setNodeFilter(new CustomNodeFilter());

        ignite(0).createCache(cache);

        fillCache(cacheName);

        verifyPartitionResultIsNull("select * from \"" + cacheName + "\".Person where _key = 1",
            1);
    }

    /**
     * Check that affinity awareness functionality works fine for custom partitions count.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testCacheWithRendezvousCustomPartitionsCount() throws Exception {
        final String cacheName = "cacheWithRendezvousCustomPartitionsCount";

        CacheConfiguration<Object, Object> cache = prepareCacheConfig(cacheName);
        cache.setAffinity(new RendezvousAffinityFunction(false, 10));

        ignite(0).createCache(cache);

        fillCache(cacheName);

        checkNodesUsage(null,
            "select * from \"" + cacheName + "\".Person where _key = 1",
            1, 1, false);
    }

    /**
     * Check that affinity cache is invalidated in case of changing topology,
     * detected during partions destribution retrieval.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testChangeTopologyDetectionWithinPartitionDistributionResponse() throws Exception {
        final String sqlQry = "select * from Person where _key = 1";

        stmt.executeQuery(sqlQry);

        AffinityCache affinityCache = GridTestUtils.getFieldValue(conn, "affinityCache");

        startGrid(3);

        stmt.executeQuery(sqlQry);

        AffinityCache recreatedAffinityCache = GridTestUtils.getFieldValue(conn, "affinityCache");

        assertTrue(recreatedAffinityCache.version().compareTo(affinityCache.version()) > 0);
    }

    /**
     * Check that affinity cache is invalidated in case of changing topology,
     * detected during query response retrieval.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testChangeTopologyDetectionWithinQueryExecutionResponse() throws Exception {
        final String sqlQry = "select * from Person where _key = 1";

        stmt.executeQuery(sqlQry);
        stmt.executeQuery(sqlQry);

        AffinityCache affinityCache = GridTestUtils.getFieldValue(conn, "affinityCache");

        startGrid(4);

        stmt.executeQuery("select * from Person where _key = 2");

        AffinityCache recreatedAffinityCache = GridTestUtils.getFieldValue(conn, "affinityCache");

        assertTrue(recreatedAffinityCache.version().compareTo(affinityCache.version()) > 0);
    }

    /**
     * Check that affinity cache is invalidated in case of changing topology,
     * detected during affinity-awareness-unrelated-query response retrieval.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testChangeTopologyDetectionWithinAffinityAwarenessUnrelatedQuery() throws Exception {
        final String sqlQry = "select * from Person where _key = 1";

        ResultSet rs = stmt.executeQuery(sqlQry);

        AffinityCache affinityCache = GridTestUtils.getFieldValue(conn, "affinityCache");

        startGrid(5);

        rs.getMetaData();

        AffinityCache recreatedAffinityCache = GridTestUtils.getFieldValue(conn, "affinityCache");

        assertTrue(recreatedAffinityCache.version().compareTo(affinityCache.version()) > 0);
    }

    /**
     * Check that client side affinity awareness optimizations are skipped if affinityAwareness is switched off.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testAffinityAwarenessIsSkippedIfItIsSwitchedOff() throws Exception {
        Connection conn = DriverManager.getConnection(
            "jdbc:ignite:thin://127.0.0.1:10800..10802?affinityAwareness=false");

        Statement stmt = conn.createStatement();

        final String cacheName = "yac";

        CacheConfiguration<Object, Object> cache = prepareCacheConfig(cacheName);

        ignite(0).createCache(cache);

        stmt.executeQuery("select * from \"" + cacheName + "\".Person where _key = 1");

        AffinityCache affinityCache = GridTestUtils.getFieldValue(conn, "affinityCache");

        assertNull("Affinity cache is not null.", affinityCache);
    }

    /**
     * Check that client side affinity awareness optimizations are skipped by default.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testAffinityAwarenessIsSkippedByDefault() throws Exception {
        Connection conn = DriverManager.getConnection(
            "jdbc:ignite:thin://127.0.0.1:10800..10802");

        Statement stmt = conn.createStatement();

        final String cacheName = "yacccc";

        CacheConfiguration<Object, Object> cache = prepareCacheConfig(cacheName);

        ignite(0).createCache(cache);

        stmt.executeQuery("select * from \"" + cacheName + "\".Person where _key = 1");

        AffinityCache affinityCache = GridTestUtils.getFieldValue(conn, "affinityCache");

        assertNull("Affinity cache is not null.", affinityCache);
    }

    /**
     * Check that affinity cache stores sql queries with their schemas.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testAffinityCacheStoresSchemaBindedQuries() throws Exception {
        final String cacheName = "yacc";

        CacheConfiguration<Object, Object> cache = prepareCacheConfig(cacheName);
        cache.setSqlSchema(cacheName);

        ignite(0).createCache(cache);

        fillCache(cacheName);

        stmt.execute("select * from \"" + cacheName.toUpperCase() + "\".Person where _key = 1");

        conn.setSchema(cacheName.toUpperCase());

        stmt = conn.createStatement();

        stmt.execute("select * from \"" + cacheName.toUpperCase() + "\".Person where _key = 1");

        AffinityCache affinityCache = GridTestUtils.getFieldValue(conn, "affinityCache");

        GridBoundedLinkedHashMap<QualifiedSQLQuery, JdbcThinPartitionResultDescriptor> sqlCache =
            GridTestUtils.getFieldValue(affinityCache, "sqlCache");

        Set<String> schemas = sqlCache.keySet().stream().map(QualifiedSQLQuery::schemaName).collect(Collectors.toSet());

        assertTrue("Affinity cache doesn't contain query  sent to 'default' schema.",
            schemas.contains("default"));
        assertTrue("Affinity cache doesn't contain query  sent to '" + cacheName.toUpperCase() + "' schema.",
            schemas.contains(cacheName.toUpperCase()));
    }

    /**
     * Check that affinity cache stores compacted version of partitoins destributions.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testAffinityCacheCompactsPartitonDestributions() throws Exception {
        final String cacheName = "yaccc";

        CacheConfiguration<Object, Object> cache = prepareCacheConfig(cacheName);

        ignite(0).createCache(cache);

        fillCache(cacheName);

        stmt.execute("select * from Person where _key = 2");
        stmt.execute("select * from Person where _key = 2");

        stmt.execute("select * from \"" + cacheName + "\".Person where _key = 2");
        stmt.execute("select * from \"" + cacheName + "\".Person where _key = 2");

        AffinityCache affinityCache = GridTestUtils.getFieldValue(conn, "affinityCache");

        GridBoundedLinkedHashMap<QualifiedSQLQuery, JdbcThinPartitionResultDescriptor> sqlCache =
            GridTestUtils.getFieldValue(affinityCache, "sqlCache");

        GridBoundedLinkedHashMap<Integer, UUID[]> cachePartitionsDistribution =
            GridTestUtils.getFieldValue(affinityCache, "cachePartitionsDistribution");

        assertEquals("Sql sub-cache of affinity cache has unexpected number of elements.",
            2, sqlCache.size());

        assertEquals("Partitions destribution sub-cache of affinity cache has unexpected number of elements.",
            2, cachePartitionsDistribution.size());

        // Main assertition of the test: we are checking that partitions destributions for different caches
        // are equal in therms of (==)
        assertTrue("Partitions distributions are not the same.",
            cachePartitionsDistribution.get(0) == cachePartitionsDistribution.get(1));
    }

    /**
     * Check that affinity awareness works fine after reconnection.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testReconnect() throws Exception {
        checkNodesUsage(null, "select * from Person where _key = 3", 1, 1,
            false);

        startGrid(7);

        for (int i = 0; i < NODES_CNT; i++)
            stopGrid(i);

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                stmt.execute("select * from Person where _key = 3");

                return null;
            }
        }, SQLException.class, "Failed to communicate with Ignite cluster.");

        for (int i = 0; i < NODES_CNT; i++)
            startGrid(i);

        stopGrid(4);
        stopGrid(5);
        stopGrid(6);
        stopGrid(7);

        stmt = conn.createStatement();

        // We need this extra query to invalidate obsolete affinity cache
        stmt.execute("select * from Person where _key = 3");

        checkNodesUsage(null, "select * from Person where _key = 3", 1, 1,
            false);
    }

    /**
     * Prepares default cache configuration with given name.
     *
     * @param cacheName Cache name.
     * @return Cache configuration.
     */
    @SuppressWarnings("unchecked")
    protected CacheConfiguration<Object, Object> prepareCacheConfig(String cacheName) {
        CacheConfiguration<Object,Object> cache = defaultCacheConfiguration();

        cache.setName(cacheName);
        cache.setCacheMode(PARTITIONED);
        cache.setIndexedTypes(
            Integer.class, Person.class
        );

        return cache;
    }

    /**
     * Utitlity method that executes given query and verifies that expeted number of records was returned.
     * Besides that given method verified that partitoin result for corresponding query is null.
     *
     * @param sqlQry Sql query.
     * @param expRowsCnt Expected rows count.
     * @throws SQLException If failed.
     */
    protected void verifyPartitionResultIsNull(String sqlQry, int expRowsCnt) throws SQLException {
        ResultSet rs = stmt.executeQuery(sqlQry);

        assert rs != null;

        int rowCntr = 0;

        while (rs.next())
            rowCntr++;

        assertEquals("Rows counter doesn't match expected value.", expRowsCnt, rowCntr);

        AffinityCache affinityCache = GridTestUtils.getFieldValue(conn, "affinityCache");

        PartitionResult gotPartRes = affinityCache.partitionResult(
            new QualifiedSQLQuery("default", sqlQry)).partitionResult();

        assertNull("Partition result descriptor is not null.", gotPartRes);
    }

    /**
     * Utility method that:
     *  1. warms up an affinity cache;
     *  2. resets query history;
     *  3. executes given query multiple times;
     *  4. checks query history metrics in order to verify that not more than expected nodes were used.
     *
     * @param ps Prepared statement, either prepared statement or sql query should be used.
     * @param sql Sql query, either prepared statement or sql query should be used.
     * @param maxNodesUsedCnt Expected maximum number of used nodes.
     * @param expRowsCnt Expected rows count within result.
     * @param dml Flag that signals whether we execute dml or not.
     * @throws Exception If failed.
     */
    private void checkNodesUsage(PreparedStatement ps, String sql, int maxNodesUsedCnt, int expRowsCnt, boolean dml)
        throws Exception {
        // Warm up an affinity cache.
        if (ps != null)
            if (dml)
                ps.executeUpdate();
            else
                ps.executeQuery();
        else {
            if (dml)
                stmt.executeUpdate(sql);
            else
                stmt.executeQuery(sql);
        }

        // Reset query history.
        for (int i = 0; i < NODES_CNT; i++) {
            ((IgniteH2Indexing)grid(i).context().query().getIndexing())
                .runningQueryManager().resetQueryHistoryMetrics();
        }

        // Execute query multiple times
        for (int i = 0; i < NODES_CNT * QUERY_EXECUTION_MULTIPLIER; i++) {
            ResultSet rs = null;

            int updatedRowsCnt = 0;

            if (ps != null)
                if (dml)
                    updatedRowsCnt = ps.executeUpdate();
                else
                    rs = ps.executeQuery();
            else {
                if (dml)
                    updatedRowsCnt = stmt.executeUpdate(sql);
                else
                    rs = stmt.executeQuery(sql);
            }

            if (dml) {
                assertEquals("Unexpected updated rows count: expected [" + expRowsCnt + "]," +
                    " got [" + updatedRowsCnt + "]", expRowsCnt, updatedRowsCnt);
            }
            else {
                assert rs != null;

                int gotRowsCnt = 0;

                while (rs.next())
                    gotRowsCnt++;

                assertEquals("Unexpected rows count: expected [" + expRowsCnt + "], got [" + gotRowsCnt + "]",
                    expRowsCnt, gotRowsCnt);
            }
        }

        // Check query history metrics in order to verify that not more than expected nodes were used.
        int nonEmptyMetricsCntr = 0;
        int qryExecutionsCntr = 0;
        for (int i = 0; i < NODES_CNT; i++) {
            Collection<QueryHistory> metrics = ((IgniteH2Indexing)grid(i).context().query().getIndexing())
                .runningQueryManager().queryHistoryMetrics().values();

            if (!metrics.isEmpty()) {
                nonEmptyMetricsCntr++;
                qryExecutionsCntr += new ArrayList<>(metrics).get(0).executions();
            }
        }

        assertTrue("Unexpected amount of used nodes: expected [0 < nodesCnt <= " + maxNodesUsedCnt +
                "], got [" + nonEmptyMetricsCntr + "]",
            nonEmptyMetricsCntr > 0 && nonEmptyMetricsCntr <= maxNodesUsedCnt);

        assertEquals("Executions count doesn't match expeted value: expected [" +
                NODES_CNT * QUERY_EXECUTION_MULTIPLIER + "], got [" + qryExecutionsCntr + "]",
            NODES_CNT * QUERY_EXECUTION_MULTIPLIER, qryExecutionsCntr);
    }

    /**
     * Fills cache with test data.
     *
     * @param cacheName Cache name.
     */
    private void fillCache(String cacheName) {
        IgniteCache<Integer, Person> cachePerson = grid(0).cache(cacheName);

        assert cachePerson != null;

        for (int i = 0; i < ROWS_COUNT; i++)
            cachePerson.put(i, new Person(i, "John" + i, "White" + i, i + 1));
    }

    /**
     * Person.
     */
    @SuppressWarnings("unused")
    private static class Person implements Serializable {
        /** ID. */
        @QuerySqlField
        private final int id;

        /** First name. */
        @QuerySqlField
        private final String firstName;

        /** Last name. */
        @QuerySqlField
        private final String lastName;

        /** Age. */
        @QuerySqlField
        private final int age;

        /**
         * @param id ID.
         * @param firstName First name.
         * @param lastName Last name.
         * @param age Age.
         */
        private Person(int id, String firstName, String lastName, int age) {
            assert !F.isEmpty(firstName);
            assert !F.isEmpty(lastName);
            assert age > 0;

            this.id = id;
            this.firstName = firstName;
            this.lastName = lastName;
            this.age = age;
        }
    }

    /**
     * Dummy affinity function.
     */
    private static class DummyAffinity implements AffinityFunction {
        /** {@inheritDoc} */
        @Override public void reset() {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public int partitions() {
            return 1;
        }

        /** {@inheritDoc} */
        @Override public int partition(Object key) {
            return 0;
        }

        /**
         * Default constructor.
         */
        public DummyAffinity() {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public List<List<ClusterNode>> assignPartitions(AffinityFunctionContext affCtx) {
            List<ClusterNode> nodes = affCtx.currentTopologySnapshot();

            List<List<ClusterNode>> assign = new ArrayList<>(partitions());

            for (int i = 0; i < partitions(); ++i)
                assign.add(Collections.singletonList(nodes.get(0)));

            return assign;
        }

        /** {@inheritDoc} */
        @Override public void removeNode(UUID nodeId) {
            // No-op.
        }
    }

    /**
     *  Filter that accepts all nodes.
     */
    public static class CustomNodeFilter implements IgnitePredicate<ClusterNode> {
        /** */
        private static final long serialVersionUID = 0L;

        /** {@inheritDoc} */
        @Override public boolean apply(ClusterNode node) {
            return true;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object obj) {
            return false;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "CustomNodeFilter";
        }
    }
}
