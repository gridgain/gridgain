/*
 * Copyright 2020 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.compatibility.sql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.compatibility.sql.model.City;
import org.apache.ignite.compatibility.sql.model.Company;
import org.apache.ignite.compatibility.sql.model.Country;
import org.apache.ignite.compatibility.sql.model.Department;
import org.apache.ignite.compatibility.sql.model.ModelFactory;
import org.apache.ignite.compatibility.sql.model.Person;
import org.apache.ignite.compatibility.sql.randomsql.RandomQuerySupplier;
import org.apache.ignite.compatibility.sql.randomsql.Schema;
import org.apache.ignite.compatibility.sql.randomsql.Table;
import org.apache.ignite.compatibility.sql.runner.PredefinedQueriesSupplier;
import org.apache.ignite.compatibility.sql.runner.QueryDuelBenchmark;
import org.apache.ignite.compatibility.sql.runner.QueryDuelResult;
import org.apache.ignite.compatibility.testframework.junits.Dependency;
import org.apache.ignite.compatibility.testframework.junits.IgniteCompatibilityAbstractTest;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ClientConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgnitionEx;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.multijvm.IgniteProcessProxy;
import org.junit.Test;

/**
 * Test for SQL queries regressions detection.
 * It happens in the next way:
 * 
 * 1. Test starts two different Ignite versions: current version and the old one.
 * 2. Then framework executes (randomly chosen/generated) equivalent queries in both versions.
 * 3. Execution time for both version is measured and if it exceeds some threshold, the query marked as suspected.
 * 4. All suspected queries are submitted to both Ignite versions one more time to get rid of outliers.
 * 5. If a poor execution time is reproducible for suspected query,
 *    this query is reported as a problematic and test fails because of it.
 */
@SuppressWarnings("TypeMayBeWeakened")
public class SqlQueryRegressionsTest extends IgniteCompatibilityAbstractTest {
    /**
     * You may wanted to set this seed to particular value in case you will need to troubleshoot particular run. If it
     * set to the some value (not {@code null}), it will be used in random generator, otherwise generator will be
     * initialized with random seed.
     */
    private static final Integer SEED = null;

    /** */
    private static final String BASE_VERSION_PARAM = "BASE_VERSION";

    /** */
    private static final String BASE_IS_IGNITE_PARAM = "BASE_IS_IGNITE";

    /** */
    private static final String SEED_PARAM = "SEED";

    /** */
    private static final String DEFAULT_BASE_VERSION = "8.7.22";

    /** */
    private static final boolean DEFAULT_BASE_IS_IGNITE = false;

    /** */
    private static final int OLD_JDBC_PORT = 10800;

    /** */
    private static final int NEW_JDBC_PORT = 10802;

    /** */
    private static final long BENCHMARK_TIMEOUT = 60_000;

    /** */
    private static final long WARM_UP_TIMEOUT = 5_000;

    /** */
    private static final String JDBC_URL = "jdbc:ignite:thin://127.0.0.1:";

    /** */
    private static final TcpDiscoveryIpFinder OLD_VER_FINDER = new TcpDiscoveryVmIpFinder(true) {{
        setAddresses(Collections.singleton("127.0.0.1:47500..47509"));
    }};

    /**  */
    private static final TcpDiscoveryVmIpFinder NEW_VER_FINDER = new TcpDiscoveryVmIpFinder(true) {{
        setAddresses(Collections.singleton("127.0.0.1:47510..47519"));
    }};

    /** Model factories. */
    private static final List<ModelFactory> MODEL_FACTORIES = Arrays.asList(
        new Person.Factory(),
        new Department.Factory(),
        new Country.Factory(),
        new City.Factory(),
        new Company.Factory()
    );

    /** */
    private static final Schema SCHEMA = new Schema();

    static {
        MODEL_FACTORIES.stream().map(ModelFactory::queryEntity).map(Table::new).forEach(SCHEMA::addTable);
    }

    /** */
    private boolean baseIsIgnite;

    /** */
    private String ver;

    /** */
    private int seed;

    /** {@inheritDoc} */
    @Override protected Collection<Dependency> getDependencies(String igniteVer) {
        Collection<Dependency> dependencies = new ArrayList<>();

        dependencies.add(new Dependency("core", groupId(), "ignite-core", igniteVer, false));
        dependencies.add(new Dependency("core", groupId(), "ignite-core", igniteVer, true));
        dependencies.add(new Dependency("indexing", groupId(), "ignite-indexing", igniteVer, false));

        if (baseIsIgnite)
            dependencies.add(new Dependency("h2", "com.h2database", "h2", "1.4.197", false));

        return dependencies;
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 5 * BENCHMARK_TIMEOUT + WARM_UP_TIMEOUT + super.getTestTimeout();
    }

    /**
     * Test for SQL performance regression detection.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testSqlPerformanceRegressions() throws Exception {
        initParam();

        try {
            Supplier<String> qrySupplier = new RandomQuerySupplier(SCHEMA, seed);

            startOldAndNewClusters(seed);

            try (Connection oldConn = createConnection(OLD_JDBC_PORT);
                 Connection newConn = createConnection(NEW_JDBC_PORT)
            ) {
                QueryDuelBenchmark benchmark = new QueryDuelBenchmark(log, oldConn, newConn);

                // 0. Warm-up.IgniteCompatibilityNodeRunner
                benchmark.runBenchmark(WARM_UP_TIMEOUT, qrySupplier, 0, 1);

                // 1. Initial run.
                Collection<QueryDuelResult> suspiciousQrys =
                    benchmark.runBenchmark(BENCHMARK_TIMEOUT, qrySupplier, 1, 1);

                if (suspiciousQrys.isEmpty())
                    return; // No suspicious queries - no problem.

                Set<String> suspiciousQrysSet = suspiciousQrys.stream()
                    .map(QueryDuelResult::query)
                    .collect(Collectors.toSet());

                if (log.isInfoEnabled())
                    log.info("Problematic queries number: " + suspiciousQrysSet.size());

                Supplier<String> problematicQrysSupplier = new PredefinedQueriesSupplier(suspiciousQrysSet, true);

                // 2. Rerun problematic queries to ensure they are not outliers.
                Collection<QueryDuelResult> failedQueries =
                    benchmark.runBenchmark(getTestTimeout(), problematicQrysSupplier, 7, 10);

                assertTrue("Found SQL performance regression for queries (seed is " + seed + "): "
                        + formatPretty(failedQueries), failedQueries.isEmpty());
            }
        }
        finally {
            stopClusters();
        }
    }

    /**
     * Starts old and new Ignite clusters. Also creates all tables and populate data.
     *
     * @param seed Random seed.
     */
    public void startOldAndNewClusters(int seed) throws Exception {
        // Old cluster.
        startGrid(2, ver, new NodeConfigurationClosure("0"),
            ignite -> createTablesAndPopulateData(ignite, seed));
        startGrid(3, ver, new NodeConfigurationClosure("1"));

        // New cluster
        Ignite ignite = IgnitionEx.start(prepareNodeConfig(
            getConfiguration(getTestIgniteInstanceName(0)), NEW_VER_FINDER, NEW_JDBC_PORT, "0"));
        IgnitionEx.start(prepareNodeConfig(
            getConfiguration(getTestIgniteInstanceName(1)), NEW_VER_FINDER, NEW_JDBC_PORT, "1"));

        createTablesAndPopulateData(ignite, seed);
    }

    /** */
    private void initParam() {
        String verParam = System.getProperty(BASE_VERSION_PARAM);
        String baseIsIgniteParam = System.getProperty(BASE_IS_IGNITE_PARAM);
        String seedParam = System.getProperty(SEED_PARAM);

        ver = !F.isEmpty(verParam) ? verParam : DEFAULT_BASE_VERSION;
        baseIsIgnite = !F.isEmpty(baseIsIgniteParam) ? Boolean.parseBoolean(baseIsIgniteParam) : DEFAULT_BASE_IS_IGNITE;
        seed = SEED != null ? SEED : !F.isEmpty(seedParam) ? Integer.parseInt(seedParam) : ThreadLocalRandom.current().nextInt();

        if (log.isInfoEnabled()) {
            log.info("Test was started with params:\n"
                + "\tseed=" + seed + "\n"
                + "\tversion=" + ver + "\n"
                + "\tbaseIsIgnite=" + baseIsIgnite + "\n"
            );
        }
    }

    /**
     * Stops both new and old clusters.
     */
    public void stopClusters() {
        // Old cluster.
        IgniteProcessProxy.killAll();

        // New cluster.
        for (Ignite ignite : G.allGrids())
            U.close(ignite, log);
    }

    /**
     * @param qrys Queries duels result.
     * @return Pretty formatted result of duels.
     */
    private static String formatPretty(Collection<QueryDuelResult> qrys) {
        StringBuilder sb = new StringBuilder().append("\n");

        for (QueryDuelResult res : qrys) {
            sb.append(res)
                .append('\n');
        }

        return sb.toString();
    }

    /**
     * @param ignite Ignite node.
     * @param seed Random seed.
     */
    private static void createTablesAndPopulateData(Ignite ignite, int seed) {
        for (ModelFactory mdlFactory : MODEL_FACTORIES)
            createAndPopulateTable(ignite, mdlFactory, seed);
    }

    /** */
    private String groupId(){
        return baseIsIgnite ? "org.apache.ignite" : "org.gridgain";
    }

    /** */
    @SuppressWarnings({"rawtypes", "unchecked"})
    private static void createAndPopulateTable(Ignite ignite, ModelFactory factory, int seed) {
        factory.init(seed);

        QueryEntity qryEntity = factory.queryEntity();
        CacheConfiguration cacheCfg = new CacheConfiguration<>(factory.tableName())
            .setQueryEntities(Collections.singleton(qryEntity))
            .setSqlSchema("PUBLIC");

        IgniteCache personCache = ignite.createCache(cacheCfg);

        for (long i = 0; i < factory.count(); i++)
            personCache.put(i, factory.createRandom());
    }

    /**
     * @param port Port.
     */
    private static Connection createConnection(int port) throws SQLException {
        Connection conn = DriverManager.getConnection(JDBC_URL + port + "?lazy=true");

        conn.setSchema("PUBLIC");

        return conn;
    }

    /**
     * Prepares ignite nodes configuration.
     */
    private static IgniteConfiguration prepareNodeConfig(IgniteConfiguration cfg, TcpDiscoveryIpFinder ipFinder,
        int jdbcPort, String consistentId) {
        cfg.setLocalHost("127.0.0.1");
        cfg.setPeerClassLoadingEnabled(false);
        cfg.setConsistentId(consistentId);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();
        disco.setIpFinder(ipFinder);
        cfg.setDiscoverySpi(disco);

        ClientConnectorConfiguration clientCfg = new ClientConnectorConfiguration();
        clientCfg.setPort(jdbcPort);
        return cfg;
    }

    /**
     * Configuration closure.
     */
    private static class NodeConfigurationClosure implements IgniteInClosure<IgniteConfiguration> {
        /** */
        private final String consistentId;

        /** */
        public NodeConfigurationClosure(String consistentId) {
            this.consistentId = consistentId;
        }

        /** {@inheritDoc} */
        @Override public void apply(IgniteConfiguration cfg) {
            prepareNodeConfig(cfg, OLD_VER_FINDER, OLD_JDBC_PORT, consistentId);
        }
    }
}
