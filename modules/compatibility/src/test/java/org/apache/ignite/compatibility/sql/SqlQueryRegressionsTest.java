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
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
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
import org.apache.ignite.compatibility.sql.runner.QueryWithParams;
import org.apache.ignite.compatibility.testframework.junits.Dependency;
import org.apache.ignite.compatibility.testframework.junits.IgniteCompatibilityAbstractTest;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ClientConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteProperties;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.lang.IgniteProductVersion;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils.SF;
import org.apache.ignite.testframework.junits.multijvm.IgniteProcessProxy;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.Collections.unmodifiableList;

/**
 * Test for SQL queries regressions detection.
 * It happens in the next way:
 * <ol>
 *     <li>
 *         Test starts two different Ignite versions: current version and the base one. The base version
 *         is a version which provided via system property {@link SqlQueryRegressionsTest#BASE_VERSION_PARAM}.
 *         If this property is not specified, predefined by {@link SqlQueryRegressionsTest#DEFAULT_BASE_VERSION}
 *         version will be used.
 *     </li>
 *     <li>
 *         Then framework executes (randomly chosen/generated) equivalent queries in both versions.
 *     </li>
 *     <li>
 *         Execution times for both version are measured and then compared to each other. If difference is
 *         exceeded some threshold, the query marked as suspected.
 *     </li>
 *     <li>
 *         All suspected queries are submitted to both Ignite versions one more time to get rid of outliers.
 *     </li>
 *     <li>
 *         If a poor execution time is reproducible for suspected query, this query is reported as a problematic
 *         and test fails because of it.
 *     </li>
 * </ol>
 */
@SuppressWarnings("TypeMayBeWeakened")
public class SqlQueryRegressionsTest extends IgniteCompatibilityAbstractTest {
    /*
     * If you are wanted to troubleshoot particular run, you have to set following defaults to the required values
     */
    /**
     * If set to non-null value, it will be used in random generator, otherwise generator will be
     * initialized with a random seed.
     */
    private static final Integer DEFAULT_SEED = null;

    /** */
    private static final String DEFAULT_BASE_VERSION = IgniteProperties.get("ignite.version");

    /** */
    private static final String DEFAULT_TARGET_VERSION = IgniteProperties.get("ignite.version");

    /** */
    private static final boolean DEFAULT_BASE_IS_IGNITE = false;

    /** */
    private static final boolean DEFAULT_TARGET_IS_IGNITE = false;

    /** */
    private static final String BASE_VERSION_PARAM = "BASE_VERSION";

    /** */
    private static final String TARGET_VERSION_PARAM = "TARGET_VERSION";

    /** */
    private static final String BASE_IS_IGNITE_PARAM = "BASE_IS_IGNITE";

    /** */
    private static final String TARGET_IS_IGNITE_PARAM = "TARGET_IS_IGNITE";

    /** */
    private static final String SEED_PARAM = "SEED";

    /** */
    private static final int BASE_JDBC_PORT = 10800;

    /** */
    private static final int NEW_JDBC_PORT = 10802;

    /** */
    private static final long BENCHMARK_TIMEOUT = SF.applyLB(60 * 60 * 1000, 60 * 1000);

    /** */
    private static final long WARM_UP_TIMEOUT = 5_000;

    /** */
    private static final int REREUN_COUNT = 10;

    /** */
    private static final int REREUN_SUCCESS_COUNT = Math.max((int)(REREUN_COUNT * 0.7), 1);

    /** */
    private static final String JDBC_URL = "jdbc:ignite:thin://127.0.0.1:";

    /** Amount of discovery ports. */
    private static final int DISCOVERY_PORT_RANGE = 9;

    /** The very first discovery port in the range for base cluster. */
    private static final int BASE_DISCOVERY_PORT = 47500;

    /** The very first discovery port in the range for target cluster. */
    private static final int TARGET_DISCOVERY_PORT = BASE_DISCOVERY_PORT + DISCOVERY_PORT_RANGE + 1;

    /** Model factories. */
    private static final List<ModelFactory<?>> MODEL_FACTORIES = Arrays.asList(
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

    /**
     * {@code true} if base is Ignite, {@code false} if it is GG
     */
    private boolean baseIsIgnite;

    /**
     * {@code true} if target is Ignite, {@code false} if it is GG
     */
    private boolean targetIsIgnite;

    /** */
    private String baseVer;

    /** */
    private String targetVer;

    /** */
    private int seed;

    /** Current context for start external JVM. */
    private ExternalJvmContext currCtx;

    /** {@inheritDoc} */
    @Override protected Collection<Dependency> getDependencies(String igniteVer) {
        assert currCtx != null;

        return currCtx.deps();
    }

    /** {@inheritDoc} */
    @Override protected Collection<String> getJvmParams() {
        assert currCtx != null;

        return currCtx.jvmArgs();
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
            Supplier<QueryWithParams> qrySupplier = new RandomQuerySupplier(SCHEMA, seed);

            startBaseAndNewClusters(seed);

            try (Connection baseConn = createConnection(BASE_JDBC_PORT);
                 Connection targetConn = createConnection(NEW_JDBC_PORT)
            ) {
                QueryDuelBenchmark benchmark = new QueryDuelBenchmark(log, baseConn, targetConn);

                // 0. Warm-up.
                benchmark.runBenchmark(WARM_UP_TIMEOUT, qrySupplier);

                // 1. Initial run.
                List<QueryDuelResult> suspiciousQrys = benchmark.runBenchmark(BENCHMARK_TIMEOUT, qrySupplier).stream()
                    .filter(this::isResultUnsuccessful)
                    .collect(Collectors.toList());

                if (suspiciousQrys.isEmpty())
                    return; // No suspicious queries - no problem.

                if (log.isInfoEnabled())
                    log.info("Problematic queries number: " + suspiciousQrys.size());

                // 2. Rerun problematic queries to ensure they are not outliers.
                Map<QueryWithParams, AtomicInteger> qryToSuccessExecCnt = suspiciousQrys.stream()
                    .collect(Collectors.toMap(QueryDuelResult::query, r -> new AtomicInteger()));

                int i = 0;
                do {
                    if (qryToSuccessExecCnt.isEmpty())
                        break;

                    List<QueryDuelResult> rerunRes = benchmark.runBenchmark(getTestTimeout(),
                        new PredefinedQueriesSupplier(qryToSuccessExecCnt.keySet(), true));

                    Set<QueryWithParams> failedQrys = rerunRes.stream()
                        .filter(this::isResultUnsuccessful)
                        .map(QueryDuelResult::query)
                        .collect(Collectors.toSet());

                    qryToSuccessExecCnt.entrySet().stream()
                        .filter(e -> !failedQrys.contains(e.getKey()))
                        .forEach(e -> e.getValue().incrementAndGet());

                    qryToSuccessExecCnt.values().removeIf(v -> v.get() == REREUN_SUCCESS_COUNT);
                } while (i++ < REREUN_COUNT);

                suspiciousQrys.removeIf(r -> !qryToSuccessExecCnt.containsKey(r.query()));

                assertTrue("Found SQL performance regression for queries (seed is " + seed + "): "
                        + formatPretty(suspiciousQrys), suspiciousQrys.isEmpty());
            }
        }
        finally {
            stopClusters();
        }
    }

    /**
     * Starts base and new Ignite clusters. Also creates all tables and populate data.
     *
     * @param seed Random seed.
     */
    private void startBaseAndNewClusters(int seed) throws Exception {
        // Base cluster.
        try (ExternalJvmContext ignored = createContext(baseVer, baseIsIgnite)) {
            startGrid(3, baseVer, new NodeConfigurationClosure("1", BASE_DISCOVERY_PORT, BASE_JDBC_PORT),
                ignite -> createTablesAndPopulateData(ignite, seed));
            startGrid(4, baseVer, new NodeConfigurationClosure("2", BASE_DISCOVERY_PORT, BASE_JDBC_PORT));
        }

        rmJvmInstance = null; // clear remote instance because we are going to start separate cluster now

        // Target cluster
        try (ExternalJvmContext ignored = createContext(targetVer, targetIsIgnite)) {
            startGrid(1, targetVer, new NodeConfigurationClosure("1", TARGET_DISCOVERY_PORT, NEW_JDBC_PORT),
                ignite -> createTablesAndPopulateData(ignite, seed));
            startGrid(2, targetVer, new NodeConfigurationClosure("2", TARGET_DISCOVERY_PORT, NEW_JDBC_PORT));
        }
    }

    /**
     * @param res Query duel result.
     * @return {@code true} if a query execution time in the target engine
     * is not much longer than in the base one.
     */
    private boolean isResultUnsuccessful(QueryDuelResult res) {
        if (res.targetError() != null)
            return true;

        if (res.baseError() != null)
            return false;

        long baseRes = res.baseExecutionTimeNanos();
        long targetRes = res.targetExecutionTimeNanos();
        final double epsilon = 500_000; // Let's say 0.5ms is about statistical error.

        if (targetRes < baseRes || targetRes < epsilon)
            // execution is faster than on the base version
            // or both times not greater than epsilon
            return false;

        double target = Math.max(targetRes, epsilon);
        double base = Math.max(baseRes, epsilon);

        return target / base > 2;
    }

    /** */
    private void initParam() {
        String baseVerParam = System.getProperty(BASE_VERSION_PARAM);
        String targetVerParam = System.getProperty(TARGET_VERSION_PARAM);
        String baseIsIgniteParam = System.getProperty(BASE_IS_IGNITE_PARAM);
        String targetIsIgniteParam = System.getProperty(TARGET_IS_IGNITE_PARAM);
        String seedParam = System.getProperty(SEED_PARAM);

        baseVer = !F.isEmpty(baseVerParam) ? baseVerParam : DEFAULT_BASE_VERSION;
        targetVer = !F.isEmpty(targetVerParam) ? targetVerParam : DEFAULT_TARGET_VERSION;

        baseIsIgnite = !F.isEmpty(baseIsIgniteParam) ? Boolean.parseBoolean(baseIsIgniteParam) : DEFAULT_BASE_IS_IGNITE;
        targetIsIgnite = !F.isEmpty(targetIsIgniteParam) ? Boolean.parseBoolean(targetIsIgniteParam) : DEFAULT_TARGET_IS_IGNITE;

        seed = DEFAULT_SEED != null ? DEFAULT_SEED : !F.isEmpty(seedParam) ? Integer.parseInt(seedParam) : ThreadLocalRandom.current().nextInt();

        if (log.isInfoEnabled()) {
            log.info("Test was started with params:\n"
                + "\tseed=" + seed + "\n"
                + "\tbaseVersion=" + baseVer + "\n"
                + "\tbaseIsIgnite=" + baseIsIgnite + "\n"
                + "\ttargetVersion=" + targetVer + "\n"
                + "\ttargetIsIgnite=" + targetIsIgnite + "\n"
            );
        }
    }

    /**
     * Stops both new and old clusters.
     */
    public void stopClusters() {
        IgniteProcessProxy.killAll();

        // Just in case
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
        for (ModelFactory<?> mdlFactory : MODEL_FACTORIES)
            createAndPopulateTable(ignite, mdlFactory, seed);
    }

    /** */
    private static <T> void createAndPopulateTable(Ignite ignite, ModelFactory<T> factory, int seed) {
        factory.init(seed);

        QueryEntity qryEntity = factory.queryEntity();
        CacheConfiguration<Long, T> cacheCfg = new CacheConfiguration<Long, T>(factory.tableName())
            .setQueryEntities(Collections.singleton(qryEntity))
            .setSqlSchema("PUBLIC");

        IgniteCache<Long, T> personCache = ignite.createCache(cacheCfg);

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
    private static IgniteConfiguration prepareNodeConfig(
        IgniteConfiguration cfg,
        int discoPort,
        int jdbcPort,
        String consistentId
    ) {
        return cfg
            .setLocalHost("127.0.0.1")
            .setPeerClassLoadingEnabled(false)
            .setConsistentId(consistentId)
            .setDiscoverySpi(
                new TcpDiscoverySpi()
                    .setLocalPort(discoPort)
                    .setLocalPortRange(DISCOVERY_PORT_RANGE)
                    .setIpFinder(
                        new TcpDiscoveryVmIpFinder(true)
                            .setAddresses(
                                singletonList("127.0.0.1:" + discoPort + ".." + (discoPort + DISCOVERY_PORT_RANGE))
                            )
                    )
            )
            .setClientConnectorConfiguration(
                new ClientConnectorConfiguration().setPort(jdbcPort)
            );
    }

    /**
     * Configuration closure.
     */
    private static class NodeConfigurationClosure implements IgniteInClosure<IgniteConfiguration> {
        /** */
        private final String consistentId;

        /** Lower port for discovery port range. */
        private final int discoPort;

        /** Port for JDBC client connection. */
        private final int jdbcPort;

        /** */
        public NodeConfigurationClosure(String consistentId, int discoPort, int jdbcPort) {
            this.consistentId = consistentId;
            this.discoPort = discoPort;
            this.jdbcPort = jdbcPort;
        }

        /** {@inheritDoc} */
        @Override public void apply(IgniteConfiguration cfg) {
            prepareNodeConfig(cfg, discoPort, jdbcPort, consistentId);
        }
    }

    /**
     * @param ver Version.
     * @param isIgnite Is ignite.
     * @param jvmArgs Additional args for JVM.
     */
    private ExternalJvmContext createContext(String ver, boolean isIgnite, @Nullable List<String> jvmArgs) {
        List<Dependency> dependencies = new ArrayList<>();

        String grpId = groupId(isIgnite);

        dependencies.add(new Dependency("core", grpId, "ignite-core", ver, false));
        dependencies.add(new Dependency("core", grpId, "ignite-core", ver, true));
        dependencies.add(new Dependency("indexing", grpId, "ignite-indexing", ver, false));

        dependencies.add(h2Dependency(ver, isIgnite));

        return (currCtx = new ExternalJvmContext(dependencies, jvmArgs));
    }

    /**
     * @param ver Version.
     * @param isIgnite Is ignite.
     */
    private ExternalJvmContext createContext(String ver, boolean isIgnite) {
        return createContext(ver, isIgnite, null);
    }

    /**
     * @param verStr Version string.
     * @param isIgnite Is ignite.
     */
    private Dependency h2Dependency(String verStr, boolean isIgnite) {
        IgniteProductVersion ver = IgniteProductVersion.fromString(verStr);

        if (isIgnite || ver.compareTo(IgniteProductVersion.fromString("8.7.6")) <= 0)
            return new Dependency("h2", "com.h2database", "h2", "1.4.197", false);

        return new Dependency("h2", groupId(isIgnite), "ignite-h2", verStr, false);
    }

    /**
     * @param isIgnite Is ignite.
     */
    private String groupId(boolean isIgnite) {
        return isIgnite ? "org.apache.ignite" : "org.gridgain";
    }

    /** */
    private class ExternalJvmContext implements AutoCloseable {
        /** */
        private final List<Dependency> deps;

        /** */
        private final List<String> jvmArgs;

        /**
         * @param deps Deps.
         */
        public ExternalJvmContext(
            @Nullable List<Dependency> deps,
            @Nullable List<String> jvmArgs
        ) {
            this.deps = F.isEmpty(deps) ? emptyList() : unmodifiableList(deps);
            this.jvmArgs = F.isEmpty(jvmArgs) ? emptyList() : unmodifiableList(jvmArgs);
        }

        /** */
        public List<Dependency> deps() {
            return deps;
        }

        /** */
        public List<String> jvmArgs() {
            return jvmArgs;
        }

        /** {@inheritDoc} */
        @Override public void close() throws Exception {
            currCtx = null;
        }
    }
}
