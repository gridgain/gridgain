package org.apache.ignite.internal.benchmarks.jmh.runner;

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.jetbrains.annotations.Nullable;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.ChainedOptionsBuilder;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@Fork(1)
@Warmup(iterations = 10, time = 2)
@Measurement(iterations = 20, time = 2)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
public abstract class AbstractMultiNodeBenchmark extends BaseBenchmark {
    public static final int DEFAULT_THREADS_COUNT = 4;

    private static final int BASE_PORT = 47500;

    protected static final String FIELD_VAL = new String(new char[100]).replace("\0", "a");

    protected static final String FIELD_VAL_WITH_SPACES = FIELD_VAL + "   ";

    protected static final String TABLE_NAME = "USERTABLE";

    private final List<Ignite> igniteServers = new ArrayList<>();

    protected static Ignite publicIgnite;

    @Param({"" + DEFAULT_THREADS_COUNT})
    protected int threads;

    @Nullable
    protected String clusterConfiguration() {
        return "ignite {}";
    }

    /**
     * Starts ignite node and creates table {@link #TABLE_NAME}.
     */
    @Setup
    public void nodeSetUp() throws Exception {
        System.setProperty("jraft.available_processors", "2");
        if (!remote()) {
            startCluster();
        }

        try {
            // Create tables on the cluster's start-up.
            createTablesOnStartup();
        } catch (Throwable th) {
            nodeTearDown();

            throw th;
        }
    }

    protected void createTablesOnStartup() {
        // createTable(TABLE_NAME);
    }

    /**
     * Stops the cluster.
     *
     * @throws Exception In case of any error.
     */
    @TearDown
    public void nodeTearDown() throws Exception {
        publicIgnite.close();
        for (Ignite igniteServer : igniteServers) {
            igniteServer.close();
        }
    }

    private void startCluster() throws Exception {
        if (remote()) {
            throw new AssertionError("Can't start the cluster in remote mode");
        }

        Path workDir = workDir();

        for (int i = 0; i < nodes(); i++) {
            int port = BASE_PORT + i;
            String nodeName = nodeName(port);

            IgniteConfiguration ops = cacheConfiguration(nodeName, i, workDir);

            Ignite ignite = Ignition.start(ops);
            igniteServers.add(ignite);
        }

        System.out.println("Waiting for topology");
        while (true) {
            long ver = igniteServers.get(0).cluster().topologyVersion();
            System.out.println("Got top ver: " + ver);
            if (ver != nodes()) {
                Thread.sleep(50);
            } else {
                break;
            }
        }

        System.out.println("Activate cluster");
        igniteServers.get(0).cluster().state(ClusterState.ACTIVE);

        IgniteConfiguration ops = cacheConfiguration("client", nodes() + 1, workDir);
        ops.setClientMode(true);
        publicIgnite = Ignition.start(ops);
    }

    private IgniteConfiguration cacheConfiguration(String nodeName, int i, Path workDir) throws Exception {
        return new IgniteConfiguration()
                .setIgniteInstanceName(nodeName)
                .setDiscoverySpi(new TcpDiscoverySpi()
                        .setLocalPort(BASE_PORT + i)
                        .setLocalPortRange(1)
                        .setIpFinder(new TcpDiscoveryVmIpFinder()
                                .setAddresses(Collections.singletonList("127.0.0.1:" + BASE_PORT))))
                .setLocalHost("127.0.0.1")
                .setWorkDirectory(workDir.toAbsolutePath().toString() + "/" + nodeName)
                .setDataStorageConfiguration(new DataStorageConfiguration()
                        .setWalSegmentSize(128 * 1024 * 1024)
                        .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                                .setPersistenceEnabled(true)
                                .setMaxSize(maxMemorySize())))
                .setCacheConfiguration(new CacheConfiguration(TABLE_NAME)
                        //.setQueryEntities(Collections.singleton(queryEntity))
                        .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
                        .setAffinity(new RendezvousAffinityFunction(false, partitionCount()))
                        .setBackups(replicaCount() - 1)
                );
                //.setPluginConfigurations(new GridGainConfiguration().setLicenseUrl("c:/work/license.xml"));
    }

    private static String nodeName(int port) {
        return "node_" + port;
    }

    protected Path workDir() throws Exception {
        return Files.createTempDirectory("tmpDirPrefix").toFile().toPath();
    }

    protected int nodes() {
        return 1;
    }

    protected int partitionCount() {
        return RendezvousAffinityFunction.DFLT_PARTITION_COUNT;
    }

    protected int replicaCount() {
        return 1;
    }

    protected boolean remote() {
        return false;
    }

    static void runBenchmark(Class<?> cls, String[] args) throws RunnerException {
        Map<String, String> params = new HashMap<>(args.length);

        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            if (arg.startsWith("jmh.")) {
                String[] av = arg.substring(4).split("=");
                params.put(av[0], av[1]);
            }
        }

        final String threadsParamName = "threads";
        int threadsCount = params.containsKey(threadsParamName) ? Integer.parseInt(params.get(threadsParamName)) : DEFAULT_THREADS_COUNT;
        ChainedOptionsBuilder builder = new OptionsBuilder()
                .include(".*" + cls.getSimpleName() + ".*")
                // .jvmArgsAppend("-Djmh.executor=VIRTUAL")
                // .addProfiler(JavaFlightRecorderProfiler.class, "configName=profile.jfc");
                .threads(threadsCount);

        for (Map.Entry<String, String> entry : params.entrySet()) {
            builder.param(entry.getKey(), entry.getValue());
        }

        new Runner(builder.build()).run();
    }
}
