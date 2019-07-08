package org.apache.ignite.internal.metrics;

import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.NoOpFailureHandler;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonTest;
import org.junit.Test;

import static org.apache.ignite.testframework.GridTestUtils.runMultiThreadedAsync;


@GridCommonTest(group = "Kernal Self")
public class MetricProcessorTest extends GridCommonAbstractTest {
    private static final String CACHE_NAME = "testCache";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        CacheConfiguration<Integer, Integer> ccfg = defaultCacheConfiguration();
        ccfg.setName(CACHE_NAME);
        ccfg.setNearConfiguration(null);
        //ccfg.setNearConfiguration(new NearCacheConfiguration<>());
        ccfg.setStatisticsEnabled(true);

        cfg.setCacheConfiguration(ccfg);

        cfg.setFailureHandler(new NoOpFailureHandler());

        return cfg;
    }

    @Test
    public void dotest() throws Exception {
        Ignite ignite = startGrids(2);

        AtomicBoolean stop = new AtomicBoolean();

        IgniteInternalFuture<Long> loadFut = runMultiThreadedAsync(new Runnable() {
            @Override public void run() {
                IgniteCache<Integer, Integer> cache = ignite.cache(CACHE_NAME);

                ThreadLocalRandom rnd = ThreadLocalRandom.current();

                while (!stop.get()) {
                    int key = rnd.nextInt(10000);
                    int val = rnd.nextInt(10000);
                    int op = rnd.nextInt(100);

                    if (op < 60)
                        cache.put(key, val);
                    else if (op >= 60 && op < 80)
                        cache.get(key);
                    else
                        cache.remove(key);
                }
            }
        }, 4, "load-thread");

        IgniteInternalFuture exportFut = GridTestUtils.runAsync(new Runnable() {
            @Override public void run() {
                TestMetricsExporter exporter = new TestMetricsExporter();

                while (!stop.get()) {
                    String res = exporter.export(ignite);

                    log.info(res);

                    try {
                        U.sleep(1000);
                    }
                    catch (IgniteInterruptedCheckedException e) {
                        e.printStackTrace();
                    }
                }
            }
        });

        IgniteInternalFuture exportMsgFut = GridTestUtils.runAsync(new Runnable() {
            @Override public void run() {
                MetricExporter exporter = new MetricExporter();

                while (!stop.get()) {
                    try {
                        MetricMessage msg = exporter.export(ignite);

                        log.info("Metrics message: schemaFrame.size=" + msg.schemaFrameSize() + ", dataFrame.size=" + msg.dataFrameSize());

                        try {
                            U.sleep(1000);
                        } catch (IgniteInterruptedCheckedException e) {
                            e.printStackTrace();
                        }
                    }
                    catch (Exception e) {
                        log.error("Unexpected error:", e);
                    }
                }
            }
        });


        U.sleep(120_000);

        stop.set(true);

        loadFut.get();
        exportFut.get();
        exportMsgFut.get();
    }

    public class TestMetricsExporter {
        public String export(Ignite ignite) {
            Map<String, MetricRegistry> metrics = ((IgniteEx) ignite).context().metrics().metrics();

            StringBuilder sb = new StringBuilder();

            for (Map.Entry<String, MetricRegistry> e : metrics.entrySet())
                sb.append(export(e.getValue()));

            return sb.toString();
        }

        String export(MetricRegistry metricRegistry) {
            StringBuilder sb = new StringBuilder();

            for (Map.Entry<String, Gauge> e : metricRegistry.metrics().entrySet()) {
                sb.append(e.getKey()).append(" = ");

                Gauge g = e.getValue();

                if (g instanceof LongGauge)
                    sb.append(((LongGauge)g).value());

                sb.append('\n');
            }

            return sb.toString();
        }
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }
}