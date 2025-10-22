package org.apache.ignite.internal.processors.rest;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.ConnectorConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteVersionUtils;
import org.apache.ignite.internal.processors.cache.warmup.BlockedWarmUpConfiguration;
import org.apache.ignite.internal.processors.cache.warmup.BlockedWarmUpStrategy;
import org.apache.ignite.internal.processors.cache.warmup.WarmUpTestPluginProvider;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.plugin.AbstractTestPluginProvider;
import org.apache.ignite.plugin.PluginContext;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_JETTY_PORT;
import static org.apache.ignite.testframework.GridTestUtils.assertContains;
import static org.apache.ignite.testframework.GridTestUtils.runAsync;

/**
 * Tests REST API of a non fully started node.
 */
@WithSystemProperty(key = IGNITE_JETTY_PORT, value = "" + TestRestClient.DFLT_REST_PORT)
public class JettyRestBeforeNodeStartTest extends GridCommonAbstractTest {
    /** Rest client. */
    private final TestRestClient restClient = new TestRestClient(() -> null);

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();

        super.beforeTest();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /**
     * Tests that {@link GridRestCommand#NAME}, {@link GridRestCommand#VERSION} and {@link GridRestCommand#NODE_STATE_BEFORE_START}
     * return valid responses even if the node is not fully started yet.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testBasicDuringWarmup() throws Exception {
        WarmUpTestPluginProvider provider = new WarmUpTestPluginProvider();

        String consistentId = getTestIgniteInstanceName(0);

        IgniteConfiguration cfg = getConfiguration(consistentId).setPluginProviders(provider);
        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration().setPersistenceEnabled(true))
            .setDefaultWarmUpConfiguration(new BlockedWarmUpConfiguration())
        );

        cfg.setConnectorConfiguration(new ConnectorConfiguration().setHost("localhost"));

        IgniteInternalFuture<IgniteEx> startFut = runAsync(() -> startGrid(cfg));

        BlockedWarmUpStrategy blockedWarmUpStgy = (BlockedWarmUpStrategy)provider.strats.get(1);

        try {
            U.await(blockedWarmUpStgy.startLatch, 60, TimeUnit.SECONDS);

            assertContains(log, restClient.content(null, GridRestCommand.NAME), consistentId);
            assertContains(log, restClient.content(null, GridRestCommand.VERSION), IgniteVersionUtils.VER_STR);
            assertContains(log, restClient.content(null, GridRestCommand.NODE_STATE_BEFORE_START), "\"successStatus\":0");
        }
        finally {
            blockedWarmUpStgy.stopLatch.countDown();

            startFut.get(60_000);
        }
    }

    /**
     * Tests that {@link GridRestCommand#NAME}, {@link GridRestCommand#VERSION} and {@link GridRestCommand#NODE_STATE_BEFORE_START}
     * return valid responses even if the node is not fully started yet.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testBasicDuringPluginStart() throws Exception {
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch stopLatch = new CountDownLatch(1);

        AbstractTestPluginProvider provider = new AbstractTestPluginProvider() {
            @Override public String name() {
                return "BlockNodeStartPluginProvider";
            }

            @Override public void start(PluginContext ctx) throws IgniteCheckedException {
                super.start(ctx);

                startLatch.countDown();

                U.await(stopLatch, 30, TimeUnit.SECONDS);
            }
        };

        String consistentId = getTestIgniteInstanceName(0);

        IgniteConfiguration cfg = getConfiguration(consistentId).setPluginProviders(provider);
        cfg.setConnectorConfiguration(new ConnectorConfiguration().setHost("localhost"));

        IgniteInternalFuture<IgniteEx> startFut = runAsync(() -> startGrid(cfg));

        try {
            U.await(startLatch, 60, TimeUnit.SECONDS);

            assertContains(log, restClient.content(null, GridRestCommand.NAME), consistentId);
            assertContains(log, restClient.content(null, GridRestCommand.VERSION), IgniteVersionUtils.VER_STR);
            assertContains(log, restClient.content(null, GridRestCommand.NODE_STATE_BEFORE_START), "\"successStatus\":0");
        }
        finally {
            stopLatch.countDown();

            startFut.get(60_000);
        }
    }
}
