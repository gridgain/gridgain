package org.apache.ignite.internal.processors.cluster;

import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

@WithSystemProperty(key = "IGNITE_SEPARATE_BASELINE_AUTO_ADJUST_FEATURE", value = "true")
public class SeparateBaselineAutoAdjustTest extends GridCommonAbstractTest {

    private static int autoAdjustTimeout = 5_000;

    private static int scaleUpAutoAdjustTimeout = 5_000;

    private static int scaleDownAutoAdjustTimeout = 5_000;

    /** */
    protected boolean isPersistent() {
        return true;
    }

    /**
     * @throws Exception if failed.
     */
    @Before
    public void before() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();

        scaleUpAutoAdjustTimeout = 5_000;

        scaleDownAutoAdjustTimeout = 5_000;
    }

    /**
     * @throws Exception if failed.
     */
    @After
    public void after() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }


    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setConsistentId(igniteInstanceName);

        DataStorageConfiguration storageCfg = new DataStorageConfiguration();

        storageCfg.getDefaultDataRegionConfiguration()
            .setPersistenceEnabled(isPersistent())
            .setMaxSize(500L * 1024 * 1024);

        storageCfg
            .setWalSegments(3)
            .setWalSegmentSize(512 * 1024);

        cfg.setDataStorageConfiguration(storageCfg);

        cfg.setCommunicationSpi(new TestRecordingCommunicationSpi());

//        cfg.setLifecycleBeans(lifecycleBean);

        return cfg;
    }

    @Test
    public void testAutoAdjustWithSeparateScaleUpBeforeScaleDown() throws Exception {
        scaleDownAutoAdjustTimeout = 5_000 * 2;

        IgniteEx ignite0 = startGrids(3);

        ignite0.cluster().baselineAutoAdjustEnabled(true);

        ignite0.cluster().baselineScaleUpAutoAdjustEnabled(true);
        ignite0.cluster().baselineScaleUpAutoAdjustTimeout(scaleUpAutoAdjustTimeout);

        ignite0.cluster().baselineScaleDownAutoAdjustEnabled(true);
        ignite0.cluster().baselineScaleDownAutoAdjustTimeout(scaleDownAutoAdjustTimeout);

        ignite0.cluster().state(ClusterState.ACTIVE);

        startGrid(3);

        stopGrid(1);

        assertTrue(waitForCondition(
            () -> ignite0.cluster().currentBaselineTopology().size() == 4,
            scaleUpAutoAdjustTimeout * 2
        ));

        assertTrue(waitForCondition(
            () -> ignite0.cluster().currentBaselineTopology().size() == 3,
            scaleDownAutoAdjustTimeout * 2
        ));
    }

    @Test
    public void testAutoAdjustWithSeparateScaleUpAfterScaleDown() throws Exception {
        scaleUpAutoAdjustTimeout = 5_000 * 2;

        IgniteEx ignite0 = startGrids(3);

        ignite0.cluster().baselineAutoAdjustEnabled(true);

        ignite0.cluster().baselineScaleUpAutoAdjustEnabled(true);
        ignite0.cluster().baselineScaleUpAutoAdjustTimeout(scaleUpAutoAdjustTimeout);

        ignite0.cluster().baselineScaleDownAutoAdjustEnabled(true);
        ignite0.cluster().baselineScaleDownAutoAdjustTimeout(scaleDownAutoAdjustTimeout);

        ignite0.cluster().state(ClusterState.ACTIVE);

        stopGrid(1);

        startGrid(3);

        assertTrue(waitForCondition(
            () -> ignite0.cluster().currentBaselineTopology().size() == 2,
            scaleDownAutoAdjustTimeout * 2
        ));

        assertTrue(waitForCondition(
            () -> ignite0.cluster().currentBaselineTopology().size() == 3,
            scaleUpAutoAdjustTimeout * 2
        ));
    }

    @Test
    public void testAutoAdjustWithOnlyScaleUpEnabled() throws Exception {
        IgniteEx ignite0 = startGrids(3);

        ignite0.cluster().baselineAutoAdjustEnabled(true);

        ignite0.cluster().baselineScaleUpAutoAdjustEnabled(true);
        ignite0.cluster().baselineScaleUpAutoAdjustTimeout(scaleUpAutoAdjustTimeout);

        ignite0.cluster().state(ClusterState.ACTIVE);

        stopGrid(1);

        startGrid(3);

        assertTrue(waitForCondition(
            () -> ignite0.cluster().currentBaselineTopology().size() == 4,
            scaleUpAutoAdjustTimeout * 2
        ));
    }

    @Test
    public void testAutoAdjustWithOnlyScaleDownEnabled() throws Exception {
        IgniteEx ignite0 = startGrids(3);

        ignite0.cluster().baselineAutoAdjustEnabled(true);

        ignite0.cluster().baselineScaleDownAutoAdjustEnabled(true);
        ignite0.cluster().baselineScaleDownAutoAdjustTimeout(scaleDownAutoAdjustTimeout);

        ignite0.cluster().state(ClusterState.ACTIVE);

        startGrid(3);

        System.out.println("========================================");
        System.out.println("4th node started");

        stopGrid(1);

        System.out.println("========================================");
        System.out.println("2nd node stopped");

        awaitPartitionMapExchange();

        assertTrue(waitForCondition(
            () -> ignite0.cluster().currentBaselineTopology().size() == 2,
            scaleDownAutoAdjustTimeout * 2
        ));

        System.out.println("========================================");
        System.out.println("test finished");
    }
}
