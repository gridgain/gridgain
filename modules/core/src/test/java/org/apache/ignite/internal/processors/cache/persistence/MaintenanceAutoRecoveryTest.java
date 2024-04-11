package org.apache.ignite.internal.processors.cache.persistence;

import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.maintenance.MaintenanceRegistry;
import org.apache.ignite.maintenance.MaintenanceTask;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Before;
import org.junit.Test;


public class MaintenanceAutoRecoveryTest extends GridCommonAbstractTest {

    String consistentId;

    @Before
    public void setUp() throws Exception {
        cleanPersistenceDir();
    }

    @Override
    protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setConsistentId(gridName);

        cfg.setCacheConfiguration(new CacheConfiguration<>()
                .setName(DEFAULT_CACHE_NAME)
                .setBackups(1)
        );

        cfg.setDataStorageConfiguration(new DataStorageConfiguration().setDefaultDataRegionConfiguration(
                new DataRegionConfiguration().setPersistenceEnabled(true)
        ));

        consistentId = cfg.getConsistentId().toString();

        return cfg;
    }

    @Test
    @WithSystemProperty(key = "MM_AUTO_SHUTDOWN_AFTER_RECOVERY", value = "true")
    public void testAutoShutdown() throws Exception {
        IgniteEx ign0 = startGrid(0);
        IgniteEx ign1 = startGrid(1);

        String taskName = "test";

        ign0.context().maintenanceRegistry().registerMaintenanceTask(
                new MaintenanceTask(taskName, "foo", null)
        );
        stopGrid(0);

        ign0 = startGrid(0);

        MaintenanceRegistry maintenanceRegistry = ign0.context().maintenanceRegistry();

        assert maintenanceRegistry.isMaintenanceMode();

        maintenanceRegistry.unregisterMaintenanceTask(taskName);

        GridTestUtils.waitForCondition(
                () -> ign1.cluster().nodes().size() == 1,
                10000
        );

        System.out.println();
    }
}
