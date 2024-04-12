/*
 * Copyright 2024 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.internal.processors.cache.persistence;

import java.util.concurrent.atomic.AtomicBoolean;
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

/**
 * Check that node will shut down automatically in maintenance mode once all maintenance tasks are fulfilled.
 */
public class MaintenanceAutoRecoveryTest extends GridCommonAbstractTest {

    String consistentId;

    @Before
    public void setUp() throws Exception {
        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
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

    /** Node will auto shut down once all maintenance tasks are completed. */
    @Test
    @WithSystemProperty(key = "IGNITE_MAINTENANCE_AUTO_SHUTDOWN_AFTER_RECOVERY", value = "true")
    public void testAutoShutdownEnabled() throws Exception {
        IgniteEx ign = startGrid(0);
        String taskName = "test";

        ign.context().maintenanceRegistry().registerMaintenanceTask(
                new MaintenanceTask(taskName, "foo", null)
        );
        stopGrid(0);

        ign = startGrid(0);

        MaintenanceRegistry maintenanceRegistry = ign.context().maintenanceRegistry();

        assertTrue(maintenanceRegistry.isMaintenanceMode());

        AtomicBoolean stopGuard = GridTestUtils.getFieldValue(ign, "stopGuard");

        assertFalse(stopGuard.get());

        maintenanceRegistry.unregisterMaintenanceTask(taskName);

        GridTestUtils.waitForCondition(stopGuard::get, 5000);
    }

    /** Node won't auto shut down once all maintenance tasks are completed. */
    @Test
    @WithSystemProperty(key = "IGNITE_MAINTENANCE_AUTO_SHUTDOWN_AFTER_RECOVERY", value = "false")
    public void testAutoShutdownDisabled() throws Exception {
        IgniteEx ign = startGrid(0);
        String taskName = "test";

        ign.context().maintenanceRegistry().registerMaintenanceTask(
                new MaintenanceTask(taskName, "foo", null)
        );
        stopGrid(0);

        ign = startGrid(0);

        MaintenanceRegistry maintenanceRegistry = ign.context().maintenanceRegistry();

        assertTrue(maintenanceRegistry.isMaintenanceMode());

        AtomicBoolean stopGuard = GridTestUtils.getFieldValue(ign, "stopGuard");

        assertFalse(stopGuard.get());

        maintenanceRegistry.unregisterMaintenanceTask(taskName);

        Thread.sleep(5000);//we wait for some time to register that node remains active

        assertFalse(stopGuard.get());
    }
}
