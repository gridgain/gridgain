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

package org.apache.ignite.internal;

import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import static org.apache.ignite.internal.processors.datastructures.DataStructuresProcessor.VOLATILE_DATA_REGION_NAME;

/**
 * Check logging local node metrics with PDS enabled.
 */
public class GridNodeMetricsLogPdsSelfTest extends GridNodeMetricsLogSelfTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        DataStorageConfiguration memCfg = new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(
                new DataRegionConfiguration()
                    .setMaxSize(30 * 1024 * 1024)
                    .setPersistenceEnabled(true))
            .setDataRegionConfigurations(new DataRegionConfiguration()
                .setName("userTransientDataRegion")
                .setMaxSize(20 * 1024 * 1024)
                .setPersistenceEnabled(false))
            .setWalMode(WALMode.LOG_ONLY);

        cfg.setDataStorageConfiguration(memCfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        cleanPersistenceDir();

        super.beforeTest();

        grid(0).cluster().active(true);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void checkNodeMetricsFormat(String logOutput) {
        super.checkNodeMetricsFormat(logOutput);

        assertTrue("Metrics are missing in the log or have an unexpected format",
            logOutput.matches("(?s).*Ignite persistence \\[used=[\\d]+MB].*"));
    }

    /** */
    @Override protected boolean persistenceEnabled(String name) {
        return !VOLATILE_DATA_REGION_NAME.equals(name) && !name.contains("Transient");
    }
}
