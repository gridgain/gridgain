/*
 * Copyright 2022 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.internal.processors.cache.persistence.wal;

import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.failure.FailureHandler;
import org.apache.ignite.failure.FailureType;
import org.apache.ignite.failure.StopNodeFailureHandler;
import org.apache.ignite.internal.processors.cache.persistence.wal.memtracker.PageMemoryTrackerConfiguration;
import org.apache.ignite.testframework.junits.GridAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Abstract WAL delta records consistency test.
 */
public abstract class AbstractWalDeltaConsistencyTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setConsistentId(igniteInstanceName);

        cfg.setDataStorageConfiguration(getDataStorageConfiguration());

        cfg.setPluginConfigurations(new PageMemoryTrackerConfiguration().setEnabled(true)
            .setCheckPagesOnCheckpoint(checkPagesOnCheckpoint()));

        return cfg;
    }

    /**
     * @param name Cache name.
     * @return Cache configuration.
     */
    protected <K, V> CacheConfiguration<K, V> cacheConfiguration(String name) {
        return GridAbstractTest.<K, V>defaultCacheConfiguration().setName(name);
    }

    /**
     * Check page memory on each checkpoint.
     */
    protected boolean checkPagesOnCheckpoint() {
        return false;
    }

    /** {@inheritDoc} */
    @Override protected FailureHandler getFailureHandler(String igniteInstanceName) {
        return checkPagesOnCheckpoint() ?
            new StopNodeFailureHandler() {
                @Override public boolean handle(Ignite ignite, FailureContext failureCtx) {
                    if (failureCtx.type() == FailureType.SYSTEM_WORKER_BLOCKED)
                        return false;

                    return super.handle(ignite, failureCtx);
                }
            } :
            super.getFailureHandler(igniteInstanceName);
    }

    /**
     * Default configuration contains one data region ('dflt-plc') with persistence enabled.
     * This method should be overridden by subclasses if another data storage configuration is needed.
     *
     * @return Data storage configuration used for starting of grid.
     */
    protected DataStorageConfiguration getDataStorageConfiguration() {
        return new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                .setInitialSize(256 * 1024 * 1024)
                .setMaxSize(256 * 1024 * 1024)
                .setPersistenceEnabled(true)
                .setName("dflt-plc"));
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        cleanPersistenceDir();
    }
}
