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
package org.apache.ignite.internal.processors.query.oom;

import org.apache.ignite.configuration.IgniteConfiguration;

/**
 * Basic class for test cases for memory quota static configuration.
 */
public abstract class AbstractMemoryQuotaStaticConfigurationTest extends DiskSpillingAbstractTest {
    /** */
    private Boolean offloadingEnabled;

    /** */
    private String globalQuota;

    /** */
    private String qryQuota;

    /** {@inheritDoc} */
    @Override protected boolean persistence() {
        return false;
    }

    /** {@inheritDoc} */
    @Override protected int nodeCount() {
        return 1;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {

    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();
        offloadingEnabled = null;
        globalQuota = null;
        qryQuota = null;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        destroyGrid();
    }

    /** */
    @Override protected boolean startClient() {
        return false;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setSqlOffloadingEnabled(IgniteConfiguration.DFLT_SQL_QUERY_OFFLOADING_ENABLED);
        cfg.setSqlQueryMemoryQuota(IgniteConfiguration.DFLT_SQL_QUERY_MEMORY_QUOTA);
        cfg.setSqlGlobalMemoryQuota(IgniteConfiguration.DFLT_SQL_QUERY_GLOBAL_MEMORY_QUOTA);

        if (offloadingEnabled != null)
            cfg.setSqlOffloadingEnabled(offloadingEnabled);

        if (globalQuota != null)
            cfg.setSqlGlobalMemoryQuota(globalQuota);

        if (qryQuota != null)
            cfg.setSqlQueryMemoryQuota(qryQuota);

        return cfg;
    }

    /** */
    protected void initGrid(String globalQuota, String queryQuota, Boolean offloadingEnabled) throws Exception {
        this.globalQuota = globalQuota;
        this.qryQuota = queryQuota;
        this.offloadingEnabled = offloadingEnabled;

        initGrid();
    }
}
