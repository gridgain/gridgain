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

import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.List;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;

import static java.nio.file.StandardWatchEventKinds.ENTRY_CREATE;
import static java.nio.file.StandardWatchEventKinds.ENTRY_DELETE;

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
    protected boolean startClient() {
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

    /** */
    protected void checkQuery(Result res, String sql) {
        checkQuery(res, sql, 1);
    }

    /** */
    protected void checkQuery(Result res, String sql, int threadNum) {
        WatchService watchSvc = null;
        WatchKey watchKey = null;

        try {
            watchSvc = FileSystems.getDefault().newWatchService();

            Path workDir = getWorkDir();

            watchKey = workDir.register(watchSvc, ENTRY_CREATE, ENTRY_DELETE);

            multithreaded(() -> grid(0).cache(DEFAULT_CACHE_NAME)
                .query(new SqlFieldsQuery(sql)
                    .setLazy(false))
                .getAll(), threadNum);
        }
        catch (Exception e) {
            assertFalse("Unexpected exception:" + X.getFullStackTrace(e) ,res.success);

            IgniteSQLException sqlEx = X.cause(e, IgniteSQLException.class);

            assertNotNull(sqlEx);

            if (res == Result.ERROR_GLOBAL_QUOTA)
                assertTrue("Wrong message:" + X.getFullStackTrace(e), sqlEx.getMessage().contains("Global quota exceeded."));
            else
                assertTrue("Wrong message:" + X.getFullStackTrace(e), sqlEx.getMessage().contains("Query quota exceeded."));
        }
        finally {
            try {
                if (watchKey != null) {
                    List<WatchEvent<?>> dirEvts = watchKey.pollEvents();

                    assertEquals("Disk spilling not happened.", res.offload, !dirEvts.isEmpty());
                }

                assertWorkDirClean();

                checkMemoryManagerState();
            }
            finally {
                U.closeQuiet(watchSvc);
            }
        }
    }

    /** */
    enum Result {
        /** */
        SUCCESS_WITH_OFFLOADING(true, true),

        /** */
        SUCCESS_NO_OFFLOADING(false, true),

        /** */
        ERROR_GLOBAL_QUOTA(false, false),

        /** */
        ERROR_QUERY_QUOTA(false, false);

        /** */
        Result(boolean offload, boolean success) {
            this.offload = offload;
            this.success = success;
        }

        /** */
        final boolean offload;

        /** */
        final boolean success;
    }
}
