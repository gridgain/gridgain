/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.util;

import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.pagemem.wal.record.DataEntry;
import org.apache.ignite.internal.processors.cache.CacheObjectImpl;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheOperation;
import org.apache.ignite.internal.processors.cache.KeyCacheObjectImpl;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.GridAbstractTest;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.junit.Test;

import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_OK;
import static org.apache.ignite.internal.processors.cache.checker.processor.PartitionReconciliationProcessor.INTERRUPTING_MSG;

// TODO: 26.12.19 Add to appropriate suites.

/**
 * Tests for checking partition reconciliation.
 */
public class GridCommandHandlerPartitionReconciliationExtendedTest extends
    GridCommandHandlerClusterPerMethodAbstractTest {
    /** Test logger. */
    private final ListeningTestLogger log = new ListeningTestLogger(false, GridAbstractTest.log);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setGridLogger(log);

        cfg.setCacheConfiguration(new CacheConfiguration(DEFAULT_CACHE_NAME).setBackups(2));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        stopAllGrids();
    }

    /**
     * Tests for checking partition reconciliation cancel control.sh command.
     */
    @Test
    public void testPartitionReconciliationCancel() throws Exception {
        LogListener lsnr = LogListener.matches(s -> s.startsWith(INTERRUPTING_MSG)).times(3).build();
        log.registerListener(lsnr);

        startGrids(3);

        IgniteEx ignite = grid(0);
        ignite.cluster().active(true);

        try (IgniteDataStreamer streamer = ignite.dataStreamer(DEFAULT_CACHE_NAME)) {
            for (int i = 0; i < 100; i++)
                streamer.addData(i, i);
        }

        IgniteEx grid = grid(1);

        for (int i = 0; i < 100; i++)
            corruptDataEntry(grid.cachex(DEFAULT_CACHE_NAME).context(), i);

        assertEquals(0, reconciliationSessionId());

        GridTestUtils.runAsync(() -> assertEquals(EXIT_CODE_OK, execute("--cache", "partition_reconciliation", "--fix-mode", "--fix-alg",
            "MAJORITY", "--recheck-attempts", "100000")));

        assertTrue(GridTestUtils.waitForCondition(() ->  reconciliationSessionId() != 0, 10_000));

        assertEquals(EXIT_CODE_OK, execute("--cache", "partition_reconciliation_cancel"));

        assertEquals(0, reconciliationSessionId());

        assertTrue(lsnr.check(10_000));
    }

    /**
     *
     */
    @Test
    @WithSystemProperty(key = "RECONCILIATION_WORK_PROGRESS_PRINT_INTERVAL", value = "0")
    public void testProgressLogPrinted() throws Exception {
        LogListener lsnr = LogListener.matches(s -> s.startsWith("Partition reconciliation task [sesId=")).atLeast(1).build();
        log.registerListener(lsnr);

        startGrids(3);

        IgniteEx ignite = grid(0);
        ignite.cluster().active(true);

        assertEquals(EXIT_CODE_OK, execute("--cache", "partition_reconciliation", "--fix-mode", "--fix-alg", "MAJORITY", "--recheck-attempts", "1"));

        assertTrue(lsnr.check(10_000));
    }

    /**
     *
     */
    private long reconciliationSessionId() {
        List<Ignite> srvs = G.allGrids().stream().filter(g -> !g.configuration().getDiscoverySpi().isClientMode()).collect(toList());

        List<Long> collect;

        do {
            collect = srvs.stream()
                .map(g -> ((IgniteEx)g).context().diagnostic().getReconciliationSessionId())
                .distinct()
                .collect(toList());
        }
        while (collect.size() > 1);

        assert collect.size() == 1;

        return collect.get(0);
    }

    /**
     * Corrupts data entry.
     *
     * @param ctx Context.
     * @param key Key.
     * @param breakCntr Break counter.
     * @param breakData Break data.
     */
    protected void corruptDataEntry(
        GridCacheContext<Object, Object> ctx,
        Object key
    ) {
        int partId = ctx.affinity().partition(key);

        try {
            long updateCntr = ctx.topology().localPartition(partId).updateCounter();

            Object valToPut = ctx.cache().keepBinary().get(key);

            // Create data entry
            DataEntry dataEntry = new DataEntry(
                ctx.cacheId(),
                new KeyCacheObjectImpl(key, null, partId),
                new CacheObjectImpl(valToPut, null),
                GridCacheOperation.UPDATE,
                new GridCacheVersion(),
                new GridCacheVersion(),
                0L,
                partId,
                updateCntr
            );

            GridCacheDatabaseSharedManager db = (GridCacheDatabaseSharedManager)ctx.shared().database();

            db.checkpointReadLock();

            try {
                U.invoke(GridCacheDatabaseSharedManager.class, db, "applyUpdate", ctx, dataEntry, false);
            }
            finally {
                db.checkpointReadUnlock();
            }
        }
        catch (IgniteCheckedException e) {
            e.printStackTrace();
        }
    }
}
