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

import java.io.File;
import java.util.List;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.pagemem.wal.record.DataEntry;
import org.apache.ignite.internal.processors.cache.CacheObjectImpl;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheOperation;
import org.apache.ignite.internal.processors.cache.KeyCacheObjectImpl;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.junit.Test;

import static java.io.File.separatorChar;
import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_OK;
import static org.apache.ignite.internal.processors.cache.checker.util.ConsistencyCheckUtils.RECONCILIATION_DIR;
import static org.apache.ignite.testframework.GridTestUtils.assertContains;
import static org.apache.ignite.testframework.GridTestUtils.assertNotContains;

/**
 * Common partition reconciliation tests.
 */
public class GridCommandHandlerPartitionReconciliationCommonTest
    extends GridCommandHandlerClusterPerMethodAbstractTest {

    /** */
    public static final int INVALID_KEY = 100;

    /** */
    public static final String VALUE_PREFIX = "abc_";

    /** */
    protected static File dfltDiagnosticDir;

    /** */
    protected IgniteEx ignite;

    /** @inheritDoc */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        cleanPersistenceDir();

        initDiagnosticDir();

        cleanDiagnosticDir();

        ignite = startGrids(4);

        ignite.cluster().active(true);
    }

    /** @inheritDoc */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        super.afterTestsStopped();
    }

    /** @inheritDoc */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        prepareCache();
    }

    /** @inheritDoc */
    @Override protected void afterTest() throws Exception {
        // No-op.
    }

    /**
     * Check the simple case of inconsistency
     * <b>Preconditions:</b>
     * <ul>
     *     <li>Grid with 4 nodes is started, cache with 3 backups with some data is up and running</li>
     * </ul>
     * <b>Test steps:</b>
     * <ol>
     *     <li>Emulate non-consistency when keys were updated on primary and 2 backups</li>
     *     <li>Start bin/control.sh --cache partition_reconciliation</li>
     * </ol>
     * <b>Expected result:</b>
     * <ul>
     *     <li>Check that report contains info about one and only one inconsistent key</li>
     * </ul>
     */
    @Test
    public void testSimpleCaseOfInconsistencyDetection() {
        ignite(0).cache(DEFAULT_CACHE_NAME).put(INVALID_KEY, VALUE_PREFIX + INVALID_KEY);

        List<ClusterNode> nodes = ignite(0).cachex(DEFAULT_CACHE_NAME).cache().context().affinity().
            nodesByKey(
                INVALID_KEY,
                ignite(0).cachex(DEFAULT_CACHE_NAME).context().topology().readyTopologyVersion());

        corruptDataEntry(((IgniteEx)grid(nodes.get(2))).cachex(
            DEFAULT_CACHE_NAME).context(),
            INVALID_KEY,
            false,
            false,
            new GridCacheVersion(0, 0, 2),
            null);

        injectTestSystemOut();

        assertEquals(EXIT_CODE_OK, execute("--cache", "partition_reconciliation", "--console"));

        assertContains(log, testOut.toString(), "INCONSISTENT KEYS: 1");
    }

    /**
     * Check that console scoped report contains aggregated info about amount of inconsistent keys and
     * doesn't contain detailed info in case not using --console argument.
     * <b>Preconditions:</b>
     * <ul>
     *     <li>Grid with 4 nodes is started, cache with 3 backups with some data is up and running</li>
     * </ul>
     * <b>Test steps:</b>
     * <ol>
     *     <li>Emulate non-consistency when keys were updated on primary and 2 backups</li>
     *     <li>Start bin/control.sh --cache partition_reconciliation</li>
     * </ol>
     * <b>Expected result:</b>
     * <ul>
     *     <li>Check that console report contains aggregated info about amount of inconsistent keys and
     *     doesn't contain detailed info.</li>
     * </ul>
     */
    @Test
    public void testConsoleScopedReportContainsAggregatedInfoAboutAmountAndNotDetailedInfoInCaseOfNonConsoleMode() {
        ignite(0).cache(DEFAULT_CACHE_NAME).put(INVALID_KEY, VALUE_PREFIX + INVALID_KEY);

        List<ClusterNode> nodes = ignite(0).cachex(DEFAULT_CACHE_NAME).cache().context().affinity().
            nodesByKey(
                INVALID_KEY,
                ignite(0).cachex(DEFAULT_CACHE_NAME).context().topology().readyTopologyVersion());

        corruptDataEntry(((IgniteEx)grid(nodes.get(1))).cachex(
            DEFAULT_CACHE_NAME).context(),
            INVALID_KEY,
            false,
            false,
            new GridCacheVersion(0, 0, 2),
            null);

        injectTestSystemOut();

        assertEquals(EXIT_CODE_OK, execute("--cache", "partition_reconciliation"));

        assertContains(log, testOut.toString(), "INCONSISTENT KEYS: 1");

        assertNotContains(log, testOut.toString(), "<nodeConsistentId>, <nodeId>: <value> <version>");
    }

    /**
     * Check that console scoped report contains detailed info in case of using --console argument.
     * <b>Preconditions:</b>
     * <ul>
     *     <li>Grid with 4 nodes is started, cache with 3 backups with some data is up and running</li>
     * </ul>
     * <b>Test steps:</b>
     * <ol>
     *     <li>Emulate non-consistency when keys were updated on primary and 2 backups</li>
     *     <li>Start bin/control.sh --cache partition_reconciliation --console</li>
     * </ol>
     * <b>Expected result:</b>
     * <ul>
     *     <li>Check that console scoped report contains detailed info about inconsistent keys.</li>
     * </ul>
     */
    @Test
    public void testConsoleScopedReportContainsAggregatedInfoAboutAmountAndNotDetailedInfoInCaseOfConsoleMode() {
        ignite(0).cache(DEFAULT_CACHE_NAME).put(INVALID_KEY, VALUE_PREFIX + INVALID_KEY);

        List<ClusterNode> nodes = ignite(0).cachex(DEFAULT_CACHE_NAME).cache().context().affinity().
            nodesByKey(
                INVALID_KEY,
                ignite(0).cachex(DEFAULT_CACHE_NAME).context().topology().readyTopologyVersion());

        corruptDataEntry(((IgniteEx)grid(nodes.get(1))).cachex(
            DEFAULT_CACHE_NAME).context(),
            INVALID_KEY,
            false,
            false,
            new GridCacheVersion(0, 0, 2),
            null);

        injectTestSystemOut();

        assertEquals(EXIT_CODE_OK, execute("--cache", "partition_reconciliation", "--console"));

        assertContains(log, testOut.toString(), "INCONSISTENT KEYS: 1");

        assertContains(log, testOut.toString(), "<nodeConsistentId>, <nodeId>: <value> <version>");
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    protected void initDiagnosticDir() throws IgniteCheckedException {
        dfltDiagnosticDir = new File(U.defaultWorkDirectory() + separatorChar + RECONCILIATION_DIR);
    }

    /**
     * Clean diagnostic directories.
     */
    protected void cleanDiagnosticDir() {
        U.delete(dfltDiagnosticDir);
    }

    // TODO: 26.12.19 Refactoring needed, another PRTest class has similar method.
    /**
     * Create cache and populate it with some data.
     */
    @SuppressWarnings("unchecked") protected void prepareCache() {
        ignite(0).destroyCache(DEFAULT_CACHE_NAME);

        ignite(0).createCache(new CacheConfiguration<>(DEFAULT_CACHE_NAME)
            .setAffinity(new RendezvousAffinityFunction(false, 16))
            .setCacheMode(CacheMode.PARTITIONED)
            .setBackups(3));

        try (IgniteDataStreamer streamer = ignite(0).dataStreamer(DEFAULT_CACHE_NAME)) {
            for (int i = 0; i < INVALID_KEY; i++)
                streamer.addData(i, VALUE_PREFIX + i);
        }
    }

    // TODO: 26.12.19 Refactoring needed, another PRTest class has similar method.
    /**
     * Corrupts data entry.
     *
     * @param ctx Context.
     * @param key Key.
     * @param breakCntr Break counter.
     * @param breakData Break data.
     * @param ver GridCacheVersion to use.
     * @param brokenValPostfix Postfix to add to value if breakData flag is set to true.
     */
    protected void corruptDataEntry(
        GridCacheContext<Object, Object> ctx,
        Object key,
        boolean breakCntr,
        boolean breakData,
        GridCacheVersion ver,
        String brokenValPostfix
    ) {
        int partId = ctx.affinity().partition(key);

        try {
            long updateCntr = ctx.topology().localPartition(partId).updateCounter();

            Object valToPut = ctx.cache().keepBinary().get(key);

            if (breakCntr)
                updateCntr++;

            if (breakData)
                valToPut = valToPut.toString() + brokenValPostfix;

            // Create data entry
            DataEntry dataEntry = new DataEntry(
                ctx.cacheId(),
                new KeyCacheObjectImpl(key, null, partId),
                new CacheObjectImpl(valToPut, null),
                GridCacheOperation.UPDATE,
                new GridCacheVersion(),
                ver,
                0L,
                partId,
                updateCntr
            );

            GridCacheDatabaseSharedManager db = (GridCacheDatabaseSharedManager)ctx.shared().database();

            db.checkpointReadLock();

            try {
                U.invoke(GridCacheDatabaseSharedManager.class, db, "applyUpdate", ctx, dataEntry,
                    false);
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
