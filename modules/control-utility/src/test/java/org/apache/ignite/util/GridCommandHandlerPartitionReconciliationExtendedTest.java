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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.logging.Formatter;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import java.util.logging.StreamHandler;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.commandline.CommandHandler;
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
import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_OK;
import static org.apache.ignite.internal.processors.cache.GridCacheUtils.UTILITY_CACHE_NAME;
import static org.apache.ignite.internal.processors.cache.checker.processor.PartitionReconciliationProcessor.SESSION_CHANGE_MSG;

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
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /**
     * Tests for checking partition reconciliation cancel control.sh command.
     */
    @Test
    public void testPartitionReconciliationCancel() throws Exception {
        LogListener lsnr = LogListener.matches(s -> s.contains(SESSION_CHANGE_MSG)).times(3).build();
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

        GridTestUtils.runAsync(() -> assertEquals(EXIT_CODE_OK, execute("--cache", "partition_reconciliation", "--repair",
            "MAJORITY", "--recheck-attempts", "5")));

        assertTrue(GridTestUtils.waitForCondition(() -> reconciliationSessionId() != 0, 10_000));

        assertEquals(EXIT_CODE_OK, execute("--cache", "partition_reconciliation_cancel"));

        assertEquals(0, reconciliationSessionId());

        assertTrue(lsnr.check(10_000));
    }

    /**
     * Checks that start, status and finished messages exist.
     * Also, checks that finished messages contain correct number of conflicted and repaired keys.
     */
    @Test
    @WithSystemProperty(key = "RECONCILIATION_WORK_PROGRESS_PRINT_INTERVAL_SEC", value = "1")
    public void testProgressLogPrinted() throws Exception {
        int nodeCnt = 3;

        startGrids(nodeCnt);

        IgniteEx ignite = grid(0);
        ignite.cluster().state(ACTIVE);

        testProgressLogPrinted(nodeCnt, true);
        testProgressLogPrinted(nodeCnt, false);
    }

    /**
     * @param nodeCnt Number of nodes.
     * @param simpleCollector {@code true} if simple collector should be used.
     * @throws Exception If failed.
     */
    private void testProgressLogPrinted(int nodeCnt, boolean simpleCollector) throws Exception {
        LogListener lsnr1 = LogListener.matches(s -> s.startsWith("Partition reconciliation status [sesId="))
            .atLeast(nodeCnt)
            .build();

        LogListener lsnr2 = LogListener.matches(s -> s.startsWith("Partition reconciliation has started [sesId="))
            .times(nodeCnt)
            .build();

        List<String> finishMsgs = new ArrayList<>();
        LogListener lsnr3 = LogListener.matches(s -> {
                if (s.startsWith("Partition reconciliation has finished locally [sesId=")) {
                    finishMsgs.add(s);

                    return true;
                }
                return false;
            })
            .times(nodeCnt)
            .build();

        log.registerListener(lsnr1);
        log.registerListener(lsnr2);
        log.registerListener(lsnr3);

        IgniteCache<Integer, Integer> cache = grid(0).cache(DEFAULT_CACHE_NAME);

        int keysCnt = 100;
        int expectedConflicts = 0;
        for (int i = 0; i < keysCnt; i++) {
            cache.put(i, i);

            if (i % 10 == 0) {
                corruptDataEntry(grid(0).cachex(DEFAULT_CACHE_NAME).context(), i);
                expectedConflicts++;
            }
        }

        try {
            List<String> args = new ArrayList<>(Arrays.asList(
                "--cache",
                "partition_reconciliation",
                DEFAULT_CACHE_NAME,
                "--repair",
                "MAJORITY",
                "--recheck-attempts",
                "1",
                "--batch-size",
                "10",
                "--recheck-delay",
                "1"
            ));

            if (simpleCollector)
                args.add("--local-output");

            assertEquals(EXIT_CODE_OK, execute(args.toArray(new String[0])));

            assertTrue(lsnr1.check(10_000));
            assertTrue(lsnr2.check(10_000));
            assertTrue(lsnr3.check(10_000));

            Pattern pattern = Pattern.compile(".*finished locally.*conflicts=(\\d+).*repaired=(\\d+).*");

            int totalConflicts = 0;
            int totalRepaired = 0;
            for (String msg : finishMsgs) {
                Matcher matcher = pattern.matcher(msg);

                assertTrue(matcher.matches());

                int conflicts = Integer.parseInt(matcher.group(1));
                int repaired = Integer.parseInt(matcher.group(2));

                assertTrue(expectedConflicts >= conflicts);
                assertTrue(expectedConflicts >= repaired);

                totalConflicts += conflicts;
                totalRepaired += repaired;
            }

            assertEquals(expectedConflicts, totalConflicts);
            assertEquals(expectedConflicts, totalRepaired);
        }
        finally {
            log.unregisterListener(lsnr1);
            log.unregisterListener(lsnr2);
            log.unregisterListener(lsnr3);
        }
    }

    /**
     * Check that utility works only with specified subset of caches in case parameter is set
     */
    @Test
    public void testWorkWithSubsetOfCaches() throws Exception {
        Set<String> usedCaches = new HashSet<>();
        LogListener lsnr = fillCacheNames(usedCaches);
        log.registerListener(lsnr);

        startGrids(3);

        IgniteEx ignite = grid(0);
        ignite.cluster().active(true);

        for (int i = 1; i <= 3; i++)
            ignite.getOrCreateCache(DEFAULT_CACHE_NAME + i);

        assertEquals(EXIT_CODE_OK, execute("--cache", "partition_reconciliation", "default, default3"));

        assertTrue(lsnr.check(10_000));

        assertTrue(usedCaches.containsAll(Arrays.asList("default", "default3")));
        assertEquals(usedCaches.size(), 2);
    }

    /**
     * Check that utility works only with specified subset of caches in case parameter is set, using regexp.
     */
    @Test
    public void testWorkWithSubsetOfCachesByRegexp() throws Exception {
        Set<String> usedCaches = new HashSet<>();
        LogListener lsnr = fillCacheNames(usedCaches);
        log.registerListener(lsnr);

        startGrids(3);

        IgniteEx ignite = grid(0);
        ignite.cluster().active(true);

        for (int i = 1; i <= 3; i++)
            ignite.getOrCreateCache(DEFAULT_CACHE_NAME + i);

        assertEquals(EXIT_CODE_OK, execute("--cache", "partition_reconciliation", "default.*"));

        assertTrue(lsnr.check(10_000));

        assertTrue(usedCaches.containsAll(Arrays.asList("default", "default1", "default2", "default3")));
        assertEquals(usedCaches.size(), 4);
    }

    /**
     * Check that utility works with system caches only if that cache type is specified.
     */
    @Test
    public void testWorkWithInternalCaches() throws Exception {
        Set<String> usedCaches = getUsedCachesForArgs("--cache", "partition_reconciliation", "ignite-sys-cache, ignite-sys-atomic-cache@default-ds-group");

        assertTrue(usedCaches.containsAll(Arrays.asList("ignite-sys-cache", "ignite-sys-atomic-cache@default-ds-group")));
        assertEquals(2, usedCaches.size());
    }

    /**
     * Check that utility works with system caches if regexp specified.
     */
    @Test
    public void testWorkWithSystemCachesByRegexp() throws Exception {
        Set<String> usedCaches = getUsedCachesForArgs("--cache", "partition_reconciliation", "ignite-sys.*");

        assertTrue(usedCaches.containsAll(Arrays.asList("ignite-sys-cache", "ignite-sys-atomic-cache@default-ds-group")));
        assertEquals(2, usedCaches.size());
    }

    /**
     * Check that utility works with all caches if caches argument is not specified.
     */
    @Test
    public void testWorkWithAllCaches() throws Exception {
        Set<String> usedCaches = getUsedCachesForArgs("--cache", "partition_reconciliation");

        assertTrue(usedCaches.containsAll(Arrays.asList("ignite-sys-cache",
            "ignite-sys-atomic-cache@default-ds-group", "default", "default1", "default2", "default3")));
        assertEquals(6, usedCaches.size());
    }

    /**
     * Tests that utility will started with all available user caches.
     */
    @Test
    public void testWorkWithAllSetOfCachesIfParameterAbsent() throws Exception {
        Set<String> usedCaches = new HashSet<>();
        LogListener lsnr = fillCacheNames(usedCaches);
        log.registerListener(lsnr);

        startGrids(3);

        IgniteEx ignite = grid(0);
        ignite.cluster().active(true);

        List<String> setOfCaches = new ArrayList<>();
        setOfCaches.add(DEFAULT_CACHE_NAME);
        setOfCaches.add(UTILITY_CACHE_NAME);

        for (int i = 1; i <= 3; i++)
            setOfCaches.add(ignite.getOrCreateCache(DEFAULT_CACHE_NAME + i).getName());

        assertEquals(EXIT_CODE_OK, execute("--cache", "partition_reconciliation"));

        assertTrue(lsnr.check(10_000));

        assertTrue(usedCaches.containsAll(setOfCaches));
        assertEquals(usedCaches.size(), setOfCaches.size());
    }

    /**
     * Creates internal and custom caches and runs reconciliation cmd with args.
     */
    private Set<String> getUsedCachesForArgs(String... args) throws Exception {
        Set<String> usedCaches = new HashSet<>();
        LogListener lsnr = fillCacheNames(usedCaches);
        log.registerListener(lsnr);

        startGrids(3);

        IgniteEx ignite = grid(0);
        ignite.cluster().active(true);

        for (int i = 1; i <= 3; i++) {
            ignite.atomicLong(DEFAULT_CACHE_NAME + i, 0, true);
            ignite.getOrCreateCache(DEFAULT_CACHE_NAME + i);
        }

        assertEquals(EXIT_CODE_OK, execute(args));

        assertTrue(lsnr.check(10_000));

        log.unregisterListener(lsnr);

        return usedCaches;
    }

    /**
     * Checks that a wrong cache name leads to interruption of utility.
     */
    @Test
    public void testWrongCacheNameTerminatesOperation() throws Exception {
        String wrongCacheName = "wrong_cache_name";
        LogListener errorMsg = LogListener.matches(s -> s.contains("The cache '" + wrongCacheName + "' doesn't exist.")).atLeast(1).build();
        log.registerListener(errorMsg);

        startGrids(3);

        IgniteEx ignite = grid(0);
        ignite.cluster().active(true);

        Logger logger = CommandHandler.initLogger(null);

        logger.addHandler(new StreamHandler(System.out, new Formatter() {
            /** {@inheritDoc} */
            @Override public String format(LogRecord record) {
                log.info(record.getMessage());

                return record.getMessage() + "\n";
            }
        }));

        assertEquals(EXIT_CODE_OK, execute(new CommandHandler(logger), "--cache", "partition_reconciliation", wrongCacheName));

        assertTrue(errorMsg.check(10_000));
    }

    /**
     * Extract cache names which used for a start.
     */
    private LogListener fillCacheNames(Set<String> usedCaches) {
        Pattern r = Pattern.compile("Partition reconciliation has started.*caches=\\[(.*)\\]\\].*");

        LogListener lsnr = LogListener.matches(s -> {
            Matcher m = r.matcher(s);

            boolean found = m.find();

            if (found && m.group(1) != null && !m.group(1).isEmpty())
                usedCaches.addAll(Arrays.asList(m.group(1).split(", ")));

            return found;
        }).atLeast(1).build();

        return lsnr;
    }

    /**
     * Extract reconciliation sessionId.
     */
    private long reconciliationSessionId() {
        List<Ignite> srvs = G.allGrids().stream().filter(g -> !g.configuration().getDiscoverySpi().isClientMode()).collect(toList());

        List<Long> collect;

        do {
            collect = srvs.stream()
                .map(g -> ((IgniteEx)g).context().diagnostic().reconciliationExecutionContext().sessionId())
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
                updateCntr,
                DataEntry.EMPTY_FLAGS
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
