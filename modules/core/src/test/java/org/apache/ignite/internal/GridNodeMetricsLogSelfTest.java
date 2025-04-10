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

import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ExecutorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsFullMessage;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.testframework.GridStringLogger;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonTest;
import org.junit.Test;

import static org.apache.ignite.internal.TestRecordingCommunicationSpi.spi;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/**
 * Check logging local node metrics
 */
@SuppressWarnings({"ProhibitedExceptionDeclared"})
@GridCommonTest(group = "Kernal")
public class GridNodeMetricsLogSelfTest extends GridCommonAbstractTest {
    /** Executor name for setExecutorConfiguration */
    private static final String CUSTOM_EXECUTOR_0 = "Custom executor 0";

    /** Executor name for setExecutorConfiguration */
    private static final String CUSTOM_EXECUTOR_1 = "Custom executor 1";

    /** */
    public static final String IN_MEMORY_REGION = "userTransientDataRegion";

    /** */
    private GridStringLogger strLog = new GridStringLogger(false, this.log);

    /** Coordinator test logger */
    private ListeningTestLogger crdLog;

    /** Coordinator log listener */
    private LogListener rebalanceInfoLsnr;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setMetricsLogFrequency(1000);

        cfg.setExecutorConfiguration(new ExecutorConfiguration(CUSTOM_EXECUTOR_0),
            new ExecutorConfiguration(CUSTOM_EXECUTOR_1));

        cfg.setCommunicationSpi(new TestRecordingCommunicationSpi());

        if (igniteInstanceName.equals(getTestIgniteInstanceName(0))) {
            assertNotNull("Coordinator logger is null", crdLog);

            cfg.setGridLogger(crdLog);
        }
        else
            cfg.setGridLogger(strLog);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        strLog.reset();

        strLog.logLength(300_000);

        rebalanceInfoLsnr = prepareRebalanceInfoLogListener();

        startGrids(2);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testNodeMetricsLog() throws Exception {
        IgniteCache<Integer, String> cache1 = grid(0).createCache("TestCache1");
        IgniteCache<Integer, String> cache2 = grid(1).createCache(
            new CacheConfiguration<Integer, String>("TestCache2")
                .setDataRegionName(persistenceEnabled() ? IN_MEMORY_REGION : "default"));

        cache1.put(1, "one");
        cache2.put(2, "two");

        final String metricsHdr = "Metrics for local node (to disable set 'metricsLogFrequency' to 0)";

        assertTrue("Metrics weren't printed.", waitForCondition(() -> strLog.toString().contains(metricsHdr), 5000));

        // Check that nodes are alive.
        assertEquals("one", cache1.get(1));
        assertEquals("two", cache2.get(2));

        String logOutput = strLog.toString();

        checkNodeMetricsFormat(logOutput);

        checkOffHeapMetrics(logOutput);

        checkDataRegionsMetrics(logOutput);

        if (!persistenceEnabled()) {
            spi(grid(0)).blockMessages(GridDhtPartitionsFullMessage.class, getTestIgniteInstanceName(2));
            spi(grid(1)).blockMessages(GridDhtPartitionsFullMessage.class, getTestIgniteInstanceName(2));
        }

        IgniteInternalFuture fut = GridTestUtils.runAsync(() -> {
            try {
                startGrid(2);
            }
            catch (Exception e) {
                log.error("Failed to start third node", e);
            }

            if (persistenceEnabled()) {
                spi(grid(0)).blockMessages(GridDhtPartitionsFullMessage.class, getTestIgniteInstanceName(2));
                spi(grid(1)).blockMessages(GridDhtPartitionsFullMessage.class, getTestIgniteInstanceName(2));

                resetBaselineTopology();
            }
        });

        assertTrue("Expected rebalance info not found", waitForCondition(() -> rebalanceInfoLsnr.check(), 60_000));

        spi(grid(0)).stopBlock();
        spi(grid(1)).stopBlock();

        fut.get();
    }

    /**
     * Prepares rebalance info log listener.
     *
     * @return Prepared log listener
     */
    private LogListener prepareRebalanceInfoLogListener() {
        crdLog = new ListeningTestLogger(log);

        LogListener rebalanceInfoLsnr =
            LogListener.matches("Current affinity assignment is not ideal, it is waiting for cache:")
                .andMatches(Pattern.compile("(?s).*grp=\\[grpId=.*, nodes=\\[node=\\[id=.*, partsNum=.*].*"))
                .build();

        crdLog.registerListener(rebalanceInfoLsnr);

        return rebalanceInfoLsnr;
    }

    /**
     * Check node metrics format.
     *
     * @param fullLog Logging output.
     */
    protected void checkNodeMetricsFormat(String fullLog) {
        String msg = "Metrics are missing in the log or have an unexpected format";

        // Don't check the format strictly, but check that all expected metrics are present.
        assertTrue(msg, fullLog.contains("Metrics for local node (to disable set 'metricsLogFrequency' to 0)"));
        assertTrue(msg, fullLog.matches("(?s).*Node \\[id=.*, consistentId=.*, name=.*, version=.*, uptime=.*].*"));
        assertTrue(msg, fullLog.matches("(?s).*Coordinator \\[id=.*, consistentId=.*, version=.*].*"));
        assertTrue(msg, fullLog.matches("(?s).*Cluster \\[hosts=.*, CPUs=.*, servers=.*, clients=.*, topVer=.*, " +
            "minorTopVer=.*, state=.*, clusterId=.*, clusterTag=.*].*"));
        assertTrue(msg, fullLog.matches("(?s).*Network \\[addrs=\\[.*], localHost=.*, discoPort=.*, commPort=.*].*"));
        assertTrue(msg, fullLog.matches("(?s).*CPU \\[CPUs=.*, curLoad=.*, avgLoad=.*, GC=.*].*"));
        assertTrue(msg, fullLog.matches("(?s).*Page memory \\[pages=.*].*"));
        assertTrue(msg, fullLog.matches("(?s).*Heap \\[used=.*, free=.*, comm=.*].*"));
        assertTrue(msg, fullLog.matches("(?s).*Off-heap memory \\[sizeUsedByData=.*, used=.*, free=.*, allocated=.*].*"));
        assertTrue(msg, fullLog.matches("(?s).* region \\[type=internal, persistence=(true|false), lazyAlloc=(true|false).*"));
        assertTrue(msg, fullLog.matches("(?s).* region \\[type=default, persistence=(true|false), lazyAlloc=(true|false).*"));
        assertTrue(msg, fullLog.matches("(?s).*... initCfg=.*, maxCfg=.*, sizeUsedByData=.*, usedRam=.*, freeRam=.*, allocRam=.*].*"));
        assertTrue(msg, fullLog.matches("(?s).*Outbound messages queue \\[size=.*].*"));
        assertTrue(msg, fullLog.matches("(?s).*Public thread pool \\[active=.*, idle=.*, qSize=.*].*"));
        assertTrue(msg, fullLog.matches("(?s).*System thread pool \\[active=.*, idle=.*, qSize=.*].*"));
        assertTrue(msg, fullLog.matches("(?s).*Striped thread pool \\[active=.*, idle=.*, qSize=.*].*"));
        assertTrue(msg, fullLog.matches("(?s).*" + CUSTOM_EXECUTOR_0 + " \\[active=.*, idle=.*, qSize=.*].*"));
        assertTrue(msg, fullLog.matches("(?s).*" + CUSTOM_EXECUTOR_1 + " \\[active=.*, idle=.*, qSize=.*].*"));
        assertTrue(msg, fullLog.matches("(?s).*No plugins found.*"));
    }

    /**
     * Check memory metrics values.
     *
     * @param logOutput Logging output.
     */
    protected void checkDataRegionsMetrics(String logOutput) {
        Set<String> regions = new HashSet<>();

        Matcher matcher = Pattern.compile("(?m).{2,} {3}(?<name>.+) region \\[type=(default|internal|user), " +
                "persistence=(?<persistent>true|false), lazyAlloc=(true|false),\\s*\\.\\.\\.\\s*" +
                "initCfg=(?<init>[-\\d]+)MB, maxCfg=(?<max>[-\\d]+)MB, " +
                "sizeUsedByData=(?<data>[-\\d]+).*MB, usedRam=(?<used>[-\\d]+).*MB, " +
                "freeRam=(?<free>[-.\\d]+)%, allocRam=(?<alloc>[-\\d]+)MB(, allocTotal=(?<total>[-\\d]+)MB)?]")
            .matcher(logOutput);

        while (matcher.find()) {
            String subj = logOutput.substring(matcher.start(), matcher.end());

            int init = Integer.parseInt(matcher.group("init"));
            int max = Integer.parseInt(matcher.group("max"));
            int used = Integer.parseInt(matcher.group("used"));
            int data = Integer.parseInt(matcher.group("data"));
            double free = Double.parseDouble(matcher.group("free"));
            int alloc = Integer.parseInt(matcher.group("alloc"));
            boolean persistent = Boolean.parseBoolean(matcher.group("persistent"));

            assertTrue(init + " should be non negative: " + subj, init >= 0);
            assertTrue(max + " is less then " + init + ": " + subj, max >= init);
            assertTrue(used + " should be non negative: " + subj, used >= 0);
            assertTrue(data + " should be non negative: " + subj, data >= 0);
            assertTrue(alloc + " is less then " + used + ": " + subj, alloc >= used);
            assertTrue(free + " is not between 0 and 100: " + subj, 0 <= free && free <= 100);

            String name = matcher.group("name").trim();

            if (persistent) {
                int total = Integer.parseInt(matcher.group("total"));

                assertTrue(total + " is less then " + used + ": " + subj, total >= used);
            } else
                assertTrue(F.isEmpty(matcher.group("total")));

            regions.add(name);
        }

        Set<String> expRegions = grid(0).context().cache().context().database().dataRegions().stream()
            .map(v -> v.config().getName().trim())
            .collect(Collectors.toSet());

        assertFalse("No data regions in the log.", regions.isEmpty());

        assertEquals("Unexpected names of data regions.", expRegions, regions);
    }

    /**
     * Check memory metrics values.
     *
     * @param logOutput Logging output.
     */
    protected void checkOffHeapMetrics(String logOutput) {
        Matcher matcher = Pattern.compile("Off-heap memory " +
                "\\[sizeUsedByData=(?<data>[-.\\d]*).*, used=(?<used>[-.\\d]*).*, " +
                "free=(?<free>[-.\\d]*).*, allocated=(?<comm>[-.\\d]*).*]")
            .matcher(logOutput);

        assertTrue("Off-heap metrics not found in the log.", matcher.find());

        String subj = logOutput.substring(matcher.start(), matcher.end());

        assertFalse("\"data\" cannot be empty: " + subj, F.isEmpty(matcher.group("data")));
        assertFalse("\"used\" cannot be empty: " + subj, F.isEmpty(matcher.group("used")));
        assertFalse("\"free\" cannot be empty: " + subj, F.isEmpty(matcher.group("free")));
        assertFalse("\"comm\" cannot be empty: " + subj, F.isEmpty(matcher.group("comm")));

        int data = Integer.parseInt(matcher.group("data"));
        int used = Integer.parseInt(matcher.group("used"));
        int comm = Integer.parseInt(matcher.group("comm"));
        double free = Double.parseDouble(matcher.group("free"));

        assertTrue(data + " should be non negative: " + subj, data >= 0);
        assertTrue(used + " should be non negative: " + subj, used >= 0);
        assertTrue(comm + " is less then " + used + ": " + subj, comm >= used);
        assertTrue(free + " is not between 0 and 100: " + subj, 0 <= free && free <= 100);
    }

    /** */
    protected boolean persistenceEnabled() {
        return false;
    }
}
