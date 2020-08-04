/*
 * Copyright 2020 GridGain Systems, Inc. and Contributors.
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
package org.apache.ignite.util;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import javax.management.DynamicMBean;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.commandline.StatisticsCommandArg;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridNearAtomicSingleUpdateRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridNearAtomicUpdateResponse;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearSingleGetRequest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearSingleGetResponse;
import org.apache.ignite.spi.metric.jmx.JmxMetricExporterSpi;
import org.junit.Test;

import static java.util.Arrays.asList;
import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_INVALID_ARGUMENTS;
import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_OK;
import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_UNEXPECTED_ERROR;
import static org.apache.ignite.internal.commandline.CommandList.STATISTICS;
import static org.apache.ignite.internal.managers.communication.GridIoManager.DIAGNOSTICS_MESSAGES;
import static org.apache.ignite.internal.managers.communication.GridIoManager.MSG_MEASURED_TYPES;
import static org.apache.ignite.internal.managers.communication.GridIoManager.MSG_STAT_TOTAL_PROCESSING_TIME;
import static org.apache.ignite.internal.managers.communication.GridIoManager.MSG_STAT_TOTAL_QUEUE_WAITING_TIME;
import static org.apache.ignite.internal.processors.metric.GridMetricManager.DIAGNOSTIC_METRICS;
import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.metricName;
import static org.apache.ignite.internal.visor.statistics.MessageStatsTaskArg.StatisticsType.PROCESSING;
import static org.apache.ignite.internal.visor.statistics.MessageStatsTaskArg.StatisticsType.QUEUE_WAITING;
import static org.apache.ignite.testframework.GridTestUtils.assertContains;
import static org.apache.ignite.testframework.GridTestUtils.assertMatches;
import static org.apache.ignite.testframework.GridTestUtils.runMultiThreadedAsync;

public class GridCommandHandlerMessageStatisticsTest extends GridCommandHandlerClusterPerMethodAbstractTest {
    private static final int NODES_CNT = 2;

    private static final Set<Class> ATOMIC_UPDATE_MSGS = new HashSet<>(asList(
        GridNearAtomicSingleUpdateRequest.class,
        GridNearSingleGetRequest.class,
        GridNearSingleGetResponse.class,
        GridNearAtomicUpdateResponse.class
    ));

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setMetricExporterSpi(new JmxMetricExporterSpi());
    }

    private void imitateLoad() throws Exception {
        grid(0).getOrCreateCache(DEFAULT_CACHE_NAME);

        int procCnt = Runtime.getRuntime().availableProcessors();

        List<IgniteCache<Integer, Integer>> cacheProxies = new LinkedList<>();

        for (int i = 0; i < NODES_CNT; i++)
            cacheProxies.add(grid(i).cache(DEFAULT_CACHE_NAME));

        runMultiThreadedAsync(() -> {
            try {
                for (int i = 0; i < 300; i++) {
                    IgniteCache<Integer, Integer> cache = cacheProxies.get(i % NODES_CNT);

                    int j = i;

                    Integer p = cache.get(j);

                    if (p == null)
                        p = 0;

                    cache.put(j, p + 1);
                }
            }
            catch (Exception e) {
                throw new IgniteException(e);
            }
        }, procCnt * 5, "txAsyncLoad").get();
    }

    @Test
    public void testMessageStat() throws Exception {
        persistenceEnable(false);

        IgniteEx ignite = startGrids(2);

        injectTestSystemOut();

        imitateLoad();

        boolean autoConfirmation = this.autoConfirmation;

        try {
            this.autoConfirmation = false;

            callStatisticsCommand(PROCESSING.toString(), ignite, ignite.localNode().id().toString(), EXIT_CODE_OK);
            callStatisticsCommand(QUEUE_WAITING.toString(), ignite, ignite.localNode().id().toString(), EXIT_CODE_OK);

            callStatisticsCommand(PROCESSING.toString(), grid(1), grid(1).localNode().id().toString(), EXIT_CODE_OK);
            callStatisticsCommand(QUEUE_WAITING.toString(), grid(1), grid(1).localNode().id().toString(), EXIT_CODE_OK);

            callStatisticsCommand(PROCESSING.toString(), null, null, EXIT_CODE_OK);
            callStatisticsCommand(QUEUE_WAITING.toString(), null, null, EXIT_CODE_OK);

            callStatisticsCommand(PROCESSING.toString(), null, "qwe1", EXIT_CODE_INVALID_ARGUMENTS);
            callStatisticsCommand(QUEUE_WAITING.toString(), null, "qwe1", EXIT_CODE_INVALID_ARGUMENTS);

            callStatisticsCommand(null, ignite, ignite.localNode().id().toString(), EXIT_CODE_UNEXPECTED_ERROR);
            callStatisticsCommand("qwe1", ignite, ignite.localNode().id().toString(), EXIT_CODE_INVALID_ARGUMENTS);
        }
        finally {
            this.autoConfirmation = autoConfirmation;
        }
    }

    private String callStatisticsCommand(String statisticsType, Ignite node, String nodeId, int expectedExitCode) throws Exception {
        List<String> args = new LinkedList<>();

        args.add(STATISTICS.text());

        if (statisticsType != null) {
            args.add(StatisticsCommandArg.TYPE.toString());
            args.add(statisticsType);
        }

        if (nodeId != null) {
            args.add(StatisticsCommandArg.NODE.toString());
            args.add(nodeId);
        }

        assertEquals(expectedExitCode, execute(args));

        if (expectedExitCode == EXIT_CODE_OK) {
            String out = testOut.toString();

            checkOutput(out, statisticsType, node);

            return out;
        }

        return null;
    }

    private void checkOutput(String out, String statisticsType, Ignite node) throws Exception {
        assertContains(log, out, "Statistics report [" + statisticsType + "]");

        assertMatches(log, out, ".*?Message[ ]+Total Total time \\(ms\\)[ ]+Avg \\(ms\\)[ ]+<= 1[ ]+<= 5[ ]+<= 10[ ]+<= 30[ ]+<= 50[ ]+<= 100[ ]+<= 250[ ]+<= 500[ ]+<= 750[ ]+<= 1000[ ]+> 1000.*");

        DynamicMBean mBean = node == null ? null : metricRegistry(
            node.name(),
            DIAGNOSTIC_METRICS,
            metricName(DIAGNOSTICS_MESSAGES, statisticsType.equals(PROCESSING.toString()) ? MSG_STAT_TOTAL_PROCESSING_TIME : MSG_STAT_TOTAL_QUEUE_WAITING_TIME)
        );

        for (Class cls : MSG_MEASURED_TYPES) {
            String totalTime = mBean == null ? "[0-9]{1,5}" : String.valueOf(mBean.getAttribute(cls.getSimpleName()));

            // Checking that histograms for atomic gets and updates have some values, for tother types of messages they are empty.
            if (ATOMIC_UPDATE_MSGS.contains(cls))
                assertMatches(log, out, ".*?" + cls.getSimpleName() + "[ ]+[0-9]{2,5}[ ]+" + totalTime + "[ ]+[0-9]{1,5}\\.[0-9]{3}[ ]+[0-9]{2,5}[ ]+[0-9]{1,3}[ ]+[0-9]{1,3}[ ]+[0-9]{1,2}[ ]+[0-9]{1,2}[ ]+[0-9]{1}[ ]+0[ ]+0[ ]+0[ ]+0[ ]+0.*");
            else
                assertMatches(log, out, ".*?" + cls.getSimpleName() + "[ ]+0[ ]+0[ ]+0\\.000[ ]+0[ ]+0[ ]+0[ ]+0[ ]+0[ ]+0[ ]+0[ ]+0[ ]+0[ ]+0[ ]+0.*");
        }
    }
}