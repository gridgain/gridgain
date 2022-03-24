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
package org.apache.ignite.internal.managers;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteDiagnosticMessage;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridDhtAtomicDeferredUpdateResponse;
import org.apache.ignite.internal.util.typedef.T4;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

public class IgniteDiagnosticMessagesBackPressureTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.getTransactionConfiguration().setTxTimeoutOnPartitionMapExchange(1_000);

        // Reduce the number of threads for simplicity.
        cfg.setSystemThreadPoolSize(2);

        cfg.setCommunicationSpi(new TestRecordingCommunicationSpi());

        return cfg;
    }

    @Test
    @WithSystemProperty(key = "IGNITE_LONG_OPERATIONS_DUMP_TIMEOUT_LIMIT", value = "1000")
    public void testDiagnosticMessageHandler() throws Exception {
        IgniteEx crd = (IgniteEx) startGridsMultiThreaded(3);

        IgniteEx client = startClientGrid(3);

        String cacheName = "test-atomic-cache-1";

        // Crerate a new atomic partitioned cache with two backups.
        IgniteCache<Integer, Integer> cache = client.getOrCreateCache(
            new CacheConfiguration<Integer, Integer>(cacheName)
                .setMaxConcurrentAsyncOperations(1024 *1024)
                .setBackups(2)
                .setAffinity(new RendezvousAffinityFunction(false, 32)));

        awaitPartitionMapExchange();

        // Let's block ack messages from backup nodes.
        TestRecordingCommunicationSpi spi0 = TestRecordingCommunicationSpi.spi(crd);
        spi0.blockMessages((node, msg) -> msg instanceof GridDhtAtomicDeferredUpdateResponse);

        TestRecordingCommunicationSpi spi1 = TestRecordingCommunicationSpi.spi(grid(1));
        spi1.blockMessages((node, msg) -> msg instanceof GridDhtAtomicDeferredUpdateResponse);
        spi1.record(IgniteDiagnosticMessage.class);

        TestRecordingCommunicationSpi spi2 = TestRecordingCommunicationSpi.spi(grid(2));
        spi2.blockMessages((node, msg) -> msg instanceof GridDhtAtomicDeferredUpdateResponse);
        spi2.record(IgniteDiagnosticMessage.class);

        // Populate the cache in async manner. We don't want to wait for completion all cache operations.
        // The big number of updates is needed for to enlist a huge number of cache futures into the partition release future.
        List<Integer> primaryKeys0 = findKeys(crd.localNode(), crd.cache(cacheName), 1, 0, 0);
        Integer primaryKey = primaryKeys0.get(0);
        for (int i = 0; i < (500 * 1_000); i++)
            cache.putAsync(primaryKey, i);

        // At least one update should be triggered for keys on server 1 and server 2
        // in order to block partition release future and, therefore, it initiates sending diagnostic messages.
        primaryKey = findKeys(grid(1).localNode(), grid(1).cache(cacheName), 1, 0, 0).get(0);
        grid(1).cache("test-atomic-cache-1").putAsync(primaryKey, 42);

        primaryKey = findKeys(grid(2).localNode(), grid(2).cache(cacheName), 1, 0, 0).get(0);
        grid(2).cache("test-atomic-cache-1").putAsync(primaryKey, 42);

        // All updates should be initiated at this moment.
        // Let's wait for ack messages from backups.
        spi0.waitForBlocked();
        spi1.waitForBlocked(1_000);
        spi2.waitForBlocked(1_000);

        // Start tracking sys pool on the corrdinator node. Just for logging.
        AtomicReference<List<T4<Long, Integer, Integer, String>>> infRef = new AtomicReference<>();
        AtomicBoolean stop = new AtomicBoolean();
        IgniteInternalFuture<?> futExec = GridTestUtils.runAsync(() -> {
            ThreadPoolExecutor exec = (ThreadPoolExecutor) crd.context().pools().getSystemExecutorService();
            List<T4<Long, Integer, Integer, String>> inf = new ArrayList<>();

            while (!stop.get()) {
                T4<Long, Integer, Integer, String> t = new T4<>();
                t.set1(exec.getCompletedTaskCount());
                t.set2(exec.getActiveCount());
                t.set3(exec.getQueue().size());

                StringBuilder sb = new StringBuilder();
                for (Runnable r : exec.getQueue())
                    sb.append("\t").append(r.toString()).append(U.nl());
                t.set4(sb.toString());

                inf.add(t);

                log.warning(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
                log.warning(">>>>> completedCnt = " + t.get1());
                log.warning(">>>>> activeCount = " + t.get2());
                log.warning(">>>>> queueSize = " + t.get3());
                log.warning(">>>>> tasks = [" + U.nl() + t.get4());
                log.warning(">>>>> ]");
                log.warning(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");

                doSleep(5_000);
            }
            infRef.set(inf);
        });

        // Initiate cluster wide partition map exchange by starting a new cache.
        IgniteInternalFuture<?> startCacheFut = GridTestUtils.runAsync(() -> {
            crd.getOrCreateCache("test-atomic-cache-2");
        });

        // Let's wait for diagnostic messages.
        spi1.waitForRecorded();
        spi2.waitForRecorded();

        spi0.stopBlock();
        spi1.stopBlock();
        spi2.stopBlock();

        startCacheFut.get(getTestTimeout());

        stop.set(true);
        futExec.get(getTestTimeout());

        log.warning(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
        for (T4<Long, Integer, Integer, String> t : infRef.get()) {
            log.warning("\t>>>>> completedCnt = " + t.get1() +
                ", activeCount = " + t.get2() +
                ", queueSize = " + t.get3() +
                ", tasks = " + t.get4());
        }
        log.warning(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
    }
}
