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

package org.apache.ignite.internal.processors.cache.distributed.dht;

import java.util.*;
import java.util.regex.*;

import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Test correct behaviour of {@link org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionsStateValidator} class.
 */
public class GridCachePartitionsUpdateCountersAndSizeTest extends GridCommonAbstractTest {
    /** Cache name. */
    private static final String CACHE_NAME = "cacheTest";

    /** Listener for parsing patterns in log. */
    private ListeningTestLogger testLog = new ListeningTestLogger(false, log());

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setConsistentId(igniteInstanceName);

        cfg.setCacheConfiguration(new CacheConfiguration(CACHE_NAME)
            .setBackups(2)
            .setAffinity(new RendezvousAffinityFunction(false, 32))
        );

        if (igniteInstanceName.endsWith("0"))
            cfg.setGridLogger(testLog);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     *  Four tests that partitions state validation works correctly and show partition size always:
     *
     * Start three-nodes grid,
     * arguments: cnt - partition counters are inconsistent(boolean)
     *           size - partition size are inconsistent(boolean)
     * @throws Exception If failed.
     */
    private void startThreeNodesGrid(boolean cnt, boolean size) throws Exception {
        LogListener lsnrCnt = SizeCounterLogListener.matches(Pattern.compile
            ("Partitions update counters are inconsistent for")).build();

        LogListener lsnrSize = SizeCounterLogListener.matches(Pattern.compile
            ("Partitions cache sizes are inconsistent for")).build();

        LogListener lsnrSizeCnt = SizeCounterLogListener.matches(Pattern.compile
            ("Partitions cache size and update counters are inconsistent for")).build();

        IgniteEx ignite = startGrids(3);
        ignite.cluster().active(true);
        awaitPartitionMapExchange();

        testLog.clearListeners();

        testLog.registerListener(lsnrSize);
        testLog.registerListener(lsnrCnt);
        testLog.registerListener(lsnrSizeCnt);

        // Populate cache to increment update counters.
        for (int i = 0; i < 1000; i++)
            ignite.cache(CACHE_NAME).put(i, i);

        if (cnt) {
            // Modify update counter for some partition.
            ignite.cachex(CACHE_NAME).context().topology().localPartitions().get(0).updateCounter(99L);
        }

        if (size) {
            // Modify update size for some partition.
            ignite.cachex(CACHE_NAME).context().topology().localPartitions().get(0).dataStore()
                .clear(ignite.cachex(CACHE_NAME).context().cacheId());
        }

        // Trigger exchange.
        startGrid(3);

        awaitPartitionMapExchange();

        // Nothing should happen (just log error message) and we're still able to put data to corrupted cache.
        ignite.cache(CACHE_NAME).put(0, 0);

        if (cnt && !size)
            assertTrue("Counters inconsistent message not found", lsnrCnt.check());

        if (!cnt && size)
            assertTrue("Size inconsistent message not found", lsnrSize.check());

        if (cnt && size)
            assertTrue("Both counters and sizes message not found", lsnrSizeCnt.check());

        if (!cnt && !size)
            assertFalse("Counters and Size inconsistent message found!", lsnrSize.check() && lsnrCnt.check()
                && lsnrSizeCnt.check());
    }

    /**
     *  1. Only partition counters are inconsistent.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testValidationfPartitionCountersInconsistent() throws Exception {
        startThreeNodesGrid(true, false);
    }

    /**
     *  2. Only partition size are inconsistent.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testValidationfPartitionSizeInconsistent() throws Exception {
        startThreeNodesGrid(false, true);
    }

    /**
     *  3. Partition counters and size are inconsistent.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testValidationBothPartitionSizesAndCountersAreInconsistent() throws Exception {
        startThreeNodesGrid(true, true);
    }

    /**
     *  4. No one partition counters and size are inconsistent.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testValidationBothPatririonSixeAndCountersAreConsistent() throws Exception {
        startThreeNodesGrid(false, false);
    }

    /**
     *  Overraided class LogListener for find specific patterns.
     * @throws Exception If failed.
     */
    private static class SizeCounterLogListener extends LogListener {

        /** {@inheritDoc} */
        @Override public boolean check() {

            return false;
        }

        /** {@inheritDoc} */
        @Override public void reset() {

        }

        // Search specific string pattern and add value of counters and sizes to arrays/
        /** {@inheritDoc} */
        @Override public void accept(String s) {
            HashSet<Long> setCnt = new HashSet<>();
            HashSet<Long> setSize = new HashSet<>();

            if (s.matches("Partitions update counters are inconsistent for Part 0: ")) {
                Pattern p = Pattern.compile("(.)=(..)");
                Matcher m = p.matcher(s);
                while (m.find()) {
                    setCnt.add(Long.parseLong(m.group(2)));
                }

                if (setCnt.size()==2 && setCnt.contains(32) && setCnt.contains(99)){
                    assertTrue("Counters inconsistent message found", true);
                }
            }

            else if (s.matches("Partitions cache sizes are inconsistent for Part 0: ")) {
                Pattern p = Pattern.compile("(.)=(..)");
                Matcher m = p.matcher(s);
                while (m.find()) {
                    setSize.add(Long.parseLong(m.group(2)));
                }

                if (setSize.size()==2 && setSize.contains(0) && setSize.contains(32)){
                    assertTrue("Size inconsistent message found", true);
                }
            }

            else if (s.matches("Partitions cache size and update counters are inconsistent for Part 0:")) {
                Pattern p = Pattern.compile("consistentId=dht.GridCachePartitionsUpdateCountersAndSizeTest(.) meta=\\[updCnt=(..),size=(..)");
                Matcher m = p.matcher(s);
                while (m.find()) {
                    setCnt.add(Long.parseLong(m.group(2)));
                    setSize.add(Long.parseLong(m.group(3)));
                }

                if (setCnt.size()==2 && setCnt.contains(32) && setCnt.contains(99) &&
                    setSize.size()==2 && setSize.contains(0) && setSize.contains(32) ){
                    assertTrue("Size inconsistent message found", true);
                }
            }
        }
    }
}
