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

package org.apache.ignite.internal.processors.cache.eviction.paged;

import java.util.Random;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataPageEvictionMode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.persistence.tree.reuse.ReuseList;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Ignore;
import org.junit.Test;

/**
 *
 */
public class PageEvictionPagesRecyclingAndReusingTest extends PageEvictionAbstractTest {
    /** Number of small entries. */
    private static final int SMALL_ENTRIES = ENTRIES * GridTestUtils.SF.applyLB(10, 5);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        return setEvictionMode(DataPageEvictionMode.RANDOM_LRU, super.getConfiguration(gridName));
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPagesRecyclingAndReusingAtomicReplicated() throws Exception {
        testPagesRecyclingAndReusing(CacheAtomicityMode.ATOMIC, CacheMode.REPLICATED);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPagesRecyclingAndReusingAtomicLocal() throws Exception {
        testPagesRecyclingAndReusing(CacheAtomicityMode.ATOMIC, CacheMode.LOCAL);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPagesRecyclingAndReusingTxReplicated() throws Exception {
        testPagesRecyclingAndReusing(CacheAtomicityMode.TRANSACTIONAL, CacheMode.REPLICATED);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPagesRecyclingAndReusingTxLocal() throws Exception {
        testPagesRecyclingAndReusing(CacheAtomicityMode.TRANSACTIONAL, CacheMode.LOCAL);
    }

    /**
     * @throws Exception If failed.
     */
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-10738")
    @Test
    public void testPagesRecyclingAndReusingMvccTxPartitioned() throws Exception {
        testPagesRecyclingAndReusing(CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT, CacheMode.PARTITIONED);
    }


    /**
     * @throws Exception If failed.
     */
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-10738")
    @Test
    public void testPagesRecyclingAndReusingMvccTxReplicated() throws Exception {
        testPagesRecyclingAndReusing(CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT, CacheMode.REPLICATED);
    }

    /**
     * @throws Exception If failed.
     */
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-7956,https://issues.apache.org/jira/browse/IGNITE-9530")
    @Test
    public void testPagesRecyclingAndReusingMvccTxLocal() throws Exception {
        testPagesRecyclingAndReusing(CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT, CacheMode.LOCAL);
    }

    /**
     * @param atomicityMode Atomicity mode.
     * @param cacheMode Cache mode.
     */
    private void testPagesRecyclingAndReusing(CacheAtomicityMode atomicityMode, CacheMode cacheMode) throws Exception {
        IgniteEx ignite = startGrid(0);

        CacheConfiguration<Object, Object> cfg = cacheConfig("evict-fair", null, cacheMode, atomicityMode,
            CacheWriteSynchronizationMode.PRIMARY_SYNC);

        IgniteCache<Object, Object> cache = ignite(0).getOrCreateCache(cfg);

        ReuseList reuseList = ignite.context().cache().context().database().reuseList(null);

        putRemoveCycles(cache, reuseList);

        long recycledPagesCnt1 = reuseList.recycledPagesCount();

        putRemoveCycles(cache, reuseList);

        long recycledPagesCnt2 = reuseList.recycledPagesCount();

        assert recycledPagesCnt1 == recycledPagesCnt2 : "Possible recycled pages leak!";
    }

    /**
     * @param cache Cache.
     * @param reuseList Reuse list.
     */
    private void putRemoveCycles(IgniteCache<Object, Object> cache, ReuseList reuseList) throws IgniteCheckedException {
        for (int i = 1; i <= ENTRIES; i++) {
            cache.put(i, new TestObject(PAGE_SIZE / 4 - 50));

            if (i % (ENTRIES / 10) == 0)
                System.out.println(">>> Entries put: " + i);
        }

        System.out.println("### Recycled pages count: " + reuseList.recycledPagesCount());

        for (int i = 1; i <= ENTRIES; i++) {
            cache.remove(i);

            if (i % (ENTRIES / 10) == 0)
                System.out.println(">>> Entries removed: " + i);
        }

        clearTombstones(cache);

        System.out.println("### Recycled pages count: " + reuseList.recycledPagesCount());

        Random rnd = new Random();

        for (int i = 1; i <= SMALL_ENTRIES; i++) {
            cache.put(i, rnd.nextInt());

            if (i % (SMALL_ENTRIES / 10) == 0)
                System.out.println(">>> Small entries put: " + i);
        }

        System.out.println("### Recycled pages count: " + reuseList.recycledPagesCount());

        for (int i = 1; i <= SMALL_ENTRIES; i++) {
            cache.remove(i);

            if (i % (SMALL_ENTRIES / 10) == 0)
                System.out.println(">>> Small entries removed: " + i);
        }

        clearTombstones(cache);

        System.out.println("### Recycled pages count: " + reuseList.recycledPagesCount());
    }
}
