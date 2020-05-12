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

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_OK;
import static org.apache.ignite.testframework.GridTestUtils.assertContains;
import static org.apache.ignite.util.GridCommandHandlerIndexingUtils.CACHE_NAME;
import static org.apache.ignite.util.GridCommandHandlerIndexingUtils.createAndFillCache;
import static org.apache.ignite.util.GridCommandHandlerIndexingUtils.GROUP_NAME;

/**
 *
 */
public class GridCommandHandlerGetCacheSizeTest extends GridCommandHandlerClusterPerMethodAbstractTest {
    /** */
    public static final int GRID_CNT = 2;

    /** */
    public static final int MAX_CACHE_SIZE = 96 * 1024;

    /** */
    @Test
    public void testValidateGridCommandHandlerGetCacheSizeTest() throws Exception {
        IgniteCache<Integer, GridCommandHandlerIndexingUtils.Person> filledCache = null;

        IgniteInternalFuture<IgniteCache<Integer, GridCommandHandlerIndexingUtils.Person>> c = GridTestUtils.runAsync(this::fillCache);

        filledCache = c.get();

        injectTestSystemOut();

        final long testSize = filledCache.size(CachePeekMode.OFFHEAP);

        assertEquals(EXIT_CODE_OK, execute("--cache", "list", "."));

        String out = testOut.toString();

        assertContains(log, out, "offHeapCnt=" + testSize);
    }

    /**
     * Fill cache with random data
     * @return IgniteCache
     */
    private IgniteCache<Integer, GridCommandHandlerIndexingUtils.Person> fillCache() throws Exception {
        Ignite ignite = prepareGridForTest();

        IgniteCache<Integer, GridCommandHandlerIndexingUtils.Person> cache = ignite.cache(CACHE_NAME);

        AtomicInteger maxCacheSizeCntr = new AtomicInteger();

        ThreadLocalRandom rnd = ThreadLocalRandom.current();

        while(maxCacheSizeCntr.getAndIncrement() < MAX_CACHE_SIZE) {
            int id = rnd.nextInt(MAX_CACHE_SIZE);
            cache.put(id, new GridCommandHandlerIndexingUtils.Person(id, "name" + id));
        }

        return cache;
    }

    /**
     * Create and fill nodes.
     *
     * @throws Exception
     */
    private Ignite prepareGridForTest() throws Exception{
        Ignite ignite = startGrids(GRID_CNT);

        ignite.cluster().active(true);

        Ignite client = startGrid(CLIENT_NODE_NAME_PREFIX);

        createAndFillCache(client, CACHE_NAME, GROUP_NAME);

        return ignite;
    }

    /**
     * Stop all grids and clean heap memory
     *
     * @throws Exception
     */
    @Override protected void afterTest() throws Exception {
        super.afterTest();
        stopAllGrids();
        cleanPersistenceDir();
    }

}
