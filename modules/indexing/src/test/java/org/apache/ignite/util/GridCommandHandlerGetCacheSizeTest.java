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
import org.junit.Test;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_OK;
import static org.apache.ignite.testframework.GridTestUtils.assertContains;
import static org.apache.ignite.util.GridCommandHandlerIndexingUtils.*;

public class GridCommandHandlerGetCacheSizeTest extends GridCommandHandlerClusterPerMethodAbstractTest {

    /** */
    public static final int GRID_CNT = 2;

    /** */
    public static final int MAX_CACHE_SIZE = 1024;

    /** */
    @Test
    public void testValidateGridCommandHandlerGetCacheSizeTest() throws Exception {
        checkpointFreq = 100L;

        Ignite ignite = prepareGridForTest();

        AtomicBoolean stopFlag = new AtomicBoolean();

        IgniteCache<Integer, GridCommandHandlerIndexingUtils.Person> cache = ignite.cache(CACHE_NAME);

        Thread loadThread = new Thread(() -> {
            ThreadLocalRandom rnd = ThreadLocalRandom.current();

            while (!stopFlag.get()) {
                int id = rnd.nextInt(MAX_CACHE_SIZE);

                cache.put(id, new GridCommandHandlerIndexingUtils.Person(id, "name" + id));

                if(Thread.interrupted())
                    break;
            }
        });

        try {
            loadThread.start();

            doSleep(checkpointFreq);

            injectTestSystemOut();

            assertEquals(EXIT_CODE_OK, execute("--cache", "list", "."));
        }
        finally {
            stopFlag.set(true);

            loadThread.join();
        }

        String out = testOut.toString();

        assertContains(log, out, "heapCnt");
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



}
