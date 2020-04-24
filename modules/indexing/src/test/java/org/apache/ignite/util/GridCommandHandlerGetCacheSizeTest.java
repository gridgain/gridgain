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
    @Test
    public void testValidateGridCommandHandlerGetCacheSizeTest() throws Exception {
        checkpointFreq = 100L;

        Ignite ignite = prepareGridForTest();

        AtomicBoolean stopFlag = new AtomicBoolean();

        IgniteCache<Integer, GridCommandHandlerIndexingUtils.Person> cache = ignite.cache(CACHE_NAME);

        Thread loadThread = new Thread(() -> {
            ThreadLocalRandom rnd = ThreadLocalRandom.current();

            while (!stopFlag.get()) {
                int id = rnd.nextInt();

                cache.put(id, new GridCommandHandlerIndexingUtils.Person(id, "name" + id));

                if (Thread.interrupted())
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
        assertContains(log, out, "offHeapCnt");
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
