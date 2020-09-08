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
package org.apache.ignite.internal.processors.query.oom;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.processors.cache.query.SqlFieldsQueryEx;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

import static java.nio.file.StandardWatchEventKinds.ENTRY_CREATE;
import static java.nio.file.StandardWatchEventKinds.ENTRY_DELETE;

/**
 * Disk spilling test for some baseline topology scenarios.
 */
public class DiskSpillingWithBaselineTest extends DiskSpillingAbstractTest {
    /** {@inheritDoc} */
    @Override protected boolean persistence() {
        return true;
    }

    /** {@inheritDoc} */
    @Override protected int nodeCount() {
        return 3;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        // No-op. Test environment wil be set up in the @beforeTest method.
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();
        initGrid();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        destroyGrid();
    }

    /**
     * Test 1 (cluster deactivation)
     *
     * start 3 server node grid
     * create tables (partitioned cache), populate with data
     * run several queries and get query cursors asynchronous. Then:
     * ensure disk offloading happened on all nodes
     * deactivate cluster
     * Check all offload files was deleted
     */
    @Test
    public void testSpillFilesDeletedOnConcurrentDeactivation() {
        // Check baseline set and cluster is activated.
        assertEquals(nodeCount(), grid(0).cluster().forServers().nodes().size());
        assertTrue(grid(0).cluster().active());
        assertEquals(grid(0).cluster().currentBaselineTopology().size(), nodeCount());

        FieldsQueryCursor<List<?>> cur1 = getSpillableCursor("SELECT DISTINCT depId, code FROM person ORDER BY code");
        FieldsQueryCursor<List<?>> cur2 = getSpillableCursor("SELECT depId, age, COUNT(nulls), AVG(nulls), " +
            "LISTAGG(nulls) FROM person GROUP BY age, depId");

        // Check offloading happened.
        checkSpillFilesCreated(cur1);
        checkSpillFilesCreated(cur2);

        // Deactivate cluster.
        grid(0).cluster().active(false);

        // Check spill files deleted on close.
        checkSpillFilesDeletedOnClose(cur1);
        checkSpillFilesDeletedOnClose(cur2);

        assertWorkDirClean();
    }

    /**
     * Test 2 (node stop, PME)
     *
     * start 3 server node grid
     * create tables (partitioned cache), populate with data
     * run several queries and get query cursors asynchronous. During this:
     * ensure disk offloading happened on all nodes
     * stop one of nodes
     * set new baseline
     * await PME
     * Check all offload files was deleted when cursors were closed
     * Check results of query the same as without baseline change
     */
    @Test
    public void testSpillFilesDeletedOnNewBaselineNodeExit() throws InterruptedException {
        // Check baseline set and cluster is activated.
        assertEquals(nodeCount(), grid(0).cluster().forServers().nodes().size());
        assertTrue(grid(0).cluster().active());
        assertEquals(grid(0).cluster().currentBaselineTopology().size(), nodeCount());

        FieldsQueryCursor<List<?>> cur1 = getSpillableCursor("SELECT DISTINCT depId, code FROM person ORDER BY code");
        FieldsQueryCursor<List<?>> cur2 = getSpillableCursor("SELECT depId, age, COUNT(nulls), AVG(nulls), " +
            "LISTAGG(nulls) FROM person GROUP BY age, depId");

        // Check offloading happened.
        checkSpillFilesCreated(cur1);
        checkSpillFilesCreated(cur2);

        // Stop node.
        stopGrid(nodeCount() - 1);

        // Set new baseline topology.
        Collection<ClusterNode> aliveNodes = grid(0).cluster().forServers().nodes();

        grid(0).cluster().setBaselineTopology(aliveNodes);
        grid(0).resetLostPartitions(Collections.singleton(DEFAULT_CACHE_NAME));

        awaitPartitionMapExchange();

        // Check spill files deleted on close.
        checkSpillFilesDeletedOnClose(cur1);
        checkSpillFilesDeletedOnClose(cur2);

        assertWorkDirClean();
    }

    /**
     * Test 3 (node start, PME)
     *
     * start 3 server node grid
     * create tables (partitioned cache), populate with data
     * run several queries and get query cursors asynchronous. During this:
     * ensure disk offloading happened on all nodes
     * start new node
     * set new baseline
     * await PME
     * ensure disk offloading happened on new node // This will not happen because query is mapped on the old nodes only.
     * Check all offload files was deleted when cursors were closed
     * Check results of query the same as without baseline change
     */
    @Test
    public void testSpillFilesDeletedOnNewBaselineNodeEnter() throws Exception {
        // Check baseline set and cluster is activated.
        assertEquals(nodeCount(), grid(0).cluster().forServers().nodes().size());
        assertTrue(grid(0).cluster().active());
        assertEquals(grid(0).cluster().currentBaselineTopology().size(), nodeCount());

        FieldsQueryCursor<List<?>> cur1 = getSpillableCursor("SELECT DISTINCT depId, code FROM person ORDER BY code");
        FieldsQueryCursor<List<?>> cur2 = getSpillableCursor("SELECT depId, age, COUNT(nulls), AVG(nulls), " +
            "LISTAGG(nulls) FROM person GROUP BY age, depId");

        // Check offloading happened.
        checkSpillFilesCreated(cur1);
        checkSpillFilesCreated(cur2);

        // Start new node.
        startGrid(nodeCount());

        // Set new baseline topology.
        Collection<ClusterNode> aliveNodes = grid(0).cluster().forServers().nodes();

        grid(0).cluster().setBaselineTopology(aliveNodes);

        awaitPartitionMapExchange();

        // Check spill files deleted on close.
        checkSpillFilesDeletedOnClose(cur1);
        checkSpillFilesDeletedOnClose(cur2);

        assertWorkDirClean();
    }

    /**
     * Test 4 (cache destroy)
     *
     * start 2 server node grid
     * create tables (partitioned cache), populate with data
     * run query and get query cursor asynchronous. During this:
     * ensure disk offloading happened on all nodes
     * destroy cache
     * Check all offload files were deleted
     */
    @Test
    public void testSpillFilesDeletedOnCacheDestroy() throws Exception {
        // Check baseline set and cluster is activated.
        assertEquals(nodeCount(), grid(0).cluster().forServers().nodes().size());
        assertTrue(grid(0).cluster().active());
        assertEquals(grid(0).cluster().currentBaselineTopology().size(), nodeCount());

        FieldsQueryCursor<List<?>> cur1 = getSpillableCursor("SELECT DISTINCT depId, code FROM person ORDER BY code");
        FieldsQueryCursor<List<?>> cur2 = getSpillableCursor("SELECT depId, age, COUNT(nulls), AVG(nulls), " +
            "LISTAGG(nulls) FROM person GROUP BY age, depId");

        // Check offloading happened.
        checkSpillFilesCreated(cur1);
        checkSpillFilesCreated(cur2);

        // Destroy all SQL caches.
        for (String cacheName : grid(0).cacheNames()) {
            if (cacheName.contains("SQL"))
                grid(0).cache(cacheName).destroy();
        }

        // Check spill files deleted on close.
        checkSpillFilesDeletedOnClose(cur1);
        checkSpillFilesDeletedOnClose(cur2);

        assertWorkDirClean();
    }

    /**
     * @param qry Query string.
     * @return Query cursor.
     */
    private FieldsQueryCursor<List<?>> getSpillableCursor(String qry) {
        return grid(0).cache(DEFAULT_CACHE_NAME)
            .query(new SqlFieldsQueryEx(qry, null)
                .setMaxMemory(SMALL_MEM_LIMIT)
                .setLazy(true));
    }

    /**
     * @param cur Query cursor.
     */
    private void checkSpillFilesCreated(FieldsQueryCursor<List<?>> cur) {
        WatchService watchSvc = null;

        try {
            Path workDir = getWorkDir();

            watchSvc = FileSystems.getDefault().newWatchService();

            WatchKey watchKey = workDir.register(watchSvc, ENTRY_CREATE);

            watchKey.reset();

            // Spill file should be created here.
            Iterator<List<?>> it = cur.iterator();

            // Check there are at least more than two rows in result set.
            assertTrue(it.hasNext());
            assertNotNull(it.next());
            assertTrue(it.hasNext());

            // Check files were created on all nodes.
            List<WatchEvent<?>> dirEvts = watchKey.pollEvents();

            assertFalse("Disk events is empty. ", dirEvts.isEmpty());

            List<String> nodeUuids = getNodesFromEvents(dirEvts);

            assertEquals("Offload didn't happen on all nodes.", nodeCount(), nodeUuids.size());
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
        finally {
            U.closeQuiet(watchSvc);
        }
    }

    /**
     * Retrieves node names from the list of file system events.
     *
     * @param evts File system events
     * @return Node names.
     */
    private List<String> getNodesFromEvents(List<WatchEvent<?>> evts) {
        //
        return evts.stream()
            .map(e -> e.context().toString().split("_")[1])
            .distinct()
            .collect(Collectors.toList());
    }

    /**
     * @param cur Query cursor.
     */
    private void checkSpillFilesDeletedOnClose(FieldsQueryCursor<List<?>> cur) {
        WatchService watchSvc = null;

        try {
            Path workDir = getWorkDir();

            watchSvc = FileSystems.getDefault().newWatchService();

            WatchKey watchKey = workDir.register(watchSvc, ENTRY_DELETE);

            watchKey.reset();

            // Spill file should be deleted here.
            cur.close();

            // It looks like ENTRY_DELETE event is not arrived instantly. So, let's wait for it a bit.
            GridTestUtils.waitForCondition(() -> !watchKey.pollEvents().isEmpty(), 1000);
        }
        catch (IOException | IgniteInterruptedCheckedException e) {
            throw new RuntimeException(e);
        }
        finally {
            U.closeQuiet(watchSvc);
        }
    }
}
