/*
 * Copyright 2024 GridGain Systems, Inc. and Contributors.
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

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.lang.IgniteUuid;
import org.junit.Test;

import static java.util.Arrays.asList;
import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_INVALID_ARGUMENTS;
import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_OK;
import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_UNEXPECTED_ERROR;
import static org.apache.ignite.util.KillCommandsTests.PAGES_CNT;
import static org.apache.ignite.util.KillCommandsTests.PAGE_SZ;
import static org.apache.ignite.util.KillCommandsTests.doTestCancelClientConnection;
import static org.apache.ignite.util.KillCommandsTests.doTestCancelComputeTask;
import static org.apache.ignite.util.KillCommandsTests.doTestCancelContinuousQuery;
import static org.apache.ignite.util.KillCommandsTests.doTestCancelSQLQuery;
import static org.apache.ignite.util.KillCommandsTests.doTestCancelService;
import static org.apache.ignite.util.KillCommandsTests.doTestCancelTx;
import static org.apache.ignite.util.KillCommandsTests.doTestScanQueryCancel;

/** Tests cancel of user created entities via control.sh. */
public class KillCommandsControlShTest extends GridCommandHandlerClusterByClassAbstractTest {
    /**  */
    private static List<IgniteEx> srvs;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        srvs = new ArrayList<>();

        for (int i = 0; i < SERVER_NODE_CNT; i++)
            srvs.add(grid(i));

        IgniteCache<Object, Object> cache = client.getOrCreateCache(
            new CacheConfiguration<>(DEFAULT_CACHE_NAME).setIndexedTypes(Integer.class, Integer.class)
                .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL));

        // There must be enough cache entries to keep scan query cursor opened.
        // Cursor may be concurrently closed when all the data retrieved.
        for (int i = 0; i < PAGES_CNT * PAGE_SZ; i++)
            cache.put(i, i);

        awaitPartitionMapExchange();

        // We change to reduce the waiting time for interrupting compute job.
        computeJobWorkerInterruptTimeout(srvs.get(0)).propagate(100L);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        // No-op. Prevent cache destroy from super class.
    }

    /** @throws Exception If failed. */
    @Test
    public void testCancelComputeTask() throws Exception {
        doTestCancelComputeTask(client, srvs, sessId -> {
            int res = execute("--kill", "compute", sessId);

            assertEquals(EXIT_CODE_OK, res);
        });
    }

    /** */
    @Test
    public void testCancelTx() {
        doTestCancelTx(client, srvs, xid -> {
            int res = execute("--kill", "transaction", xid);

            assertEquals(EXIT_CODE_OK, res);
        });
    }

    /** @throws Exception If failed. */
    @Test
    public void testCancelService() throws Exception {
        doTestCancelService(client, client, srvs.get(0), name -> {
            int res = execute("--kill", "service", name);

            assertEquals(EXIT_CODE_OK, res);
        });
    }

    /**  */
    @Test
    public void testCancelSQLQuery() {
        doTestCancelSQLQuery(client, qryId -> {
            int res = execute("--kill", "sql", qryId);

            assertEquals(EXIT_CODE_OK, res);
        });
    }

    /** @throws Exception If failed. */
    @Test
    public void testCancelContinuousQuery() throws Exception {
        doTestCancelContinuousQuery(client, srvs, (nodeId, routineId) -> {
            int res = execute("--kill", "continuous", nodeId.toString(), routineId.toString());

            assertEquals(EXIT_CODE_OK, res);
        });
    }

    /** */
    @Test
    public void testCancelScanQuery() {
        doTestScanQueryCancel(client, srvs, args -> {
            int res = execute("--kill", "scan", args.get1().toString(), args.get2(), args.get3().toString());

            assertEquals(EXIT_CODE_OK, res);
        });
    }

    /** @throws Exception If failed. */
    @Test
    public void testCancelClientConnection() {
        doTestCancelClientConnection(srvs, (nodeId, connId) -> {
            List<String> params = new ArrayList<>(
                asList("--kill", "client", connId == null ? "ALL" : Long.toString(connId))
            );

            if (nodeId != null)
                params.addAll(asList("--node-id", nodeId.toString()));

            assertEquals(EXIT_CODE_OK, execute(params));
        });
    }

    /**  */
    @Test
    public void testCancelUnknownSQLQuery() {
        int res = execute("--kill", "sql", srvs.get(0).localNode().id().toString() + "_42");

        assertEquals(EXIT_CODE_OK, res);
    }

    /** */
    @Test
    public void testCancelUnknownContinuousQuery() {
        int res = execute("--kill", "continuous", srvs.get(0).localNode().id().toString(),
            UUID.randomUUID().toString());

        assertEquals(EXIT_CODE_OK, res);
    }

    /** */
    @Test
    public void testCancelUnknownScanQuery() {
        int res = execute("--kill", "scan", srvs.get(0).localNode().id().toString(), "unknown", "1");

        assertEquals(EXIT_CODE_OK, res);
    }

    /** @throws Exception If failed. */
    @Test
    public void testCancelClientConnectionWrongParams() {
        assertEquals(EXIT_CODE_INVALID_ARGUMENTS, execute("--kill", "client"));
        assertEquals(EXIT_CODE_INVALID_ARGUMENTS, execute("--kill", "client", "not_a_number"));
        assertEquals(EXIT_CODE_INVALID_ARGUMENTS, execute("--kill", "client", "1", "--node-id"));
        assertEquals(EXIT_CODE_INVALID_ARGUMENTS, execute("--kill", "client", "1", "--node-id", "not_an_uuid"));
        assertEquals("Unknown connection id", EXIT_CODE_UNEXPECTED_ERROR, execute("--kill", "client", "123"));
    }

    /** */
    @Test
    public void testCancelUnknownComputeTask() {
        int res = execute("--kill", "compute", IgniteUuid.randomUuid().toString());

        assertEquals(EXIT_CODE_OK, res);
    }

    /** */
    @Test
    public void testCancelUnknownService() {
        int res = execute("--kill", "service", "unknown");

        assertEquals(EXIT_CODE_OK, res);
    }

    /** */
    @Test
    public void testCancelUnknownTx() {
        int res = execute("--kill", "transaction", "unknown");

        assertEquals(EXIT_CODE_OK, res);
    }
}
