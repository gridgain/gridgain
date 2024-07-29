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
import org.junit.Test;

import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_OK;
import static org.apache.ignite.util.KillCommandsTests.PAGE_SZ;
import static org.apache.ignite.util.KillCommandsTests.doTestCancelContinuousQuery;
import static org.apache.ignite.util.KillCommandsTests.doTestCancelSQLQuery;

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

        for (int i = 0; i < PAGE_SZ * PAGE_SZ; i++)
            cache.put(i, i);

        awaitPartitionMapExchange();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        // No-op. Prevent cache destroy from super class.
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

}
