/*
 * Copyright 2025 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.events;

import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import java.util.Locale;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.ignite.events.EventType.EVT_QUERY_FINISHED;

/** QueryExecutionFinishedEvent test. */
public class QueryExecutionFinishedEventTest extends GridCommonAbstractTest {
    /** */
    private static final int NODES_CNT = 2;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName).setIncludeEventTypes(EVT_QUERY_FINISHED);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrids(NODES_CNT);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /** */
    @Test
    public void testQueryFinishEvents() throws IgniteInterruptedCheckedException {
        IgniteEx ignite = grid(0);

        String sql = "select 1";

        AtomicInteger eventTriggered = new AtomicInteger();

        ignite.events().localListen(event -> {
            assertTrue(event instanceof QueryExecutionFinishedEvent);
            QueryExecutionFinishedEvent event0 = (QueryExecutionFinishedEvent) event;
            event0.status().query().toUpperCase(Locale.ROOT).equals(sql.toUpperCase(Locale.ROOT));

            eventTriggered.incrementAndGet();
            return true;
        }, EVT_QUERY_FINISHED);

        ignite.events(ignite.cluster().forRemotes()).remoteListen((id, event) -> {
            assertTrue(event instanceof QueryExecutionFinishedEvent);
            QueryExecutionFinishedEvent event0 = (QueryExecutionFinishedEvent) event;
            event0.status().query().toUpperCase(Locale.ROOT).equals(sql.toUpperCase(Locale.ROOT));

            eventTriggered.incrementAndGet();
            return true;
        }, f -> true, EVT_QUERY_FINISHED);

        grid(0).getOrCreateCache(DEFAULT_CACHE_NAME).query(new SqlFieldsQuery("select 1")).getAll();
        grid(1).getOrCreateCache(DEFAULT_CACHE_NAME).query(new SqlFieldsQuery("select 1")).getAll();

        assertTrue(GridTestUtils.waitForCondition(() -> eventTriggered.get() == 2, 5_000));
    }
}
