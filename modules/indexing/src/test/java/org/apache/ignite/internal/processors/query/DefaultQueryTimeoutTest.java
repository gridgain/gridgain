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

package org.apache.ignite.internal.processors.query;

import java.util.concurrent.Callable;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.index.AbstractIndexingCommonTest;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

// t0d0 add to suite
// t0d0 check DML
// t0d0 use different query APIs
public abstract class DefaultQueryTimeoutTest extends AbstractIndexingCommonTest {
    private long defaultQueryTimeout;

    private final boolean updateQuery;

    protected DefaultQueryTimeoutTest() {
        this(false);
    }

    protected DefaultQueryTimeoutTest(boolean updateQuery) {
        this.updateQuery = updateQuery;
    }

    protected void prepareQueryExecution() throws Exception {
    }

    protected abstract void executeQuery(String sql) throws Exception;

    protected abstract void executeQuery(String sql, long timeout) throws Exception;

    protected abstract void assertQueryCancelled(Callable<?> c);

    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setDefaultQueryTimeout(defaultQueryTimeout);
    }

    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    @Test
    public void testNoExplicitTimeout1() throws Exception {
        checkQueryNoExplicitTimeout(1000, 0, false);
    }

    @Test
    public void testNoExplicitTimeout2() throws Exception {
        checkQueryNoExplicitTimeout(1000, 1500, false);
    }

    @Test
    public void testNoExplicitTimeout3() throws Exception {
        checkQueryNoExplicitTimeout(1000, 500, true);
    }

    @Test
    public void testExplicitTimeout1() throws Exception {
        checkQuery(500, 0, 0, false);
    }

    @Test
    public void testExplicitTimeout2() throws Exception {
        checkQuery(500, 1000, 0, false);
    }

    @Test
    public void testExplicitTimeout3() throws Exception {
        checkQuery(2000, 1000, 0, true);
    }

    @Test
    public void testExplicitTimeout4() throws Exception {
        checkQuery(1500, 0, 1000, false);
    }

    @Test
    public void testExplicitTimeout5() throws Exception {
        checkQuery(1000, 1500, 500, false);
    }

    @Test
    public void testExplicitTimeout6() throws Exception {
        checkQuery(2000, 1000, 500, true);
    }

    @Test
    public void testExplicitTimeout7() throws Exception {
        checkQuery(500, 1000, 2000, false);
    }

    @Test
    public void testExplicitTimeout8() throws Exception {
        checkQuery(2000, 1000, 2000, true);
    }

    @Test
    public void testConcurrent() throws Exception {
        defaultQueryTimeout = 1000;

        IgniteEx ign = startGrid(0);

        prepareQueryExecution();

        TimedQueryHelper helper = new TimedQueryHelper(1000, DEFAULT_CACHE_NAME);

        helper.createCache(ign);

        String qryText = updateQuery ? helper.buildTimedUpdateQuery() : helper.buildTimedQuery();

        IgniteInternalFuture<?> fut1 = GridTestUtils.runAsync(() -> {
            executeQuery(qryText, 500);

            return null;
        });

        IgniteInternalFuture<?> fut2 = GridTestUtils.runAsync(() -> {
            executeQuery(qryText, 1500);

            return null;
        });

        assertQueryCancelled((Callable<?>)fut1::get);

        fut2.get(); // assert no exception here
    }

    private void checkQueryNoExplicitTimeout(long execTime, long defaultTimeout, boolean expectCancelled)
        throws Exception {
        checkQuery0(execTime, null, defaultTimeout, expectCancelled);
    }

    private void checkQuery(long execTime, long explicitTimeout, long defaultTimeout, boolean expectCancelled)
        throws Exception {
        checkQuery0(execTime, explicitTimeout, defaultTimeout, expectCancelled);
    }

    private void checkQuery0(long execTime, Long explicitTimeout, long defaultTimeout, boolean expectCancelled)
        throws Exception {
        defaultQueryTimeout = defaultTimeout;

        // t0d0 multiple nodes
        startGrid(0);

        prepareQueryExecution();

        TimedQueryHelper helper = new TimedQueryHelper(execTime, DEFAULT_CACHE_NAME);

        helper.createCache(grid(0));

        Callable<Void> c = () -> {
            String qryText = updateQuery ? helper.buildTimedUpdateQuery() : helper.buildTimedQuery();

            if (explicitTimeout != null)
                executeQuery(qryText, explicitTimeout);
            else
                executeQuery(qryText);

            return null;
        };

        if (expectCancelled)
            assertQueryCancelled(c);
        else
            c.call(); // assert no exception here
    }
}
