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

import org.apache.ignite.cache.query.QueryCancelledException;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.index.AbstractIndexingCommonTest;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

// t0d0 add to suite
public class DefaultQueryTimeoutConfigurationTest extends AbstractIndexingCommonTest {
    private long defaultQueryTimeout;

    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setDefaultQueryTimeout(defaultQueryTimeout);
    }

    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    @Test
    public void testDifferentConfigurationValues1() throws Exception {
        defaultQueryTimeout = 500;

        // Currently we expect that all nodes will use a default timeout value from local IgniteConfiguration.
        // Test assertions should be changed when this logic is improved.
        IgniteEx srv0 = startGrid(0);

        defaultQueryTimeout = 2000;

        IgniteEx srv1 = startGrid(1);

        IgniteEx cli = startClientGrid(2);

        awaitPartitionMapExchange();

        TimedQueryHelper helper = new TimedQueryHelper(1000, DEFAULT_CACHE_NAME);

        helper.createCache(grid(0));

        GridTestUtils.assertThrowsWithCause(() -> helper.executeQuery(srv0), QueryCancelledException.class);

        // assert no exception
        helper.executeQuery(srv1);

        // assert no exception
        helper.executeQuery(cli);
    }

    @Test
    public void testDifferentConfigurationValues2() throws Exception {
        defaultQueryTimeout = 2000;

        // Currently we expect that all nodes will use a default timeout value from local IgniteConfiguration.
        // Test assertions should be changed when this logic is improved.
        IgniteEx srv0 = startGrid(0);

        defaultQueryTimeout = 500;

        IgniteEx srv1 = startGrid(1);

        IgniteEx cli = startClientGrid(2);

        awaitPartitionMapExchange();

        TimedQueryHelper helper = new TimedQueryHelper(1000, DEFAULT_CACHE_NAME);

        helper.createCache(grid(0));

        // assert no exception
        helper.executeQuery(srv0);

        GridTestUtils.assertThrowsWithCause(() -> helper.executeQuery(srv1), QueryCancelledException.class);

        GridTestUtils.assertThrowsWithCause(() -> helper.executeQuery(cli), QueryCancelledException.class);
    }

    @Test
    public void testNegativeDefaultTimeout() throws Exception {
        defaultQueryTimeout = -1;

        GridTestUtils.assertThrowsWithCause(() -> startGrid(0), IllegalArgumentException.class);
    }

    @Test
    public void testZeroDefaultTimeout() throws Exception {
        defaultQueryTimeout = 0;

        startGrid(0);

        // assert no exception here
    }

    @Test
    public void testPositiveDefaultTimeout() throws Exception {
        defaultQueryTimeout = 1;

        startGrid(0);

        // assert no exception here
    }

    @Test
    public void testTooBigDefaultTimeout() throws Exception {
        defaultQueryTimeout = Integer.MAX_VALUE + 1L;

        assert defaultQueryTimeout > Integer.MAX_VALUE;

        GridTestUtils.assertThrowsWithCause(() -> startGrid(0), IllegalArgumentException.class);
    }
}
