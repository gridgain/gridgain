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

package org.apache.ignite.internal.managers.discovery;

import java.util.List;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsExchangeFuture;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Tests for {@link ClusterMetricsImpl}.
 */
public class ClusterMetricsTest extends GridCommonAbstractTest {

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testInitTimeAfterStartTime() throws Exception {
        IgniteEx ig = startGrid(0);
        GridKernalContext ctx = ig.context();
        GridCacheSharedContext cctx = ctx.cache().context();

        List<GridDhtPartitionsExchangeFuture> futs = cctx.exchange().exchangeFutures();
        for (GridDhtPartitionsExchangeFuture fut : futs) {
            assertTrue(0 != fut.getInitTime());
            assertTrue(0 != fut.getStartTime());
            assertTrue(fut.getInitTime() > fut.getStartTime());
        }
    }

}
