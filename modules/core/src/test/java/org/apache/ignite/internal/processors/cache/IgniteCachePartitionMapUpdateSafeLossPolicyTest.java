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

package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.Ignite;
import org.apache.ignite.cache.PartitionLossPolicy;
import org.apache.ignite.cluster.ClusterTopologyException;
import org.apache.ignite.internal.util.typedef.G;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Stream;

/**
 *
 */
public class IgniteCachePartitionMapUpdateSafeLossPolicyTest extends IgniteCachePartitionMapUpdateTest {
    /** {@inheritDoc} */
    @Override protected PartitionLossPolicy policy() {
        return PartitionLossPolicy.READ_WRITE_SAFE;
    }

    /** Lost partitions counter. */
    private AtomicInteger lostCnt = new AtomicInteger();

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        lostCnt.set(0);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        assertTrue(lostCnt.get() > 0);
    }

    /** {@inheritDoc} */
    @Override protected void stopGrid(int idx) {
        super.stopGrid(idx);

        List<Ignite> grids = G.allGrids();

        if (grids.isEmpty())
            return;

        Stream.of(CACHE1, CACHE2).forEach(new Consumer<String>() {
            @Override public void accept(String cache) {
                lostCnt.addAndGet(grids.get(0).cache(cache).lostPartitions().size());

                try {
                    grids.get(0).resetLostPartitions(Collections.singleton(cache));
                } catch (ClusterTopologyException ignored) {
                    // Expected.
                }
            }
        });
    }
}
