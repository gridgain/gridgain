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

package org.apache.ignite.internal.util.offheap.unsafe;

import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.internal.util.offheap.GridOffHeapMapFactory;
import org.apache.ignite.internal.util.offheap.GridOffHeapPartitionedMap;
import org.apache.ignite.internal.util.offheap.GridOffHeapPartitionedMapPerformanceAbstractTest;

/**
 * Unsafe partitioned map performance test.
 */
public class GridUnsafePartitionedMapPerformanceTest extends GridOffHeapPartitionedMapPerformanceAbstractTest {
    /** {@inheritDoc} */
    @Override protected GridOffHeapPartitionedMap newMap() {
        return GridOffHeapMapFactory.unsafePartitionedMap(RendezvousAffinityFunction.DFLT_PARTITION_COUNT,
            Runtime.getRuntime().availableProcessors(), load, LOAD_CNT, mem, lruStripes, null);
    }
}