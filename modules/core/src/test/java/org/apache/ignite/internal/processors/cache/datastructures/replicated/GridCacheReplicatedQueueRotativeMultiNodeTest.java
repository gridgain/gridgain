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

package org.apache.ignite.internal.processors.cache.datastructures.replicated;

import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.internal.processors.cache.datastructures.GridCacheQueueRotativeMultiNodeAbstractTest;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.REPLICATED;

/**
 * Queue rotative multi node tests.
 */
public class GridCacheReplicatedQueueRotativeMultiNodeTest extends GridCacheQueueRotativeMultiNodeAbstractTest {
    /** {@inheritDoc} */
    @Override protected CacheMode collectionCacheMode() {
        return REPLICATED;
    }

    /** {@inheritDoc} */
    @Override protected CacheAtomicityMode collectionCacheAtomicityMode() {
        return TRANSACTIONAL;
    }
}