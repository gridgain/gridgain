/*
 * Copyright 2021 GridGain Systems, Inc. and Contributors.
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

import org.junit.Test;

/**
 * Tests that put/putAll/replace to in-memory atomic cache doesn't cause IgniteOutOfMemoryException.
 */
public class IgniteOOMWithoutNodeFailureAtomicTest extends IgniteOOMWithoutNodeFailureAbstractTest {
    /** Tests that put/putAll/replace to in-memory atomic cache doesn't cause IgniteOutOfMemoryException. */
    @Test
    public void testAtomicCache() throws Exception {
        testIgniteOOMWithoutNodeFailure(ignite.cache(DEFAULT_CACHE_NAME), Runnable::run);
    }
}
