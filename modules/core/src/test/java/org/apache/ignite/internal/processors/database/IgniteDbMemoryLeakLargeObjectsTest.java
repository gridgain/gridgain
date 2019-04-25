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

package org.apache.ignite.internal.processors.database;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.internal.IgniteEx;

/**
 *
 */
public class IgniteDbMemoryLeakLargeObjectsTest extends IgniteDbMemoryLeakAbstractTest {
    /** */
    private static final int[] ARRAY;

    static {
        ARRAY = new int[1024];

        for (int i = 0; i < ARRAY.length; i++)
            ARRAY[i] = nextInt();
    }

    /** {@inheritDoc} */
    @Override protected IgniteCache<Object, Object> cache(IgniteEx ig) {
        return ig.cache("large");
    }

    /** {@inheritDoc} */
    @Override protected Object key() {
        return new LargeDbKey(nextInt(10_000), 1024);
    }

    /** {@inheritDoc} */
    @Override protected Object value(Object key) {
        return new LargeDbValue("test-value-1-" + nextInt(200), "test-value-2-" + nextInt(200), ARRAY);
    }

    /** {@inheritDoc} */
    @Override protected long pagesMax() {
        return 35_000;
    }
}
