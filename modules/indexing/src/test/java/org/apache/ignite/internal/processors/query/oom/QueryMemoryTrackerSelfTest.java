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

package org.apache.ignite.internal.processors.query.oom;

import org.junit.Ignore;
import org.junit.Test;

/**
 * Query memory manager test for distributed queries.
 */
public class QueryMemoryTrackerSelfTest extends AbstractQueryMemoryTrackerSelfTest {
    /** {@inheritDoc} */
    @Override protected boolean isLocal() {
        return false;
    }

    /** Check simple query on large data set with small limit. */
    @Test
    @Override public void testQueryWithLimit() {
        execQuery("select * from K LIMIT 500", false);

        assertEquals(2, localResults.size());
        assertEquals(500, localResults.get(0).getRowCount());
        assertEquals(500, localResults.get(1).getRowCount());
    }

    /** {@inheritDoc} */
    @Ignore("https://ignite.apache.org/browse/IGNITE-9933")
    @Test
    @Override public void testLazyQueryWithHighLimit() {
        super.testLazyQueryWithHighLimit();
    }

    /** {@inheritDoc} */
    @Ignore("https://ignite.apache.org/browse/IGNITE-9933")
    @Test
    @Override public void testLazyQueryWithSortByIndexedCol() {
        // TODO: IGNITE-9933: OOM on reducer.
        execQuery("select * from K ORDER BY K.indexed", true);
        assertEquals(1, localResults.size());
    }

    /** Check GROUP BY operation on large data set with small result set. */
    @Test
    @Override public void testQueryWithGroupsSmallResult() {
        execQuery("select K.grp, avg(K.id), min(K.id), sum(K.id) from K GROUP BY K.grp", false); // Tiny local result.
        assertEquals(2, localResults.size());
        localResults.clear();

        execQuery("select K.indexed, sum(K.id) from K GROUP BY K.indexed", false); // Sorted grouping.
        assertEquals(2, localResults.size());
        localResults.clear();

        execQuery("select K.grp_indexed, sum(K.id) as s from K GROUP BY K.grp_indexed ORDER BY s", false); // Tiny local result with sort.
        assertEquals(2, localResults.size());
    }
}