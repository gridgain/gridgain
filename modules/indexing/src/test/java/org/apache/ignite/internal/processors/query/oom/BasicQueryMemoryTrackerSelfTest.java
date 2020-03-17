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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import javax.cache.CacheException;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.internal.processors.query.h2.H2Utils;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Ignore;
import org.junit.Test;

import static org.apache.ignite.internal.util.IgniteUtils.KB;
import static org.apache.ignite.internal.util.IgniteUtils.MB;

/**
 * Query memory manager tests.
 */
public abstract class BasicQueryMemoryTrackerSelfTest extends AbstractQueryMemoryTrackerSelfTest {
    /** Check simple query on small data set. */
    @Test
    public void testSimpleQuerySmallResult() throws Exception {
        execQuery("select * from T", false);

        assertEquals(1, localResults.size());
        assertEquals(SMALL_TABLE_SIZE, localResults.get(0).getRowCount());
    }

    /** Check simple lazy query on large data set. */
    @Test
    public void testLazyQueryLargeResult() throws Exception {
        execQuery("select * from K", true);

        assertEquals(0, localResults.size()); // No local result required.
    }

    /** Check simple query failure on large data set. */
    @Test
    public void testSimpleQueryLargeResult() throws Exception {
        checkQueryExpectOOM("select * from K", false);

        assertEquals(1, localResults.size());
        assertTrue(BIG_TABLE_SIZE > localResults.get(0).getRowCount());
    }

    /** Check simple query on large data set with small limit. */
    @Test
    public void testQueryWithLimit() throws Exception {
        execQuery("select * from K LIMIT 500", false);

        assertEquals(1, localResults.size());
        assertEquals(500, localResults.get(0).getRowCount());
    }

    /** Check lazy query on large data set with large limit. */
    @Test
    public void testLazyQueryWithHighLimit() throws Exception {
        execQuery("select * from K LIMIT 8000", true);

        assertEquals(0, localResults.size()); // No local result required.
    }

    /** Check simple query on large data set with small limit. */
    @Test
    public void testQueryWithHighLimit() {
        checkQueryExpectOOM("select * from K LIMIT 8000", false);

        assertEquals(1, localResults.size());
        assertTrue(maxMem > localResults.get(0).memoryReserved());
        assertTrue(8000 > localResults.get(0).getRowCount());
    }

    /** Check lazy query with ORDER BY indexed col. */
    @Test
    public void testLazyQueryWithSortByIndexedCol() throws Exception {
        execQuery("select * from K ORDER BY K.indexed", true);

        // No local result needed.
        assertEquals(0, localResults.size());
    }

    /** Check query failure with ORDER BY indexed col. */
    @Test
    public void testQueryWithSortByIndexedCol() {
        checkQueryExpectOOM("select * from K ORDER BY K.indexed", false);
        assertEquals(1, localResults.size());
        assertTrue(BIG_TABLE_SIZE > localResults.get(0).getRowCount());
    }

    /** Check lazy query failure with ORDER BY non-indexed col. */
    @Test
    public void testLazyQueryWithSort() {
        checkQueryExpectOOM("select * from K ORDER BY K.grp", true);
        assertEquals(1, localResults.size());
        assertTrue(BIG_TABLE_SIZE > localResults.get(0).getRowCount());
    }

    /** Check query failure with ORDER BY non-indexed col. */
    @Test
    public void testQueryWithSort() {
        // Order by non-indexed field.
        checkQueryExpectOOM("select * from K ORDER BY K.grp", false);
        assertEquals(1, localResults.size());
        assertTrue(BIG_TABLE_SIZE > localResults.get(0).getRowCount());
    }

    /** Check UNION operation with large sub-selects. */
    @Test
    public void testUnionSimple() throws Exception {
        maxMem = 9L * MB;
        assert localResults.isEmpty();

        execQuery("select * from T as T0, T as T1 where T0.id < 3 " +
            "UNION " +
            "select * from T as T2, T as T3 where T2.id > 1 AND T2.id < 4", true);

        assertEquals(3, localResults.size());
        assertTrue(maxMem > localResults.get(1).memoryReserved() + localResults.get(2).memoryReserved());
        assertEquals(3000, localResults.get(1).getRowCount());
        assertEquals(2000, localResults.get(2).getRowCount());
        assertEquals(4000, localResults.get(0).getRowCount());
    }

    /** Check UNION operation with large sub-selects. */
    @Test
    public void testUnionLargeDataSets() {
        // None of sub-selects fits to memory.
        checkQueryExpectOOM("select * from T as T0, T as T1 where T0.id < 4 " +
            "UNION " +
            "select * from T as T2, T as T3 where T2.id >= 2 AND T2.id < 6", true);

        assertEquals(2, localResults.size());
        assertTrue(maxMem > localResults.get(1).memoryReserved());
        assertTrue(4000 > localResults.get(0).getRowCount());
        assertTrue(4000 > localResults.get(1).getRowCount());
    }

    /** Check large UNION operation with small enough sub-selects, but large result set. */
    @Test
    public void testUnionOfSmallDataSetsWithLargeResult() {
        maxMem = 3L * MB;

        checkQueryExpectOOM("select * from T as T0, T as T1 where T0.id < 2 " +
            "UNION " +
            "select * from T as T2, T as T3 where T2.id > 2 AND T2.id < 4", false);

        assertEquals(3, localResults.size());
        assertTrue(maxMem > localResults.get(1).memoryReserved() + localResults.get(2).memoryReserved());
        assertEquals(2000, localResults.get(1).getRowCount());
        assertEquals(1000, localResults.get(2).getRowCount());
        assertTrue(3000 > localResults.get(0).getRowCount());
    }

    /** Check simple Joins. */
    @Test
    public void testSimpleJoins() throws Exception {
        checkQueryExpectOOM("select * from K ORDER BY K.grp", true);
        execQuery("select * from T as T0, T as T1 where T0.id < 2", false);
        execQuery("select * from T as T0, T as T1 where T0.id >= 2 AND T0.id < 4", false);
        execQuery("select * from T as T0, T as T1", true);
    }

    /** Check simple Joins. */
    @Test
    public void testSimpleJoinsHugeResult() {
        // Query with single huge local result.
        checkQueryExpectOOM("select * from T as T0, T as T1", false);

        assertEquals(1, localResults.size());
        assertTrue(maxMem >= localResults.get(0).memoryReserved());

    }

    /** Check simple Joins. */
    @Test
    public void testLazyQueryWithJoinAndSort() {
        // Query with huge local result.
        checkQueryExpectOOM("select * from T as T0, T as T1 ORDER BY T1.id", true);

        assertEquals(1, localResults.size());
        assertTrue(maxMem >= localResults.get(0).memoryReserved());
    }

    /** Check GROUP BY operation on large data set with small result set. */
    @Test
    public void testQueryWithGroupsSmallResult() throws Exception {
        execQuery("select K.grp, avg(K.id), min(K.id), sum(K.id) from K GROUP BY K.grp", false); // Tiny local result.

        assertEquals(1, localResults.size());
        assertEquals(100, localResults.get(0).getRowCount());
    }

    /** Check GROUP BY operation on indexed col. */
    @Test
    public void testQueryWithGroupByIndexedCol() throws Exception {
        execQuery("select K.indexed, sum(K.grp) from K GROUP BY K.indexed", true);

        assertEquals(0, localResults.size());
    }

    /** Check GROUP BY operation on indexed col. */
    @Test
    @Ignore("https://ggsystems.atlassian.net/browse/GG-19071")
    public void testQueryWithGroupByPrimaryKey() throws Exception {
        //TODO: GG-19071: make next query use correct index (K_IDX instead of primary).
        execQuery("select K.indexed, sum(K.id) from K GROUP BY K.indexed", true);

        assertEquals(0, localResults.size());
    }

    /** Check GROUP BY operation on indexed col. */
    @Test
    public void testQueryWithGroupThenSort() throws Exception {
        // Tiny local result with sorting.
        execQuery("select K.grp_indexed, sum(K.id) as s from K GROUP BY K.grp_indexed ORDER BY s", false);

        assertEquals(1, localResults.size());
        assertEquals(100, localResults.get(0).getRowCount());
    }

    /** Check lazy query with GROUP BY non-indexed col failure due to too many groups. */
    @Test
    public void testQueryWithGroupBy() {
        // Too many groups causes OOM.
        checkQueryExpectOOM("select K.name, count(K.id), sum(K.grp) from K GROUP BY K.name", true);

        // Local result is quite small.
        assertEquals(1, localResults.size());
        assertTrue(maxMem > localResults.get(0).memoryReserved() + 1000);
        assertTrue(BIG_TABLE_SIZE > localResults.get(0).getRowCount());
    }

    /** Check query with GROUP BY non-indexed col and with DISTINCT aggregates. */
    @Test
    public void testQueryWithGroupByNonIndexedColAndDistinctAggregates() {
        checkQueryExpectOOM("select K.grp, count(DISTINCT k.name) from K GROUP BY K.grp", true);

        // Local result is quite small.
        assertEquals(1, localResults.size());
        assertTrue(maxMem > localResults.get(0).memoryReserved() + 1000);
        assertTrue(100 > localResults.get(0).getRowCount());
    }

    /** Check lazy query with GROUP BY indexed col and with and DISTINCT aggregates. */
    @Test
    public void testLazyQueryWithGroupByIndexedColAndDistinctAggregates() throws Exception {
        execQuery("select K.grp_indexed, count(DISTINCT k.name) from K  USE INDEX (K_GRP_IDX) GROUP BY K.grp_indexed", true);

        assertEquals(0, localResults.size());
    }

    /** Check lazy query with GROUP BY indexed col (small result), then sort. */
    @Test
    public void testLazyQueryWithGroupByThenSort() throws Exception {
        maxMem = MB / 2;

        checkQueryExpectOOM("select K.indexed, sum(K.grp) as a from K " +
            "GROUP BY K.indexed ORDER BY a DESC", true);

        assertEquals(1, localResults.size());
        assertTrue(maxMem > localResults.get(0).memoryReserved());
        assertTrue(BIG_TABLE_SIZE > localResults.get(0).getRowCount());
    }

    /** Check query with DISTINCT and GROUP BY indexed col (small result). */
    @Test
    public void testQueryWithDistinctAndGroupBy() throws Exception {
        checkQueryExpectOOM("select DISTINCT K.name from K GROUP BY K.id", true);

        // Local result is quite small.
        assertEquals(1, localResults.size());
        assertTrue(maxMem > localResults.get(0).memoryReserved());
        assertTrue(BIG_TABLE_SIZE > localResults.get(0).getRowCount());
    }

    /** Check simple query with DISTINCT constraint. */
    @Test
    public void testQueryWithDistinctAndLowCardinality() throws Exception {
        // Distinct on indexed column with small cardinality.
        execQuery("select DISTINCT K.grp_indexed from K", false);

        assertEquals(1, localResults.size());
        assertEquals(100, localResults.get(0).getRowCount());
    }

    /** Check query failure with DISTINCT constraint. */
    @Test
    public void testQueryWithDistinctAndHighCardinality() throws Exception {
        // Distinct on indexed column with unique values.
        checkQueryExpectOOM("select DISTINCT K.id from K", true);

        assertEquals(1, localResults.size());
        assertTrue(BIG_TABLE_SIZE > localResults.get(0).getRowCount());
    }

    /** Check HashJoin with large table. */
    @Test
    public void testHashJoinWithLargeTable() {
        maxMem = 512 * KB;

        GridTestUtils.setFieldValue(H2Utils.class, "enableHashJoin", true);

        try {
            checkQueryExpectOOM("select * from T, K USE INDEX(HASH_JOIN_IDX) where T.id = K.grp_indexed", true);

            assertEquals(0, localResults.size());
        }
        finally {
            GridTestUtils.setFieldValue(H2Utils.class, "enableHashJoin", false);
        }
    }

    /** Check Join with large table. */
    @Test
    public void testJoinWithLargeTable() throws Exception {
        maxMem = 512 * KB;

        execQuery("select * from T, K where T.id = K.grp_indexed", true);

        assertEquals(0, localResults.size());
    }

    /** Check query failure due to global memory quota exceeded. */
    @Test
    public void testGlobalQuota() throws Exception {
        final List<QueryCursor> cursors = new ArrayList<>();

        IgniteH2Indexing h2 = (IgniteH2Indexing)grid(0).context().query().getIndexing();

        assertEquals(10L * MB, h2.memoryManager().memoryLimit());

        try {
            CacheException ex = (CacheException)GridTestUtils.assertThrows(log, () -> {
                for (int i = 0; i < 100; i++) {
                    QueryCursor<List<?>> cur = query("select T.name, avg(T.id), sum(T.ref_key) from T GROUP BY T.name",
                        true);

                    cursors.add(cur);

                    Iterator<List<?>> iter = cur.iterator();
                    iter.next();
                }

                return null;
            }, CacheException.class, "SQL query run out of memory: Global quota exceeded.");

            if (isLocal())
                assertEquals(18, localResults.size());
            else
                assertEquals(34, localResults.size());

            assertEquals(18, cursors.size());

            long globallyReserved = h2.memoryManager().reserved();

            assertTrue(h2.memoryManager().memoryLimit() < globallyReserved + MB);
        }
        finally {
            for (QueryCursor c : cursors)
                IgniteUtils.closeQuiet(c);
        }
    }
}