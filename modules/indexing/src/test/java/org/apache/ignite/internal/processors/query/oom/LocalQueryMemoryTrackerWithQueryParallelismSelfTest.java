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
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;
import javax.cache.CacheException;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.internal.processors.query.h2.H2ManagedLocalResult;
import org.apache.ignite.internal.processors.query.h2.H2MemoryTracker;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.testframework.GridTestUtils;
import org.gridgain.internal.h2.value.ValueInt;
import org.gridgain.internal.h2.value.ValueString;
import org.hamcrest.CustomMatcher;
import org.hamcrest.Matcher;
import org.junit.Test;

import static java.util.stream.Collectors.summarizingLong;
import static org.apache.ignite.internal.processors.query.h2.H2Utils.rowSizeInBytes;
import static org.apache.ignite.internal.util.IgniteUtils.MB;
import static org.junit.Assert.assertThat;

/**
 * Query memory manager for local queries.
 */
public class LocalQueryMemoryTrackerWithQueryParallelismSelfTest extends BasicQueryMemoryTrackerSelfTest {
    /** Parallelism. */
    private static final int PARALLELISM = 4;

    /** {@inheritDoc} */
    @Override protected boolean isLocal() {
        return true;
    }

    /** {@inheritDoc} */
    @Override protected void createSchema() {
        execSql("create table T (id int primary key, ref_key int, name varchar)" +
            " WITH \"PARALLELISM=" + PARALLELISM + "\"");
        execSql("create table K (id int primary key, indexed int, grp int, grp_indexed int, name varchar)" +
            " WITH \"PARALLELISM=" + PARALLELISM + "\"");
        execSql("create index K_IDX on K(indexed)");
        execSql("create index K_GRP_IDX on K(grp_indexed)");
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testSimpleQuerySmallResult() throws Exception {
        execQuery("select * from T", false);

        long rowCount = localResults.stream().mapToLong(r -> r.getRowCount()).sum();

        assertFalse(localResults.isEmpty());
        assertTrue(localResults.size() <= 4);
        assertEquals(SMALL_TABLE_SIZE, rowCount);
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testQueryWithSort() {
        maxMem = 2 * MB;
        // Order by non-indexed field.
        checkQueryExpectOOM("select * from K ORDER BY K.grp", false);

        assertEquals(4, localResults.size());
        assertTrue(BIG_TABLE_SIZE > localResults.stream().mapToLong(H2ManagedLocalResult::getRowCount).sum());
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testGlobalQuota() throws Exception {
        maxMem = -1L;

        final List<QueryCursor> cursors = new ArrayList<>();

        IgniteH2Indexing h2 = (IgniteH2Indexing)grid(0).context().query().getIndexing();

        assertEquals(globalQuotaSize(), h2.memoryManager().memoryLimit());

        try {
            long rowSize = rowSizeInBytes(new Object[]{ValueString.get(UUID.randomUUID().toString()),
                ValueInt.get(512), ValueInt.get(512)});

            long resSetSize = rowSize * SMALL_TABLE_SIZE;

            // adjust to the size of the reservation block
            // (multiply by 2 because we track splitter buffer size at memory tracker)
            if (resSetSize % RESERVATION_BLOCK_SIZE != 0)
                resSetSize = (resSetSize / RESERVATION_BLOCK_SIZE + 1) * RESERVATION_BLOCK_SIZE * 2;

            int expCursorCnt = (int)(globalQuotaSize() / resSetSize);

            CacheException ex = (CacheException)GridTestUtils.assertThrows(log, () -> {
                for (int i = 0; i < expCursorCnt * 2; i++) {
                    QueryCursor<List<?>> cur = query("select T.name, 512, 512 from T ORDER BY T.name LIMIT 1000000",
                        true);

                    cursors.add(cur);

                    Iterator<List<?>> iter = cur.iterator();
                    iter.next();
                }

                return null;
            }, CacheException.class, "SQL query ran out of memory: Global quota was exceeded.");

            assertEquals(expCursorCnt, cursors.size());

            assertTrue(h2.memoryManager().memoryLimit() < h2.memoryManager().reserved() + MB);
        }
        finally {
            for (QueryCursor c : cursors)
                IgniteUtils.closeQuiet(c);
        }
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testUnionOfSmallDataSetsWithLargeResult() {
        checkQueryExpectOOM("select * from T as T0, T as T1 where T0.id < 2 " +
            "UNION " +
            "select * from T as T2, T as T3 where T2.id > 2 AND T2.id < 4", false);

        assertEquals(11, localResults.size());

        long rowCnt = localResults.stream().mapToLong(H2ManagedLocalResult::getRowCount).sum();

        assertTrue(3000 > rowCnt);

        Map<H2MemoryTracker, Long> collect = localResults.stream().collect(
            Collectors.toMap(H2ManagedLocalResult::memoryTracker, H2ManagedLocalResult::memoryReserved, Long::sum));
        assertTrue(collect.values().stream().allMatch(s -> s < maxMem));
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testLazyQueryWithJoinAndSort() {
        // Query with huge local result.
        checkQueryExpectOOM("select * from T as T0, T as T1 ORDER BY T1.id", true);

        assertFalse(localResults.isEmpty());
        assertTrue(localResults.size() <= 4);
        assertTrue(localResults.stream().allMatch(r -> r.memoryReserved() < maxMem));
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testUnionLargeDataSets() {
        maxMem = 2L * MB;

        // None of sub-selects fits to memory.
        checkQueryExpectOOM("select * from T as T0, T as T1 where T0.id < 4 " +
            "UNION " +
            "select * from T as T2, T as T3 where T2.id >= 2 AND T2.id < 6", true);

        assertEquals(3, localResults.size());
        // Reduce
        assertEquals(0, localResults.get(0).getRowCount());
        // Map
        assertTrue(4000 > localResults.get(1).getRowCount() + localResults.get(2).getRowCount());

    }

    /** {@inheritDoc} */
    @Test
    @Override public void testQueryWithLimit() throws Exception {
        execQuery("select * from K LIMIT 500", false);

        assertEquals(5, localResults.size());
        // Reduce
        assertTrue(localResults.stream().allMatch(r -> r.getRowCount() == 500));
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testQueryWithDistinctAndGroupBy() throws Exception {
        checkQueryExpectOOM("select DISTINCT K.name from K GROUP BY K.id", true);

        // Local result is quite small.
        assertEquals(1, localResults.size());
        assertEquals(0, localResults.get(0).memoryReserved());
        assertEquals(0, localResults.get(0).getRowCount());
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testSimpleJoinsHugeResult() {
        // Query with single huge local result.
        checkQueryExpectOOM("select * from T as T0, T as T1", false);

        assertFalse(localResults.isEmpty());
        assertTrue(localResults.size() <= 4);
        assertTrue(localResults.stream().allMatch(r -> r.memoryReserved() < maxMem));
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testLazyQueryWithSort() {
        maxMem = 2 * MB;

        checkQueryExpectOOM("select * from K ORDER BY K.grp", true);

        assertEquals(4, localResults.size());
        assertFalse(localResults.stream().anyMatch(r -> r.memoryReserved() + 1000 > maxMem));
        // Map
        assertTrue(BIG_TABLE_SIZE > localResults.stream().limit(4).mapToLong(H2ManagedLocalResult::getRowCount).sum());
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testLazyQueryWithSortByIndexedCol() throws Exception {
        // OOM on reducer.
        checkQueryExpectOOM("select * from K ORDER BY K.indexed", true);

        // Reduce only.
        assertEquals(1, localResults.size());
        assertTrue(maxMem > localResults.get(0).memoryReserved());
        assertTrue(BIG_TABLE_SIZE > localResults.get(0).getRowCount());
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testQueryWithGroupBy() {
        // Too many groups causes OOM.
        checkQueryExpectOOM("select K.name, count(K.id), sum(K.grp) from K GROUP BY K.name", true);

        // Local result is quite small.
        assertFalse(localResults.isEmpty());
        assertTrue(localResults.size() <= 4);
        assertTrue(BIG_TABLE_SIZE > localResults.stream().mapToLong(r -> r.getRowCount()).sum());
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testQueryWithGroupByPrimaryKey() throws Exception {
        //TODO: GG-19071: make next test pass without hint.
        // OOM on reducer.
        checkQueryExpectOOM("select K.indexed, sum(K.id) from K USE INDEX (K_IDX) GROUP BY K.indexed", true);

        assertEquals(1, localResults.size());
        assertTrue(BIG_TABLE_SIZE > localResults.get(0).getRowCount());
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testUnionSimple() throws Exception {
        maxMem = 2L * MB;

        execQuery("select * from T as T0, T as T1 where T0.id < 2 " +
            "UNION " +
            "select * from T as T2, T as T3 where T2.id >= 1 AND T2.id < 2", true);

        assertEquals(3, localResults.size());
        assertTrue(maxMem > localResults.get(1).memoryReserved() + localResults.get(2).memoryReserved());
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testQueryWithSortByIndexedCol() {
        maxMem = 2 * MB;

        checkQueryExpectOOM("select * from K ORDER BY K.indexed", false);

        assertEquals(4, localResults.size());
        assertFalse(localResults.stream().limit(4).anyMatch(r -> r.memoryReserved() + 1000 > maxMem));
        // Map
        assertTrue(BIG_TABLE_SIZE > localResults.stream().mapToLong(H2ManagedLocalResult::getRowCount).sum());
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testQueryWithGroupsSmallResult() throws Exception {
        execQuery("select K.grp, avg(K.id), min(K.id), sum(K.id) from K GROUP BY K.grp", false); // Tiny local result.

        assertEquals(5, localResults.size());
        assertFalse(localResults.stream().limit(4).anyMatch(r -> r.memoryReserved() + 1000 > maxMem));
        // Map
        assertEquals(100, localResults.stream().limit(4).mapToLong(r -> r.getRowCount()).sum());
        // Reduce
        assertEquals(100, localResults.get(4).getRowCount());
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testLazyQueryWithHighLimit() throws Exception {
        // OOM on reducer.
        checkQueryExpectOOM("select * from K LIMIT 8000", true);

        assertEquals(1, localResults.size());
        assertTrue(maxMem > localResults.get(0).memoryReserved());
        assertTrue(8000 > localResults.get(0).getRowCount());
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testQueryWithHighLimit() {
        long rowSize = rowSizeInBytes(new Object[]{ValueInt.get(1), ValueInt.get(1),
            ValueInt.get(1), ValueString.get(UUID.randomUUID().toString())});

        long rowCntPerSegment = BIG_TABLE_SIZE / PARALLELISM;

        // there should be enough memory for (PARALLELISM - 1) and a half segments
        maxMem = (long)(rowSize * rowCntPerSegment * (1.0 * PARALLELISM - 0.5));

        checkQueryExpectOOM("select 1, 1, 1, name from K LIMIT 8000", false);

        assertEquals(PARALLELISM, localResults.size());

        long rowsRetrieved = localResults.stream().collect(summarizingLong(H2ManagedLocalResult::getRowCount)).getSum();

        assertThat(rowsRetrieved, inRange(rowCntPerSegment * (PARALLELISM - 1), rowCntPerSegment * PARALLELISM));
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testLazyQueryWithGroupByIndexedColAndDistinctAggregates() throws Exception {
        checkQueryExpectOOM("select K.grp_indexed, count(DISTINCT k.name) from K  USE INDEX (K_GRP_IDX) GROUP BY K.grp_indexed", true);

        assertEquals(1, localResults.size());
        assertTrue(100 > localResults.get(0).getRowCount());
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testQueryWithGroupThenSort() throws Exception {
        // Tiny local result with sorting.
        execQuery("select K.grp_indexed, sum(K.id) as s from K GROUP BY K.grp_indexed ORDER BY s", false);

        assertEquals(5, localResults.size());
        assertEquals(100, localResults.get(4).getRowCount());
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testSimpleQueryLargeResult() throws Exception {
        maxMem = 3 * MB;
        execQuery("select * from K", false);

        assertFalse(localResults.isEmpty());
        assertTrue(localResults.size() <= 4);
        assertEquals(BIG_TABLE_SIZE, localResults.stream().mapToLong(H2ManagedLocalResult::getRowCount).sum());
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testQueryWithGroupByIndexedCol() throws Exception {
        checkQueryExpectOOM("select K.indexed, sum(K.grp) from K GROUP BY K.indexed", true);

        assertEquals(1, localResults.size());
        assertTrue(100 > localResults.get(0).getRowCount());
    }

    /** Check simple query with DISTINCT constraint. */
    @Test
    @Override public void testQueryWithDistinctAndLowCardinality() throws Exception {
        // Distinct on indexed column with small cardinality.
        execQuery("select DISTINCT K.grp_indexed from K", false);

        assertEquals(5, localResults.size());
        assertEquals(100, localResults.get(4).getRowCount());
    }

    /** Check query failure with DISTINCT constraint. */
    @Test
    @Override public void testQueryWithDistinctAndHighCardinality() throws Exception {
        // Distinct on indexed column with unique values.
        checkQueryExpectOOM("select DISTINCT K.id from K", true);

        assertEquals(4, localResults.size());
        assertFalse(localResults.stream().allMatch(r -> r.memoryReserved() + 1000 > maxMem));
        assertTrue(BIG_TABLE_SIZE > localResults.stream().mapToLong(H2ManagedLocalResult::getRowCount).sum());
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testLazyQueryWithGroupByThenSort() {
        maxMem = MB / 2;

        // Query failed on map side due too many groups.
        checkQueryExpectOOM("select K.indexed, sum(K.grp) as a from K " +
            "GROUP BY K.indexed ORDER BY a DESC", true);

        // Result on reduce side.
        assertEquals(1, localResults.size());
        assertEquals(0, localResults.get(0).memoryReserved());
        assertEquals(0, localResults.get(0).getRowCount());
    }

    /**
     * @param lowerBound Lower bound.
     * @param upperBound Upper bound.
     */
    private static <T extends Comparable<? super T>> Matcher<T> inRange(T lowerBound, T upperBound) {
        Objects.requireNonNull(lowerBound, "lowerBound");
        Objects.requireNonNull(upperBound, "upperBound");

        return new CustomMatcher<T>("should be in range [" + lowerBound + ", " + upperBound + "]") {
            @SuppressWarnings({"unchecked", "rawtypes"})
            @Override public boolean matches(Object item) {
                return lowerBound != null && upperBound != null && item instanceof Comparable
                        && ((Comparable)item).compareTo(lowerBound) >= 0 && ((Comparable)item).compareTo(upperBound) <= 0;
            }
        };
    }

}
