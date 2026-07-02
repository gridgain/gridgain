/*
 * Copyright 2026 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.internal.processors.query.h2.opt;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.MemorySegmentAccessInput;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

/**
 * End-to-end proof that Lucene's memory-segment vector scorer actually engages on a real HNSW search
 * through {@link GridLuceneDirectory} — the plumbing tests ({@link GridLuceneNoCopyInputIT}) exercise
 * {@link MemorySegmentAccessInput} directly but cannot show that Lucene takes the no-copy path — and that
 * the contiguous-on-close storage delivers the design's memory promise: <b>total off-heap == the exact sum
 * of file lengths, and a vector query allocates nothing</b> (no mirror, no doubling).
 * <p>
 * Engagement is asserted via the overlay's first-engagement INFO, which is emitted only when a segment is
 * actually handed to a caller of {@code segmentSliceOrNull} (i.e., the memory-segment scorer). Requires
 * the Panama vectorization provider ({@code --add-modules jdk.incubator.vector} in the failsafe argLine) —
 * Lucene's default scorer never asks for memory segments.
 */
public class GridLuceneNoCopyE2EIT extends GridLuceneNoCopyE2ESupport {
    /** */
    @Test
    public void testNoCopyScorerEngagesEndToEnd() throws Exception {
        try (IndexInput vec = dir.openInput(vecFileName(), IOContext.DEFAULT)) {
            assumeTrue("MR overlay not loaded — run against the packaged jar", vec instanceof MemorySegmentAccessInput);
        }

        try (DirectoryReader r = DirectoryReader.open(dir)) {
            IndexSearcher s = new IndexSearcher(r);

            // Contiguous-on-close storage: every closed file was compacted to its exact length, so total
            // off-heap equals the exact byte sum of the live files (the page layout would be bigger).
            long baseline = mem.allocatedSize();
            long exact = exactFileBytes();

            assertTrue("compacted storage must equal the exact file bytes: allocated=" + baseline
                + ", exact=" + exact, baseline >= exact && baseline <= exact + 64 * 1024);

            assertTrue("per-file compaction must be logged at FINE", compactionLogged());

            TopDocs first = s.search(new KnnFloatVectorQuery(FIELD, queryV[0], FANOUT), K);

            // THE no-doubling property this design exists for: serving the scorer allocates nothing —
            // the segment views the file's own storage.
            assertEquals("a vector query must not allocate off-heap memory (storage is already contiguous)",
                baseline, mem.allocatedSize());

            assertTrue("memory-segment scorer did not engage (no first-engagement INFO). Check that the "
                    + "failsafe argLine passes --add-modules jdk.incubator.vector (Lucene's default scorer "
                    + "never requests memory segments).",
                engagementLogged());

            TopDocs again = s.search(new KnnFloatVectorQuery(FIELD, queryV[0], FANOUT), K);

            assertEquals("repeat query allocated off-heap memory", baseline, mem.allocatedSize());

            assertArrayEquals("same query, same index — results must be deterministic",
                docIds(first), docIds(again));

            // And the results scored through the no-copy path must be correct.
            double recall = avgRecall(s);

            assertTrue("recall@10 through the no-copy scorer collapsed: " + recall, recall >= 0.99);
        }
    }
}
