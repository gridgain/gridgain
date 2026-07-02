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
 * {@link MemorySegmentAccessInput} directly but cannot show that Lucene takes the no-copy path.
 * <p>
 * Engagement is asserted behaviourally: the contiguous mirror is only ever built when Lucene calls
 * {@code segmentSliceOrNull} (i.e., the memory-segment scorer path), so off-heap allocation beyond the
 * page buffers ≈ the {@code .vec} file length proves it, and ~0 refutes it. Requires the Panama
 * vectorization provider ({@code --add-modules jdk.incubator.vector} in the failsafe argLine) — Lucene's
 * default scorer never asks for memory segments.
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

            TopDocs first = s.search(new KnnFloatVectorQuery(FIELD, queryV[0], FANOUT), K);

            long excess = mirrorExcessBytes();

            // The mirror may have been built at merge time (graph construction scores the just-written
            // .vec through the production input) or by the query above — either way it must exist now.
            assertTrue("memory-segment scorer did not engage: no contiguous mirror after a vector query "
                    + "(excess=" + excess + ", expected >= " + VEC_BYTES + "). If 0, check that the failsafe "
                    + "argLine passes --add-modules jdk.incubator.vector (Lucene's default scorer never "
                    + "requests memory segments).",
                excess >= VEC_BYTES);

            assertTrue("only the .vec mirror should exceed the page buffers (excess=" + excess + ")",
                excess <= dir.fileLength(vecFileName()) + 256 * 1024);

            assertTrue("the overlay must log the mirror build", mirrorBuildLogged());

            // The mirror is built once and cached: a repeat query must not allocate again.
            TopDocs again = s.search(new KnnFloatVectorQuery(FIELD, queryV[0], FANOUT), K);

            assertEquals("repeat query re-allocated off-heap memory (mirror not cached?)",
                excess, mirrorExcessBytes());

            assertArrayEquals("same query, same index — results must be deterministic",
                docIds(first), docIds(again));

            // And the results scored through the no-copy path must be correct.
            double recall = avgRecall(s);

            assertTrue("recall@10 through the no-copy scorer collapsed: " + recall, recall >= 0.99);
        }
    }
}
