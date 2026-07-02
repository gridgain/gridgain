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
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.MemorySegmentAccessInput;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

/**
 * The {@code -DGRIDGAIN_VECTOR_NOCOPY_SCORER=false} kill switch: the overlay stays loaded (it is a class
 * in the MR jar, not a runtime toggle), but files keep the base page-chain layout —
 * {@code onOutputClosed()} does not compact, {@code contiguousAddr()} returns 0, {@code segmentSliceOrNull}
 * yields {@code null} and Lucene falls back to its copy-based scorer, restoring the exact pre-overlay
 * behavior. This IT runs in its own failsafe execution ({@code nocopy-killswitch-off}) that sets the
 * property before the JVM loads {@code GridLuceneFile} (the switch is read once, at class init).
 */
public class GridLuceneNoCopyKillSwitchIT extends GridLuceneNoCopyE2ESupport {
    /** */
    @Test
    public void testKillSwitchFallsBackToCopyScorer() throws Exception {
        assertEquals("this IT must run in the failsafe execution that disables the no-copy scorer",
            "false", System.getProperty("GRIDGAIN_VECTOR_NOCOPY_SCORER"));

        // The overlay itself is still the loaded class — only compaction + the scorer path are gated.
        try (IndexInput vec = dir.openInput(vecFileName(), IOContext.DEFAULT)) {
            assumeTrue("MR overlay not loaded — run against the packaged jar", vec instanceof MemorySegmentAccessInput);
        }

        assertTrue("GridLuceneFile must log the kill-switch notice at class init", disabledNoticeLogged());

        assertFalse("no compaction may happen with the kill switch off", compactionLogged());

        try (DirectoryReader r = DirectoryReader.open(dir)) {
            IndexSearcher s = new IndexSearcher(r);

            // Base layout preserved: whole 32 KB pages per file, nothing beyond them.
            long baseline = mem.allocatedSize();

            assertEquals("kill switch off must keep the exact page-chain layout",
                pageRoundedFileBytes(), baseline);

            s.search(new KnnFloatVectorQuery(FIELD, queryV[0], FANOUT), K);

            assertEquals("the copy-scorer fallback must not allocate off-heap memory either",
                baseline, mem.allocatedSize());

            assertFalse("the scorer must not engage no-copy with the switch off", engagementLogged());

            // Fallback correctness: the copy-based scorer must return the same-quality results.
            double recall = avgRecall(s);

            assertTrue("recall@10 through the copy fallback collapsed: " + recall, recall >= 0.99);
        }
    }
}
