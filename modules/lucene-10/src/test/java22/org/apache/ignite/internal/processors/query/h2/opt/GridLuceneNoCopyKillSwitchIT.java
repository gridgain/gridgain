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
 * in the MR jar, not a runtime toggle), but {@code GridLuceneFile.contiguousAddr()} returns 0, so
 * {@code segmentSliceOrNull} yields {@code null} and Lucene falls back to its copy-based scorer — trading
 * the mirror's off-heap memory back for RAM. This IT runs in its own failsafe execution
 * ({@code nocopy-killswitch-off}) that sets the property before the JVM loads {@code GridLuceneFile}
 * (the switch is read once, at class init).
 */
public class GridLuceneNoCopyKillSwitchIT extends GridLuceneNoCopyE2ESupport {
    /** */
    @Test
    public void testKillSwitchFallsBackToCopyScorer() throws Exception {
        assertEquals("this IT must run in the failsafe execution that disables the no-copy scorer",
            "false", System.getProperty("GRIDGAIN_VECTOR_NOCOPY_SCORER"));

        // The overlay itself is still the loaded class — only the mirror is gated.
        try (IndexInput vec = dir.openInput(vecFileName(), IOContext.DEFAULT)) {
            assumeTrue("MR overlay not loaded — run against the packaged jar", vec instanceof MemorySegmentAccessInput);
        }

        assertTrue("GridLuceneFile must log the kill-switch notice at class init", disabledNoticeLogged());

        try (DirectoryReader r = DirectoryReader.open(dir)) {
            IndexSearcher s = new IndexSearcher(r);

            s.search(new KnnFloatVectorQuery(FIELD, queryV[0], FANOUT), K);

            long excess = mirrorExcessBytes();

            assertTrue("kill switch off must mean NO contiguous mirror, but excess=" + excess
                + " bytes beyond the page buffers", excess < 64 * 1024);

            assertFalse("no mirror build may be logged with the kill switch off", mirrorBuildLogged());

            // Fallback correctness: the copy-based scorer must return the same-quality results.
            double recall = avgRecall(s);

            assertTrue("recall@10 through the copy fallback collapsed: " + recall, recall >= 0.99);
        }
    }
}
