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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import org.apache.ignite.internal.util.offheap.unsafe.GridUnsafeMemory;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.junit.After;
import org.junit.Before;

import static org.apache.ignite.internal.processors.query.h2.opt.GridLuceneOutputStream.BUFFER_SIZE;
import static org.junit.Assume.assumeTrue;

/**
 * Shared fixture for the end-to-end no-copy scorer tests: builds a real Lucene HNSW index over
 * {@link GridLuceneDirectory}, computes exact ground truth by brute force, and provides two probes:
 * <ul>
 * <li>{@link #mirrorExcessBytes()} — off-heap bytes allocated beyond the directory's page buffers. The
 * only non-page allocation in this module is {@link GridLuceneFile}'s contiguous mirror, so a value of
 * about the {@code .vec} file length means Lucene's memory-segment scorer engaged (whether at merge time —
 * graph construction scores through the production input — or at first query), and ~0 means it did not.</li>
 * <li>captured JUL records from the overlay {@code GridLuceneFile} logger (attached before the index is
 * built, so merge-time mirror builds and the kill-switch static-init line are observed too).</li>
 * </ul>
 * Not named {@code *IT}, so failsafe does not run it directly.
 */
abstract class GridLuceneNoCopyE2ESupport {
    /** Small enough to build in ~a second, big enough that HNSW at low fanout is not trivially exact. */
    static final int N = 2_000;

    /** */
    static final int DIM = 64;

    /** */
    static final int NUM_Q = 100;

    /** HNSW candidate fanout (efSearch); generous for N=2000, so recall@10 should be ~1.0. */
    static final int FANOUT = 200;

    /** */
    static final int K = 10;

    /** */
    static final String FIELD = "v";

    /** Raw vector payload of the {@code .vec} file — the mirror is at least this big. */
    static final long VEC_BYTES = (long)N * DIM * 4;

    /** */
    GridUnsafeMemory mem;

    /** */
    GridLuceneDirectory dir;

    /** */
    float[][] baseV;

    /** */
    float[][] queryV;

    /** Exact top-{@link #K} doc ids per query (docId == insertion order: single forceMerged segment). */
    int[][] exactTop;

    /** Captured records from the overlay GridLuceneFile logger (mirror builds, kill-switch notice). */
    final List<LogRecord> fileLogRecords = Collections.synchronizedList(new ArrayList<>());

    /** */
    private Logger fileLog;

    /** */
    private Handler capture;

    /** */
    @Before
    public void setUpE2E() throws Exception {
        assumeTrue("no-copy overlay needs a Java 22+ runtime", Runtime.version().feature() >= 22);

        // Attach the capturing handler BEFORE anything touches GridLuceneFile, so its static-init line
        // (kill switch) and merge-time mirror builds are observed as well.
        fileLog = Logger.getLogger(GridLuceneFile.class.getName());
        fileLog.setLevel(Level.FINE);
        capture = new Handler() {
            @Override public void publish(LogRecord r) { fileLogRecords.add(r); }
            @Override public void flush() { /* No-op. */ }
            @Override public void close() { /* No-op. */ }
        };
        capture.setLevel(Level.ALL);
        fileLog.addHandler(capture);

        mem = new GridUnsafeMemory(0);
        dir = new GridLuceneDirectory(mem);

        baseV = randomUnitVectors(N, 41);
        queryV = randomUnitVectors(NUM_Q, 42);
        exactTop = bruteForceTopK();

        // Keep the merged segment non-compound so the .vec flat-vectors file is a standalone
        // GridLuceneFile — the unit the mirror is built for.
        IndexWriterConfig iwc = new IndexWriterConfig().setUseCompoundFile(false);
        iwc.getMergePolicy().setNoCFSRatio(0.0);

        try (IndexWriter w = new IndexWriter(dir, iwc)) {
            for (float[] v : baseV) {
                Document d = new Document();

                d.add(new KnnFloatVectorField(FIELD, v, VectorSimilarityFunction.DOT_PRODUCT));

                w.addDocument(d);
            }

            w.forceMerge(1);
        }
    }

    /** */
    @After
    public void tearDownE2E() throws Exception {
        if (fileLog != null && capture != null)
            fileLog.removeHandler(capture);

        if (dir != null)
            dir.close();
    }

    /**
     * Off-heap bytes allocated beyond the page buffers of all live files. Files are stored as whole
     * {@link GridLuceneOutputStream#BUFFER_SIZE} pages, so page bytes = Σ ceil(len / BUFFER_SIZE) pages;
     * anything above that is the contiguous no-copy mirror (transient merge-time mirrors of deleted
     * files are freed with their files and do not linger).
     */
    long mirrorExcessBytes() throws IOException {
        long pages = 0;

        for (String f : dir.listAll()) {
            long len = dir.fileLength(f);

            pages += ((len + BUFFER_SIZE - 1) / BUFFER_SIZE) * BUFFER_SIZE;
        }

        return mem.allocatedSize() - pages;
    }

    /** The {@code .vec} flat-vectors file of the single forceMerged segment. */
    String vecFileName() throws IOException {
        for (String f : dir.listAll()) {
            if (f.endsWith(".vec"))
                return f;
        }

        throw new AssertionError("no .vec file in " + Arrays.toString(dir.listAll())
            + " — compound files unexpectedly enabled");
    }

    /** @return {@code true} if the overlay logged a contiguous-mirror build (INFO first, FINE after). */
    boolean mirrorBuildLogged() {
        synchronized (fileLogRecords) {
            for (LogRecord r : fileLogRecords) {
                if (r.getMessage() != null && r.getMessage().contains("mirror built"))
                    return true;
            }
        }

        return false;
    }

    /** @return {@code true} if the overlay logged the kill-switch "disabled" notice at class init. */
    boolean disabledNoticeLogged() {
        synchronized (fileLogRecords) {
            for (LogRecord r : fileLogRecords) {
                if (r.getMessage() != null && r.getMessage().contains("disabled"))
                    return true;
            }
        }

        return false;
    }

    /** Average recall@{@link #K} over all queries against the brute-force ground truth. */
    double avgRecall(IndexSearcher s) throws IOException {
        double sum = 0;

        for (int i = 0; i < NUM_Q; i++) {
            TopDocs td = s.search(new KnnFloatVectorQuery(FIELD, queryV[i], FANOUT), K);

            Set<Integer> truth = new HashSet<>();

            for (int j = 0; j < K; j++)
                truth.add(exactTop[i][j]);

            int hit = 0;

            for (int j = 0; j < Math.min(K, td.scoreDocs.length); j++) {
                if (truth.contains(td.scoreDocs[j].doc))
                    hit++;
            }

            sum += hit / (double)K;
        }

        return sum / NUM_Q;
    }

    /** */
    static int[] docIds(TopDocs td) {
        int[] out = new int[td.scoreDocs.length];

        for (int i = 0; i < out.length; i++)
            out[i] = td.scoreDocs[i].doc;

        return out;
    }

    /** Exact top-K by dot product (vectors are unit-normalized, so dot == cosine). */
    private int[][] bruteForceTopK() {
        int[][] out = new int[NUM_Q][K];

        for (int qi = 0; qi < NUM_Q; qi++) {
            float[] q = queryV[qi];
            double[] score = new double[N];

            for (int b = 0; b < N; b++) {
                double dot = 0;

                for (int d = 0; d < DIM; d++)
                    dot += (double)q[d] * baseV[b][d];

                score[b] = dot;
            }

            Integer[] idx = new Integer[N];

            for (int b = 0; b < N; b++)
                idx[b] = b;

            Arrays.sort(idx, (a, b) -> Double.compare(score[b], score[a]));

            for (int j = 0; j < K; j++)
                out[qi][j] = idx[j];
        }

        return out;
    }

    /** Deterministic random unit vectors (gaussian components, L2-normalized). */
    private static float[][] randomUnitVectors(int count, long seed) {
        Random r = new Random(seed);
        float[][] out = new float[count][DIM];

        for (int i = 0; i < count; i++) {
            double norm = 0;

            for (int d = 0; d < DIM; d++) {
                out[i][d] = (float)r.nextGaussian();

                norm += (double)out[i][d] * out[i][d];
            }

            float inv = (float)(1.0 / Math.sqrt(norm));

            for (int d = 0; d < DIM; d++)
                out[i][d] *= inv;
        }

        return out;
    }
}
