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
 * <li>off-heap accounting — with contiguous-on-close storage (no-copy on), total allocation equals the
 * <b>exact</b> sum of live file lengths ({@link #exactFileBytes()}); with the kill switch off it equals
 * the <b>page-rounded</b> sum ({@link #pageRoundedFileBytes()}); and in neither case may a query allocate
 * anything (the no-doubling property this design exists for);</li>
 * <li>captured JUL records from the overlay {@code GridLuceneFile} and {@code GridLuceneInputStream}
 * loggers (attached before the index is built, so compaction, the first-engagement INFO and the
 * kill-switch static-init line are all observed).</li>
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

    /** Captured records from the overlay loggers (compaction, engagement, kill-switch notice). */
    final List<LogRecord> fileLogRecords = Collections.synchronizedList(new ArrayList<>());

    /** */
    private Logger fileLog;

    /** */
    private Logger inputLog;

    /** */
    private Handler capture;

    /** */
    @Before
    public void setUpE2E() throws Exception {
        assumeTrue("no-copy overlay needs a Java 22+ runtime", Runtime.version().feature() >= 22);

        // Attach the capturing handler BEFORE anything touches the overlay classes, so the kill-switch
        // static-init line, per-file compaction (FINE) and the first-engagement INFO are all observed.
        capture = new Handler() {
            @Override public void publish(LogRecord r) { fileLogRecords.add(r); }
            @Override public void flush() { /* No-op. */ }
            @Override public void close() { /* No-op. */ }
        };
        capture.setLevel(Level.ALL);

        fileLog = Logger.getLogger(GridLuceneFile.class.getName());
        fileLog.setLevel(Level.FINE);
        fileLog.addHandler(capture);

        inputLog = Logger.getLogger(GridLuceneInputStream.class.getName());
        inputLog.setLevel(Level.FINE);
        inputLog.addHandler(capture);

        mem = new GridUnsafeMemory(0);
        dir = new GridLuceneDirectory(mem);

        baseV = randomUnitVectors(N, 41);
        queryV = randomUnitVectors(NUM_Q, 42);
        exactTop = bruteForceTopK();

        // Keep the merged segment non-compound so the .vec flat-vectors file is a standalone
        // GridLuceneFile with a well-known name for the overlay-loaded probe.
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
        if (capture != null) {
            if (fileLog != null)
                fileLog.removeHandler(capture);

            if (inputLog != null)
                inputLog.removeHandler(capture);
        }

        if (dir != null)
            dir.close();
    }

    /** Exact sum of live file lengths — total off-heap when every closed file is compacted (no-copy on). */
    long exactFileBytes() throws IOException {
        long sum = 0;

        for (String f : dir.listAll())
            sum += dir.fileLength(f);

        return sum;
    }

    /**
     * Page-rounded sum of live file lengths, Σ ceil(len / {@link GridLuceneOutputStream#BUFFER_SIZE})
     * pages — total off-heap for the base page-chain layout (kill switch off).
     */
    long pageRoundedFileBytes() throws IOException {
        long sum = 0;

        for (String f : dir.listAll()) {
            long len = dir.fileLength(f);

            sum += ((len + BUFFER_SIZE - 1) / BUFFER_SIZE) * BUFFER_SIZE;
        }

        return sum;
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

    /** @return {@code true} if the overlay logged the first-engagement INFO (segment served to the scorer). */
    boolean engagementLogged() {
        return logged("no-copy vector scorer engaged");
    }

    /** @return {@code true} if the overlay logged at least one FINE page-to-contiguous compaction. */
    boolean compactionLogged() {
        return logged("Compacted");
    }

    /** */
    private boolean logged(String needle) {
        synchronized (fileLogRecords) {
            for (LogRecord r : fileLogRecords) {
                if (r.getMessage() != null && r.getMessage().contains(needle))
                    return true;
            }
        }

        return false;
    }

    /** @return {@code true} if the overlay logged the kill-switch "disabled" notice at class init. */
    boolean disabledNoticeLogged() {
        return logged("disabled");
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
