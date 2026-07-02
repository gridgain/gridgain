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
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Random;
import org.apache.ignite.internal.util.offheap.unsafe.GridUnsafeMemory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.MemorySegmentAccessInput;
import org.apache.lucene.store.RandomAccessInput;
import org.junit.Before;
import org.junit.Test;

import static org.apache.ignite.internal.processors.query.h2.opt.GridLuceneOutputStream.BUFFER_SIZE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

/**
 * Exercises the Lucene-10-only no-copy vector-scoring path — the {@code src/main/java22} FFM overlay of
 * {@link GridLuceneInputStream} that implements {@link MemorySegmentAccessInput} (and its parent
 * {@link RandomAccessInput}). The base ({@code src/main/java}) input implements neither; it is loaded on
 * Java 21 and falls back to Lucene's copy scorer. The overlay's extra surface — absolute-position reads
 * that don't move the sequential cursor, and {@link MemorySegmentAccessInput#segmentSliceOrNull} exposing
 * the off-heap bytes in place — is what this test covers.
 * <p>
 * The overlay only shadows the base when classes load from the <b>multi-release JAR</b> on a Java-22+
 * runtime; exploded {@code target/classes} on the classpath never applies {@code META-INF/versions/}
 * versioning, so it would load the base. This is therefore run by {@code maven-failsafe-plugin} against the
 * packaged jar (see the {@code nocopy-java22-overlay} profile), not by surefire against {@code target/classes}.
 * If it ever loads the base (wrong classpath, or a pre-22 JVM), the {@code instanceof} guard below skips it
 * rather than reporting a false pass.
 */
public class GridLuceneNoCopyInputIT {
    /** */
    private GridLuceneDirectory dir;

    /** */
    @Before
    public void setUp() {
        assumeTrue("no-copy overlay needs a Java 22+ runtime", Runtime.version().feature() >= 22);

        dir = new GridLuceneDirectory(new GridUnsafeMemory(0));
    }

    /**
     * The load-bearing precondition: opened against the MR jar on Java 22+, the input must be the overlay,
     * i.e. a {@link MemorySegmentAccessInput}. A false here means the jar wiring is broken (the base loaded),
     * which would silently disable no-copy scoring in production.
     */
    @Test
    public void testOverlayInputIsMemorySegmentAccessInput() throws IOException {
        writeBytes("probe", randomBytes(128, 1));

        try (IndexInput in = dir.openInput("probe", IOContext.DEFAULT)) {
            assertTrue("expected the FFM overlay (MemorySegmentAccessInput), got " + in.getClass(),
                in instanceof MemorySegmentAccessInput);
            assertTrue("MemorySegmentAccessInput must also be a RandomAccessInput",
                in instanceof RandomAccessInput);
        }
    }

    /** Absolute-position reads return the right little-endian values and do not disturb the sequential cursor. */
    @Test
    public void testRandomAccessReadsDoNotMoveCursor() throws IOException {
        byte[] data = randomBytes(4096, 7);
        writeBytes("ra", data);

        try (IndexInput in = dir.openInput("ra", IOContext.DEFAULT)) {
            assumeOverlay(in);

            RandomAccessInput rai = (RandomAccessInput)in;

            in.seek(100);
            long cursor = in.getFilePointer();

            for (int pos : new int[] {0, 1, 17, 512, 1000, 4095})
                assertEquals("byte@" + pos, data[pos], rai.readByte(pos));

            assertEquals("short@10", leShort(data, 10), rai.readShort(10));
            assertEquals("int@20", leInt(data, 20), rai.readInt(20));
            assertEquals("long@40", leLong(data, 40), rai.readLong(40));

            assertEquals("random access must not move the sequential cursor", cursor, in.getFilePointer());
        }
    }

    /** Multi-byte absolute reads that straddle a 32 KB page boundary take the byte-by-byte fallback correctly. */
    @Test
    public void testRandomAccessAcrossPageBoundary() throws IOException {
        byte[] data = randomBytes(BUFFER_SIZE + 64, 9);
        writeBytes("bnd", data);

        try (IndexInput in = dir.openInput("bnd", IOContext.DEFAULT)) {
            assumeOverlay(in);

            RandomAccessInput rai = (RandomAccessInput)in;

            // Positions chosen so the read spans the buffer edge (off + width > BUFFER_SIZE).
            assertEquals("short straddle", leShort(data, BUFFER_SIZE - 1), rai.readShort(BUFFER_SIZE - 1));
            assertEquals("int straddle", leInt(data, BUFFER_SIZE - 2), rai.readInt(BUFFER_SIZE - 2));
            assertEquals("long straddle", leLong(data, BUFFER_SIZE - 3), rai.readLong(BUFFER_SIZE - 3));
        }
    }

    /** {@code segmentSliceOrNull} exposes the off-heap bytes in place; the view must be byte-exact and correctly sized. */
    @Test
    public void testSegmentSliceReflectsBytes() throws IOException {
        byte[] data = randomBytes(2000, 11);
        writeBytes("seg", data);

        try (IndexInput in = dir.openInput("seg", IOContext.DEFAULT)) {
            assumeOverlay(in);

            MemorySegmentAccessInput msai = (MemorySegmentAccessInput)in;

            MemorySegment whole = msai.segmentSliceOrNull(0, in.length());
            assertNotNull("contiguous storage should back a whole-file segment", whole);
            assertEquals("segment length", in.length(), whole.byteSize());
            for (int i = 0; i < data.length; i++)
                assertEquals("whole seg byte " + i, data[i], whole.get(ValueLayout.JAVA_BYTE, i));

            MemorySegment sub = msai.segmentSliceOrNull(100, 50);
            assertNotNull("sub-range segment", sub);
            assertEquals("sub-range length", 50L, sub.byteSize());
            for (int i = 0; i < 50; i++)
                assertEquals("sub seg byte " + i, data[100 + i], sub.get(ValueLayout.JAVA_BYTE, i));
        }
    }

    /**
     * Proves the "no-copy" property behaviourally, not just by byte-equality (which a copy would also pass):
     * repeated whole-file slices return the <b>same</b> native address (the segment views the file's contiguous storage, laid down once
     * and reused, not re-copied per call), and a sub-range slice is a view at the exact offset into that same
     * region ({@code base + pos}). A copy-based implementation would hand back fresh, unrelated addresses.
     */
    @Test
    public void testSegmentSliceIsStableAliasNotCopy() throws IOException {
        byte[] data = randomBytes(2000, 17);
        writeBytes("alias", data);

        try (IndexInput in = dir.openInput("alias", IOContext.DEFAULT)) {
            assumeOverlay(in);

            MemorySegmentAccessInput msai = (MemorySegmentAccessInput)in;

            MemorySegment first = msai.segmentSliceOrNull(0, in.length());
            MemorySegment again = msai.segmentSliceOrNull(0, in.length());
            assertNotNull(first);
            assertNotNull(again);
            assertEquals("repeated whole-file slice must return the same storage address, not a copy",
                first.address(), again.address());

            MemorySegment sub = msai.segmentSliceOrNull(100, 50);
            assertNotNull(sub);
            assertEquals("sub-range must be a view at base + pos (aliased, not copied)",
                first.address() + 100, sub.address());
        }
    }

    /** Out-of-bounds ranges must yield {@code null} (Lucene then falls back to its copy path), never a bad segment. */
    @Test
    public void testSegmentSliceOutOfBoundsReturnsNull() throws IOException {
        byte[] data = randomBytes(256, 3);
        writeBytes("oob", data);

        try (IndexInput in = dir.openInput("oob", IOContext.DEFAULT)) {
            assumeOverlay(in);

            MemorySegmentAccessInput msai = (MemorySegmentAccessInput)in;
            long len = in.length();

            assertNull("len overruns file", msai.segmentSliceOrNull(0, len + 1));
            assertNull("pos at EOF", msai.segmentSliceOrNull(len, 1));
            assertNull("negative pos", msai.segmentSliceOrNull(-1, 1));
        }
    }

    /** A slice's absolute reads and segment are relative to the slice start, and bounded by the slice length. */
    @Test
    public void testSliceRandomAccessIsSliceRelative() throws IOException {
        byte[] data = randomBytes(1000, 13);
        writeBytes("slc", data);

        int off = 200, len = 300;

        try (IndexInput base = dir.openInput("slc", IOContext.DEFAULT)) {
            assumeOverlay(base);

            IndexInput sl = base.slice("s", off, len);

            assertEquals("slice length", (long)len, sl.length());

            RandomAccessInput rai = (RandomAccessInput)sl;
            for (int p : new int[] {0, 1, 150, 299})
                assertEquals("slice byte@" + p, data[off + p], rai.readByte(p));
            assertEquals("slice int@10", leInt(data, off + 10), rai.readInt(10));

            MemorySegment seg = ((MemorySegmentAccessInput)sl).segmentSliceOrNull(0, len);
            assertNotNull("slice segment", seg);
            assertEquals("slice segment length", (long)len, seg.byteSize());
            for (int i = 0; i < len; i++)
                assertEquals("slice seg byte " + i, data[off + i], seg.get(ValueLayout.JAVA_BYTE, i));
        }
    }

    /** The overlay carries its own {@code readFloats}; exercise it end-to-end so the jar path is covered too. */
    @Test
    public void testReadFloatsThroughOverlay() throws IOException {
        float[] expected = randomFloats(1536);
        writeFloatsLittleEndian("f", expected);

        try (IndexInput in = dir.openInput("f", IOContext.DEFAULT)) {
            assumeOverlay(in);

            float[] actual = new float[expected.length];
            in.readFloats(actual, 0, actual.length);

            for (int i = 0; i < expected.length; i++)
                assertEquals("float " + i, expected[i], actual[i], 0.0f);
        }
    }

    /** Skip (not fail) if the base input loaded — see class doc; the {@code instanceof} probe test asserts it. */
    private static void assumeOverlay(IndexInput in) {
        assumeTrue("base input loaded (not the MR overlay) — skipping no-copy check",
            in instanceof MemorySegmentAccessInput);
    }

    /** Writes {@code data} verbatim to a directory file. */
    private void writeBytes(String name, byte[] data) throws IOException {
        try (IndexOutput out = dir.createOutput(name, IOContext.DEFAULT)) {
            out.writeBytes(data, 0, data.length);
        }
    }

    /** Writes {@code data} as little-endian float bytes (Lucene's vector encoding). */
    private void writeFloatsLittleEndian(String name, float[] data) throws IOException {
        byte[] bytes = new byte[data.length * 4];

        for (int i = 0; i < data.length; i++) {
            int bits = Float.floatToRawIntBits(data[i]);
            int j = i * 4;

            bytes[j] = (byte)bits;
            bytes[j + 1] = (byte)(bits >> 8);
            bytes[j + 2] = (byte)(bits >> 16);
            bytes[j + 3] = (byte)(bits >> 24);
        }

        writeBytes(name, bytes);
    }

    /** Deterministic pseudo-random bytes. */
    private static byte[] randomBytes(int n, long seed) {
        byte[] a = new byte[n];

        new Random(seed).nextBytes(a);

        return a;
    }

    /** Deterministic float data spanning positive/negative magnitudes (no NaN/Inf). */
    private static float[] randomFloats(int n) {
        Random r = new Random(42);
        float[] a = new float[n];

        for (int i = 0; i < n; i++)
            a[i] = r.nextFloat() * 2000.0f - 1000.0f;

        return a;
    }

    /** Little-endian decode helpers matching Lucene's on-disk order. */
    private static short leShort(byte[] b, int o) {
        return (short)((b[o] & 0xFF) | ((b[o + 1] & 0xFF) << 8));
    }

    /** */
    private static int leInt(byte[] b, int o) {
        return (b[o] & 0xFF) | ((b[o + 1] & 0xFF) << 8) | ((b[o + 2] & 0xFF) << 16) | ((b[o + 3] & 0xFF) << 24);
    }

    /** */
    private static long leLong(byte[] b, int o) {
        return (leInt(b, o) & 0xFFFFFFFFL) | (((long)leInt(b, o + 4)) << 32);
    }
}
