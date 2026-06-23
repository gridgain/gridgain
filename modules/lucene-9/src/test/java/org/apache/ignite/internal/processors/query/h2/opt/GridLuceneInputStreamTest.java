/*
 * Copyright 2025 GridGain Systems, Inc. and Contributors.
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
import java.util.Random;
import org.apache.ignite.internal.util.offheap.unsafe.GridUnsafeMemory;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.junit.Before;
import org.junit.Test;

import static org.apache.ignite.internal.processors.query.h2.opt.GridLuceneOutputStream.BUFFER_SIZE;

/**
 * Tests {@link GridLuceneInputStream#readFloats(float[], int, int)} — the bulk vector read used by the
 * Lucene HNSW vector search. Covers both the fast path (a vector fully contained in one off-heap page
 * buffer, copied via {@code Unsafe}) and the fallback (a vector straddling a {@link
 * GridLuceneOutputStream#BUFFER_SIZE} boundary, read byte-wise and little-endian decoded). The fallback
 * decode is also the exact code the big-endian branch uses, so these tests cover that decode too; the
 * only big-endian-specific element — the {@code !GridUnsafe.BIG_ENDIAN} guard that routes to the
 * fallback — cannot be exercised on little-endian hardware.
 */
public class GridLuceneInputStreamTest extends GridCommonAbstractTest {
    /** Floats per page buffer (BUFFER_SIZE is a whole number of 4-byte floats). */
    private static final int FLOATS_PER_BUF = BUFFER_SIZE / 4;

    /** */
    private GridLuceneDirectory dir;

    /** */
    @Before
    public void setUp() {
        dir = new GridLuceneDirectory(new GridUnsafeMemory(0));
    }

    /** Fast path: a vector that fits entirely inside the current page buffer. */
    @Test
    public void testFastPathWithinBuffer() throws IOException {
        float[] expected = randomFloats(1536); // one 1536-d vector, 6 KB < 32 KB buffer
        writeFloatsLittleEndian("vec", expected);

        try (IndexInput in = dir.openInput("vec", IOContext.DEFAULT)) {
            float[] actual = new float[expected.length];
            in.readFloats(actual, 0, actual.length);

            assertFloatsEqual(expected, actual);
            assertEquals((long)expected.length * 4, in.getFilePointer());
        }
    }

    /** Fallback: a single read that spans several page buffers. */
    @Test
    public void testFallbackAcrossBuffers() throws IOException {
        float[] expected = randomFloats(FLOATS_PER_BUF * 2 + 813); // > 2 buffers, not buffer-aligned
        writeFloatsLittleEndian("big", expected);

        try (IndexInput in = dir.openInput("big", IOContext.DEFAULT)) {
            float[] actual = new float[expected.length];
            in.readFloats(actual, 0, actual.length);

            assertFloatsEqual(expected, actual);
        }
    }

    /** Fallback: a small read positioned so the float run straddles exactly one buffer boundary. */
    @Test
    public void testStraddleAtBufferBoundary() throws IOException {
        float[] expected = randomFloats(FLOATS_PER_BUF + 64);
        writeFloatsLittleEndian("straddle", expected);

        // Start 2 floats before the boundary and read 8 — the run crosses the buffer edge.
        int startFloat = FLOATS_PER_BUF - 2;

        try (IndexInput in = dir.openInput("straddle", IOContext.DEFAULT)) {
            in.seek((long)startFloat * 4);

            float[] actual = new float[8];
            in.readFloats(actual, 0, actual.length);

            for (int i = 0; i < actual.length; i++)
                assertEquals("float " + (startFloat + i), expected[startFloat + i], actual[i], 0.0f);
        }
    }

    /** The {@code offset} argument must write only into {@code dst[offset, offset+len)}. */
    @Test
    public void testReadIntoDestOffset() throws IOException {
        float[] expected = randomFloats(100);
        writeFloatsLittleEndian("off", expected);

        try (IndexInput in = dir.openInput("off", IOContext.DEFAULT)) {
            float[] actual = new float[110];
            float sentinel = -7.5f;
            java.util.Arrays.fill(actual, sentinel);

            in.readFloats(actual, 10, 100);

            for (int i = 0; i < 10; i++)
                assertEquals("untouched prefix " + i, sentinel, actual[i], 0.0f);
            for (int i = 0; i < 100; i++)
                assertEquals("payload " + i, expected[i], actual[10 + i], 0.0f);
        }
    }

    /** Fast path must decode bytes identically to an explicit little-endian {@code readByte} loop. */
    @Test
    public void testMatchesScalarLittleEndianDecode() throws IOException {
        float[] expected = randomFloats(1024);
        writeFloatsLittleEndian("cmp", expected);

        float[] viaReadFloats = new float[expected.length];
        try (IndexInput in = dir.openInput("cmp", IOContext.DEFAULT)) {
            in.readFloats(viaReadFloats, 0, viaReadFloats.length);
        }

        float[] viaBytes = new float[expected.length];
        try (IndexInput in = dir.openInput("cmp", IOContext.DEFAULT)) {
            for (int i = 0; i < viaBytes.length; i++) {
                int b0 = in.readByte() & 0xFF, b1 = in.readByte() & 0xFF;
                int b2 = in.readByte() & 0xFF, b3 = in.readByte() & 0xFF;
                viaBytes[i] = Float.intBitsToFloat(b0 | (b1 << 8) | (b2 << 16) | (b3 << 24));
            }
        }

        assertFloatsEqual(viaBytes, viaReadFloats);
    }

    /** Out-of-bounds (offset, len) must trip the bounds assert when assertions are enabled. */
    @Test
    public void testBoundsAssert() throws IOException {
        boolean assertionsOn = false;
        assert assertionsOn = true;

        if (!assertionsOn)
            return; // -ea disabled: the Unsafe fast path has no bounds check to verify

        writeFloatsLittleEndian("b", randomFloats(16));

        try (IndexInput in = dir.openInput("b", IOContext.DEFAULT)) {
            float[] dst = new float[8];

            try {
                in.readFloats(dst, 4, 8); // 4 + 8 > 8

                fail("Expected AssertionError for out-of-bounds (offset, len)");
            }
            catch (AssertionError expected) {
                // Expected.
            }
        }
    }

    /** Asserts two float arrays are bit-for-bit equal (no tolerance — the bytes round-trip exactly). */
    private static void assertFloatsEqual(float[] expected, float[] actual) {
        assertEquals("length", expected.length, actual.length);

        for (int i = 0; i < expected.length; i++)
            assertEquals("float " + i, expected[i], actual[i], 0.0f);
    }

    /** Writes {@code data} as little-endian float bytes (Lucene's vector encoding) to a directory file. */
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

        try (IndexOutput out = dir.createOutput(name, IOContext.DEFAULT)) {
            out.writeBytes(bytes, 0, bytes.length);
        }
    }

    /** Deterministic float data spanning positive/negative magnitudes (no NaN/Inf). */
    private static float[] randomFloats(int n) {
        Random r = new Random(42);
        float[] a = new float[n];

        for (int i = 0; i < n; i++)
            a[i] = r.nextFloat() * 2000.0f - 1000.0f;

        return a;
    }
}
