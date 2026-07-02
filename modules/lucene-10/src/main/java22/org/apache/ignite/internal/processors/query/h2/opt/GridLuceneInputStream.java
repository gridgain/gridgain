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

import org.apache.ignite.internal.processors.query.h2.opt.GridLuceneFile;
import org.apache.ignite.internal.processors.query.h2.opt.GridLuceneOutputStream;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.internal.util.offheap.unsafe.GridUnsafeMemory;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.MemorySegmentAccessInput;

import java.io.EOFException;
import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.util.Objects;

import static org.apache.ignite.internal.processors.query.h2.opt.GridLuceneOutputStream.BUFFER_SIZE;

/**
 * A memory-resident {@link IndexInput} implementation.
 */
public class GridLuceneInputStream extends IndexInput implements MemorySegmentAccessInput {
    /** */
    private GridLuceneFile file;

    /** */
    private long length;

    /** */
    private long currBuf;

    /** */
    private int currBufIdx;

    /** */
    private int bufPosition;

    /** */
    private long bufStart;

    /** */
    private int bufLength;

    /** */
    private final GridUnsafeMemory mem;

    /** */
    private volatile boolean closed;

    /** */
    private boolean isClone;

    /**
     * Constructor.
     *
     * @param name Name.
     * @param f File.
     * @throws IOException If failed.
     */
    public GridLuceneInputStream(String name, GridLuceneFile f) throws IOException {
        this(name, f, f.getLength());
    }

    /**
     * Constructor.
     *
     * @param name Name.
     * @param f File.
     * @param length inputStream length.
     * @throws IOException If failed.
     */
    public GridLuceneInputStream(String name, GridLuceneFile f, final long length) throws IOException {
        super("RAMInputStream(name=" + name + ")");

        file = f;

        this.length = length;

        if (length / BUFFER_SIZE >= Integer.MAX_VALUE)
            throw new IOException("RAMInputStream too large length=" + length + ": " + name);

        mem = file.getDirectory().memory();

        // make sure that we switch to the
        // first needed buffer lazily
        currBufIdx = -1;
        currBuf = 0;
    }

    /** {@inheritDoc} */
    @Override public void close() {
        if (!isClone) {
            closed = true;

            file.releaseRef();
        }
    }

    /**
     * {@inheritDoc}
     * <p>Returns {@link GridLuceneInputStream} (a subtype of both {@link IndexInput} and
     * {@link MemorySegmentAccessInput}) so it satisfies the covariant {@code clone()} of both.
     */
    @Override public GridLuceneInputStream clone() {
        GridLuceneInputStream clone = (GridLuceneInputStream) super.clone();

        if (closed)
            throw new AlreadyClosedException(toString());

        clone.isClone = true;

        return clone;

    }

    /** {@inheritDoc} */
    @Override public long length() {
        return length;
    }

    /** {@inheritDoc} */
    @Override public byte readByte() throws IOException {
        if (bufPosition >= bufLength) {
            currBufIdx++;

            switchCurrentBuffer(true);
        }

        return mem.readByte(currBuf + bufPosition++);
    }

    /** {@inheritDoc} */
    @Override public void readBytes(byte[] b, int offset, int len) throws IOException {
        while (len > 0) {
            if (bufPosition >= bufLength) {
                currBufIdx++;

                switchCurrentBuffer(true);
            }

            int remainInBuf = bufLength - bufPosition;
            int bytesToCp = len < remainInBuf ? len : remainInBuf;

            mem.readBytes(currBuf + bufPosition, b, offset, bytesToCp);

            offset += bytesToCp;
            len -= bytesToCp;

            bufPosition += bytesToCp;
        }
    }

    /**
     * Bulk float read. Fast path: when the whole vector is contiguous in the current off-heap buffer, copy
     * native memory straight into the destination {@code float[]} — no heap {@code byte[]} staging and no
     * scalar decode loop. When a vector straddles a page-buffer boundary, walk the buffers copying each run
     * straight into {@code dst} (still no staging). The copied bytes are in Lucene's little-endian order, so
     * on a little-endian JVM (the common case) {@code dst} is already correct; on big-endian we reverse each
     * 4-byte word in place. Replaces the default per-{@code readByte} scalar loop that dominated vector-search CPU.
     */
    @Override public void readFloats(float[] dst, int offset, int len) throws IOException {
        // Bounds-check the destination in production: the copyMemory writes below are unchecked native
        // stores, so enforce offset >= 0, len >= 0, offset + len <= dst.length here (not just under -ea).
        Objects.checkFromIndexSize(offset, len, dst.length);

        int n = len << 2;

        if (bufPosition >= bufLength) {
            currBufIdx++;

            switchCurrentBuffer(true);
        }

        long dstAddr = GridUnsafe.FLOAT_ARR_OFF + ((long)offset << 2);

        if (bufPosition + n <= bufLength) {
            // Fast path: the whole vector is contiguous in the current off-heap buffer — one native copy.
            GridUnsafe.copyMemory(null, currBuf + bufPosition, dst, dstAddr, n);

            bufPosition += n;
        }
        else {
            // Slow path: the vector straddles a page-buffer boundary, so it can't be copied in one shot.
            // Walk the buffers, copying each contiguous run straight into dst's native memory (still no
            // heap byte[] staging); switchCurrentBuffer() advances to the next page between runs.
            int rem = n;

            while (rem > 0) {
                if (bufPosition >= bufLength) {
                    currBufIdx++;

                    switchCurrentBuffer(true);
                }

                int chunk = Math.min(rem, bufLength - bufPosition);

                GridUnsafe.copyMemory(null, currBuf + bufPosition, dst, dstAddr, chunk);

                bufPosition += chunk;
                dstAddr += chunk;
                rem -= chunk;
            }
        }

        // The bytes copied above are in Lucene's little-endian order. That already matches the native float
        // layout on a little-endian JVM; on big-endian, reverse each 4-byte word in place to fix it up.
        if (GridUnsafe.BIG_ENDIAN) {
            for (int i = 0; i < len; i++)
                dst[offset + i] = Float.intBitsToFloat(Integer.reverseBytes(Float.floatToRawIntBits(dst[offset + i])));
        }
    }

    // ---- RandomAccessInput / MemorySegmentAccessInput: no-copy vector scoring ----
    // These read at an absolute logical position without disturbing the sequential cursor, so Lucene's
    // memory-segment flat-vector scorer can read each candidate vector in place (off-heap) instead of
    // copying it into a scratch float[] before every distance. All multi-byte reads are little-endian
    // (Lucene 9+ index format).

    /** Maps a position relative to this input to an absolute file offset (overridden by slices). */
    long fileOffset(long pos) {
        return pos;
    }

    /** {@inheritDoc} */
    @Override public byte readByte(long pos) {
        long a = fileOffset(pos);

        return mem.readByte(file.getBuffer((int)(a / BUFFER_SIZE)) + (int)(a % BUFFER_SIZE));
    }

    /** {@inheritDoc} */
    @Override public short readShort(long pos) throws IOException {
        long a = fileOffset(pos);
        int off = (int)(a % BUFFER_SIZE);

        if (off + 2 <= BUFFER_SIZE) {
            long base = file.getBuffer((int)(a / BUFFER_SIZE)) + off;

            return (short)((mem.readByte(base) & 0xFF) | ((mem.readByte(base + 1) & 0xFF) << 8));
        }

        return (short)((readByte(pos) & 0xFF) | ((readByte(pos + 1) & 0xFF) << 8));
    }

    /** {@inheritDoc} */
    @Override public int readInt(long pos) throws IOException {
        long a = fileOffset(pos);
        int off = (int)(a % BUFFER_SIZE);

        if (off + 4 <= BUFFER_SIZE) {
            long base = file.getBuffer((int)(a / BUFFER_SIZE)) + off;

            return (mem.readByte(base) & 0xFF) | ((mem.readByte(base + 1) & 0xFF) << 8)
                | ((mem.readByte(base + 2) & 0xFF) << 16) | ((mem.readByte(base + 3) & 0xFF) << 24);
        }

        return (readByte(pos) & 0xFF) | ((readByte(pos + 1) & 0xFF) << 8)
            | ((readByte(pos + 2) & 0xFF) << 16) | ((readByte(pos + 3) & 0xFF) << 24);
    }

    /** {@inheritDoc} */
    @Override public long readLong(long pos) throws IOException {
        return (readInt(pos) & 0xFFFFFFFFL) | (((long)readInt(pos + 4)) << 32);
    }

    /**
     * Returns a {@link MemorySegment} view over the off-heap bytes {@code [pos, pos+len)} using the file's
     * contiguous mirror, so Lucene's memory-segment vector scorer can read vectors in place with no copy.
     * Lucene requests the whole input ({@code segmentSliceOrNull(0, length())}) up front; that only
     * succeeds against a single contiguous region, which {@link GridLuceneFile#contiguousAddr()} provides
     * (the file is otherwise stored as discontiguous 32 KB pages). Returns {@code null} if the mirror is
     * unavailable or the range is out of bounds, in which case Lucene falls back to its copy path.
     */
    @Override public MemorySegment segmentSliceOrNull(long pos, long len) throws IOException {
        long base = file.contiguousAddr();

        if (base == 0)
            return null;

        long a = fileOffset(pos);

        if (a < 0 || a + len > length)
            return null;

        return MemorySegment.ofAddress(base + a).reinterpret(len);
    }

    /**
     * Switch buffer to next.
     *
     * @param enforceEOF if we need to enforce {@link EOFException}.
     * @throws IOException if failed.
     */
    private void switchCurrentBuffer(boolean enforceEOF) throws IOException {
        bufStart = (long)BUFFER_SIZE * (long)currBufIdx;

        if (currBufIdx >= file.numBuffers()) {
            // end of file reached, no more buffers left
            if (enforceEOF)
                throw new EOFException("read past EOF: " + this);

            // Force EOF if a read takes place at this position
            currBufIdx--;
            bufPosition = BUFFER_SIZE;
        }
        else {
            currBuf = file.getBuffer(currBufIdx);
            bufPosition = 0;

            long buflen = length - bufStart;

            bufLength = buflen > BUFFER_SIZE ? BUFFER_SIZE : (int)buflen;
        }
    }

    /** {@inheritDoc} */
    @Override public IndexInput slice(final String sliceDescription, final long offset, final long length)
        throws IOException {
        if (offset < 0 || length < 0 || offset + length > this.length)
            throw new IllegalArgumentException("slice() " + sliceDescription + " out of bounds: " + this);

        final String newResourceDescription = (sliceDescription == null) ? toString() : (toString() + " [slice=" + sliceDescription + "]");

        return new SlicedInputStream(newResourceDescription, offset, length);
    }

    /**
     * For direct calls from {@link GridLuceneOutputStream}.
     *
     * @param ptr Pointer.
     * @param len Length.
     * @throws IOException If failed.
     */
    void readBytes(long ptr, int len) throws IOException {
        while (len > 0) {
            if (bufPosition >= bufLength) {
                currBufIdx++;

                switchCurrentBuffer(true);
            }

            int remainInBuf = bufLength - bufPosition;
            int bytesToCp = len < remainInBuf ? len : remainInBuf;

            mem.copyMemory(currBuf + bufPosition, ptr, bytesToCp);

            ptr += bytesToCp;
            len -= bytesToCp;

            bufPosition += bytesToCp;
        }
    }

    /** {@inheritDoc} */
    @Override public long getFilePointer() {
        return currBufIdx < 0 ? 0 : bufStart + bufPosition;
    }

    /** {@inheritDoc} */
    @Override public void seek(long pos) throws IOException {
        if (currBuf == 0 || pos < bufStart || pos >= bufStart + BUFFER_SIZE) {
            currBufIdx = (int)(pos / BUFFER_SIZE);

            switchCurrentBuffer(false);
        }

        bufPosition = (int)(pos % BUFFER_SIZE);
    }

    /** */
    private class SlicedInputStream extends GridLuceneInputStream {
        /** */
        private final long offset;

        /** */
        public SlicedInputStream(String newResourceDescription, long offset, long length) throws IOException {
            super(newResourceDescription, GridLuceneInputStream.this.file, offset + length);

            // Avoid parent resource closing together with this.
            super.isClone = true;

            this.offset = offset;

            seek(0L);
        }

        /** {@inheritDoc} */
        @Override public void seek(long pos) throws IOException {
            if (pos < 0L) {
                throw new IllegalArgumentException("Seeking to negative position: " + this);
            }
            super.seek(pos + offset);
        }

        /** {@inheritDoc} */
        @Override public long getFilePointer() {
            return super.getFilePointer() - offset;
        }

        /** {@inheritDoc} */
        @Override public long length() {
            return super.length() - offset;
        }

        /** {@inheritDoc} Positions in a slice are relative to the slice's start within the file. */
        @Override long fileOffset(long pos) {
            return offset + pos;
        }

        /** {@inheritDoc} */
        @Override public IndexInput slice(String sliceDescription, long ofs, long len) throws IOException {
            return super.slice(sliceDescription, offset + ofs, len);
        }
    }
}
