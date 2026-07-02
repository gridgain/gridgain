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

import org.apache.lucene.util.Accountable;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

import static org.apache.ignite.internal.processors.query.h2.opt.GridLuceneOutputStream.BUFFER_SIZE;

/**
 * Lucene file.
 */
public class GridLuceneFile implements Accountable {
    /** */
    private LongArray buffers = new LongArray();

    /** */
    private long length;

    /** */
    private final String name;

    /** */
    private final GridLuceneDirectory dir;

    /** */
    private volatile long sizeInBytes;

    /** */
    private final AtomicLong refCnt = new AtomicLong();

    /** */
    private final AtomicBoolean deleted = new AtomicBoolean();

    /**
     * Kill switch for the no-copy contiguous mirror (default on). When off, {@link #contiguousAddr()}
     * returns 0 and Lucene falls back to its copy-based vector scorer — useful to trade the mirror's extra
     * memory back for RAM, or to isolate regressions.
     */
    private static final boolean NOCOPY_MIRROR =
        Boolean.parseBoolean(System.getProperty("GRIDGAIN_VECTOR_NOCOPY_SCORER", "true"));

    /** JUL, matching Lucene's own vectorization-status logging — this module has no Ignite logger wiring. */
    private static final Logger log = Logger.getLogger(GridLuceneFile.class.getName());

    /** First mirror build is logged at INFO (the operator-visible "no-copy is active" signal). */
    private static final AtomicBoolean engagedLogged = new AtomicBoolean();

    static {
        // Whether the no-copy scorer can engage is otherwise invisible in logs (an MR-jar-vs-exploded
        // classpath mix-up or this switch silently fall back to the copy scorer), so announce the off state.
        if (!NOCOPY_MIRROR) {
            log.info("GridGain no-copy vector scorer disabled via -DGRIDGAIN_VECTOR_NOCOPY_SCORER=false; " +
                "Lucene will use its copy-based vector scorer.");
        }
    }

    /** Lazily-built contiguous off-heap mirror of the whole file (0 = not built). See {@link #contiguousAddr()}. */
    private volatile long contiguousPtr;

    /** Allocation size of the contiguous mirror, for release on delete. */
    private long contiguousLen;

    /**
     * File used as buffer, in no RAMDirectory
     *
     * @param dir Directory.
     */
    GridLuceneFile(GridLuceneDirectory dir, String name) {
        this.dir = dir;
        this.name = name;
    }

    /**
     * @return filename
     */
    public String getName() {
        return name;
    }

    /**
     * For non-stream access from thread that might be concurrent with writing
     *
     * @return Length.
     */
    public synchronized long getLength() {
        return length;
    }

    /**
     * Sets length.
     *
     * @param length Length.
     */
    protected synchronized void setLength(long length) {
        this.length = length;
    }

    /**
     * @return New buffer address.
     */
    final long addBuffer() {
        long buf = newBuffer();

        synchronized (this) {
            buffers.add(buf);

            sizeInBytes += BUFFER_SIZE;
        }

        if (dir != null)
            dir.sizeInBytes.getAndAdd(BUFFER_SIZE);

        return buf;
    }

    /**
     * Increment ref counter.
     */
    void lockRef() {
        refCnt.incrementAndGet();
    }

    /**
     * Decrement ref counter.
     */
    void releaseRef() {
        refCnt.decrementAndGet();

        deferredDelete();
    }

    /**
     * Checks if there is file stream opened.
     *
     * @return {@code True} if file has external references.
     */
    boolean hasRefs() {
        long refs = refCnt.get();

        assert refs >= 0;

        return refs != 0;
    }

    /**
     * Gets address of buffer.
     *
     * @param idx Index.
     * @return Pointer.
     */
    final synchronized long getBuffer(int idx) {
        return buffers.get(idx);
    }

    /**
     * @return Number of buffers.
     */
    final synchronized int numBuffers() {
        return buffers.size();
    }

    /**
     * Address of a contiguous off-heap copy of the entire file, built lazily on first call. The file is
     * normally stored as discontiguous {@link GridLuceneOutputStream#BUFFER_SIZE} pages; this single
     * contiguous mirror lets Lucene's memory-segment vector scorer read each candidate vector in place
     * (no per-candidate copy into a scratch {@code float[]}). Returns {@code 0} when unavailable
     * (empty/deleted file). Freed in {@link #deferredDelete()}. The sequential read path is unaffected.
     *
     * @return Contiguous mirror address, or {@code 0}.
     */
    final long contiguousAddr() {
        long p = contiguousPtr;

        if (p != 0)
            return p;

        return NOCOPY_MIRROR ? buildContiguous() : 0;
    }

    /**
     * Builds the contiguous mirror once (synchronized so concurrent query threads build at most one).
     *
     * @return Mirror address, or {@code 0}.
     */
    private synchronized long buildContiguous() {
        if (contiguousPtr != 0)
            return contiguousPtr;

        if (deleted.get() || length <= 0)
            return 0;

        long dst = dir.memory().allocate(length);

        long copied = 0;

        for (int i = 0, nb = buffers.size(); i < nb && copied < length; i++) {
            long chunk = Math.min(BUFFER_SIZE, length - copied);

            dir.memory().copyMemory(buffers.get(i), dst + copied, chunk);

            copied += chunk;
        }

        contiguousLen = length;
        contiguousPtr = dst;

        if (engagedLogged.compareAndSet(false, true)) {
            log.info("GridGain no-copy vector scorer engaged: contiguous off-heap mirror built for '" + name +
                "' (" + length + " bytes); further mirror builds are logged at FINE.");
        }
        else if (log.isLoggable(Level.FINE))
            log.fine("No-copy mirror built for '" + name + "' (" + length + " bytes).");

        return dst;
    }

    /**
     * Expert: allocate a new buffer. Subclasses can allocate differently.
     *
     * @return allocated buffer.
     */
    private long newBuffer() {
        return dir.memory().allocate(BUFFER_SIZE);
    }

    /**
     * Deletes file and deallocates memory.
     */
    public void delete() {
        if (!deleted.compareAndSet(false, true))
            return;

        deferredDelete();
    }

    /**
     * Deferred delete.
     */
    synchronized void deferredDelete() {
        if (!deleted.get() || hasRefs())
            return;

        assert refCnt.get() == 0;

        for (int i = 0; i < buffers.idx; i++)
            dir.memory().release(buffers.arr[i], BUFFER_SIZE);

        if (contiguousPtr != 0) {
            dir.memory().release(contiguousPtr, contiguousLen);

            contiguousPtr = 0;
        }

        buffers = null;
        dir.pendingDeletions.remove(name);
    }

    /**
     * @return Size in bytes.
     */
    long getSizeInBytes() {
        return sizeInBytes;
    }

    /**
     * @return Directory.
     */
    public GridLuceneDirectory getDirectory() {
        return dir;
    }

    /** {@inheritDoc} */
    @Override public long ramBytesUsed() {
        return sizeInBytes;
    }

    /** {@inheritDoc} */
    @Override public Collection<Accountable> getChildResources() {
        return Collections.emptyList();
    }

    /**
     * Simple expandable long[] wrapper.
     */
    private static class LongArray {
        /** */
        private long[] arr = new long[128];

        /** */
        private int idx;

        /**
         * @return Size.
         */
        int size() {
            return idx;
        }

        /**
         * Gets value by index.
         *
         * @param idx Index.
         * @return Value.
         */
        long get(int idx) {
            assert idx < this.idx;

            return arr[idx];
        }

        /**
         * Adds value.
         *
         * @param val Value.
         */
        void add(long val) {
            int len = arr.length;

            if (idx == len)
                arr = Arrays.copyOf(arr, Math.min(len * 2, len + 1024));

            arr[idx++] = val;
        }
    }
}
