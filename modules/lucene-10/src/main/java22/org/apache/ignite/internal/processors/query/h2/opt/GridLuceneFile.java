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
 * Lucene file. This java22 overlay stores a closed file's content <b>contiguously</b>: the file is written
 * into the usual append-friendly {@link GridLuceneOutputStream#BUFFER_SIZE} page chain (page addresses must
 * not move under the writer), and {@link #onOutputClosed()} then compacts the pages into one exact-size
 * off-heap block and frees them. {@link #contiguousAddr()} exposes that block, letting the no-copy
 * MemorySegment vector scorer read vectors in place — with <b>no mirror copy and no memory doubling</b>
 * (storage even shrinks from page-rounded to exact size). Compacting at output close is safe because the
 * Lucene {@code Directory} contract forbids opening an input before the output is closed, and the file is
 * immutable afterwards.
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
     * Kill switch for the no-copy vector scorer (default on). When off, files keep the base page-chain
     * layout ({@link #onOutputClosed()} does not compact), {@link #contiguousAddr()} returns 0 and Lucene
     * falls back to its copy-based vector scorer — restoring the exact pre-overlay behavior to isolate
     * regressions.
     */
    private static final boolean NOCOPY_SCORER =
        Boolean.parseBoolean(System.getProperty("GRIDGAIN_VECTOR_NOCOPY_SCORER", "true"));

    /** JUL, matching Lucene's own vectorization-status logging — this module has no Ignite logger wiring. */
    private static final Logger log = Logger.getLogger(GridLuceneFile.class.getName());

    static {
        // Whether the no-copy scorer can engage is otherwise invisible in logs (an MR-jar-vs-exploded
        // classpath mix-up or this switch silently fall back to the copy scorer), so announce the off state.
        if (!NOCOPY_SCORER) {
            log.info("GridGain no-copy vector scorer disabled via -DGRIDGAIN_VECTOR_NOCOPY_SCORER=false; " +
                "file storage stays paged and Lucene will use its copy-based vector scorer.");
        }
    }

    /** Base address of the file's contiguous storage (0 = still paged: open for write, empty, or switch off). */
    private volatile long contiguousPtr;

    /** Exact size of the contiguous allocation (== file length at compaction time), for release on delete. */
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
     * Invoked by {@link GridLuceneOutputStream#close()} once the file's content is complete (and before
     * any input can legally be opened on it): compacts the file into contiguous storage, unless the
     * kill switch is off. Replaces the base copy's no-op (this overlay shadows the base class in the
     * multi-release jar; it does not subclass it).
     */
    void onOutputClosed() {
        if (NOCOPY_SCORER)
            compact();
    }

    /**
     * Re-lays the closed file's pages into one exact-size contiguous off-heap block, repoints the page
     * table into it ({@code getBuffer(i) == base + i * BUFFER_SIZE}, so the sequential read path is
     * untouched) and frees the pages. No reader can hold a stale page address: the Directory contract
     * forbids opening an input before the output is closed, and the writer's cached page address is dead
     * after {@code close()}. Total off-heap for the file goes page-rounded → exact, and the no-copy scorer
     * needs no mirror copy at query time.
     */
    private synchronized void compact() {
        if (contiguousPtr != 0 || deleted.get() || length <= 0)
            return;

        int nb = buffers.size();
        long dst = dir.memory().allocate(length);
        long copied = 0;

        for (int i = 0; i < nb && copied < length; i++) {
            long chunk = Math.min(BUFFER_SIZE, length - copied);

            dir.memory().copyMemory(buffers.get(i), dst + copied, chunk);

            copied += chunk;
        }

        for (int i = 0; i < nb; i++) {
            long page = buffers.get(i);

            buffers.set(i, dst + (long)i * BUFFER_SIZE);

            dir.memory().release(page, BUFFER_SIZE);
        }

        contiguousLen = length;
        contiguousPtr = dst;

        // Storage shrank from page-rounded to exact size.
        long delta = length - (long)nb * BUFFER_SIZE;

        sizeInBytes += delta;

        if (dir != null)
            dir.sizeInBytes.getAndAdd(delta);

        if (log.isLoggable(Level.FINE))
            log.fine("Compacted '" + name + "' to contiguous off-heap storage (" + length + " bytes, " + nb + " pages freed).");
    }

    /**
     * @return New buffer address.
     */
    final long addBuffer() {
        long buf = newBuffer();

        synchronized (this) {
            assert contiguousPtr == 0 : "append after compaction: " + name;

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
     * Address of the file's contiguous off-heap storage, laid down by {@link #onOutputClosed()} when the
     * output was closed. Lucene's memory-segment vector scorer requests the whole input as one segment,
     * which only a single contiguous region can satisfy; since the storage itself is contiguous, no mirror
     * copy is ever made. Returns {@code 0} when unavailable (file still open for write, empty, or the
     * {@code GRIDGAIN_VECTOR_NOCOPY_SCORER} switch is off) — Lucene then falls back to its copy path.
     * Freed in {@link #deferredDelete()}. The sequential read path is unaffected either way.
     *
     * @return Contiguous storage address, or {@code 0}.
     */
    final long contiguousAddr() {
        return contiguousPtr;
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

        if (contiguousPtr != 0) {
            // The pages were re-laid into (and freed in favor of) the contiguous block: the page table
            // holds derived addresses inside it, so free only the block itself.
            dir.memory().release(contiguousPtr, contiguousLen);

            contiguousPtr = 0;
        }
        else {
            for (int i = 0; i < buffers.idx; i++)
                dir.memory().release(buffers.arr[i], BUFFER_SIZE);
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
         * Replaces value by index.
         *
         * @param idx Index.
         * @param val Value.
         */
        void set(int idx, long val) {
            assert idx < this.idx;

            arr[idx] = val;
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
