/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.internal.processors.query.h2.disk;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.query.h2.H2MemoryTracker;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.gridgain.internal.h2.api.ErrorCode;
import org.gridgain.internal.h2.message.DbException;
import org.gridgain.internal.h2.value.Value;
import org.gridgain.internal.h2.value.ValueRow;

import static java.nio.file.StandardOpenOption.CREATE_NEW;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;

/**
 * Disk-based hash index for the sorted external result.
 * Open addressing hashing scheme is used.
 */
@SuppressWarnings("TypeMayBeWeakened")
public class ExternalResultHashIndex implements AutoCloseable {
    /** Hash map load factor. */
    private static final double LOAD_FACTOR = 0.5;

    /** Minimum index capacity. */
    private static final long MIN_CAPACITY = 1L << 8; // 256 entries.

    /** */
    private static final long MAX_CAPACITY = 1L << 59 - 1;

    /** */
    private static final long REMOVED_FLAG = 1L << 63;

    /** */
    private final TrackableFileIoFactory fileIOFactory;

    /** Index file directory. */
    private final String dir;

    /** Row store file directory. */
    private final String spillFileName;

    /** Current file id. It is incremented on each hashtable extension. */
    private int id;

    /** Index file. */
    private File idxFile;

    /** Index file channel. */
    private FileIO fileIo;

    /** Row store. */
    private final ExternalResultData rowStore;

    /** Reusable byte buffer for reading and writing. We use this only instance just for prevention GC pressure. */
    private final ByteBuffer reusableBuff = ByteBuffer.allocate(Entry.ENTRY_BYTES);

    /** */
    private final H2MemoryTracker memTracker;

    /** Max HashMap size in entries - capacity. */
    private long cap;

    /** Total count of the stored entries. */
    private long entriesCnt;

    /** */
    private boolean closed;

    /**
     * @param fileIOFactory File
     * @param spillFile File with rows.
     * @param rowStore External result being indexed by this hash index.
     * @param initSize Init hash map size.
     * @param tracker Memory tracker.
     */
    ExternalResultHashIndex(
        TrackableFileIoFactory fileIOFactory,
        File spillFile,
        ExternalResultData rowStore,
        long initSize,
        H2MemoryTracker tracker) {
        this.fileIOFactory = fileIOFactory;
        dir = spillFile.getParent();
        spillFileName = spillFile.getName();
        this.rowStore = rowStore;
        this.memTracker = tracker;

        if (initSize <= MIN_CAPACITY)
            initSize = MIN_CAPACITY;

        // We need at least the half of hash map be empty to minimize collisions number.
        long initCap = Long.highestOneBit(initSize) * 4;

        initNewIndexFile(initCap);
    }

    /**
     * Shallow copy constructor.
     * @param parent Parent.
     */
    private ExternalResultHashIndex(ExternalResultHashIndex parent) {
        try {
            fileIOFactory = parent.fileIOFactory;
            idxFile = parent.idxFile;

            synchronized (this) {
                checkCancelled();

                fileIo = fileIOFactory.create(idxFile, parent.memTracker, READ);
            }

            rowStore = parent.rowStore;
            cap = parent.cap;
            entriesCnt = parent.entriesCnt;
            dir = parent.dir;
            spillFileName = parent.spillFileName;
            memTracker = parent.memTracker;
        }
        catch (IOException e) {
            throw new IgniteException("Failed to create new hash index.", e);
        }
    }

    /**
     * Puts row into the hash index.
     *
     * @param key Row distinct key.
     * @param rowAddr Row address in the rows file.
     */
    public void put(ValueRow key, long rowAddr) {
        assert key != null;

        ensureCapacity();

        int hashCode = key.hashCode();

        putEntryToFreeSlot(hashCode, rowAddr);
    }

    /**
     * Finds row address in the hash index by its key.
     *
     * @param key Row distinct key.
     * @return Row address in the rows file.
     */
    public long get(ValueRow key) {
        Entry entry = findEntry(key);

        return entry == null ? -1 : entry.rowAddress();
    }

    /**
     * Finds row address in the hash index by its key.
     *
     * @param key Row distinct key.
     * @return Whether row is in the file.
     */
    public boolean contains(ValueRow key) {
        Entry entry = findEntry(key);

        return entry != null;
    }

    /**
     * Removes row from the hash index.
     *
     * @param key Row distinct key
     * @return Row address in the rows file.
     */
    public long remove(ValueRow key) {
        Entry entry = findEntry(key);

        if (entry == null)
            return -1;

        writeEntryToIndexFile(entry.slot(), key.hashCode(),entry.rowAddress() | REMOVED_FLAG);

        entriesCnt--;

        return entry.rowAddress();
    }

    /**
     * Puts new index entry to the empty slot..
     *
     * @param hashCode Row key hashcode.
     * @param rowAddr Row address in the main row store.
     */
    private void putEntryToFreeSlot(int hashCode, long rowAddr) {
        long slot = findFreeSlotForInsert(hashCode);

        writeEntryToIndexFile(slot, hashCode, rowAddr);

        entriesCnt++;
    }

    /**
     * Finds free slot for the given hashcode.
     *
     * @param hashCode Hash code.
     * @return Free slot number.
     */
    private long findFreeSlotForInsert(int hashCode) {
        long slot = slot(hashCode);
        long startSlot = slot;
        Entry entry;

        while ((entry = readEntryFromIndexFile(slot)) != null && !entry.isRemoved() && !entry.isEmpty()) {
            slot = (slot + 1) % cap; // Check next slot.

            assert slot != startSlot;
        }

        return slot;
    }

    /**
     * Ideal slot for the given hashcode: <code>slot = hashcode % capacity</code>.
     *
     * @param hashCode Hashcode.
     * @return Ideal slot.
     */
    private long slot(long hashCode) {
        long hc64 = hashCode << 48 ^ hashCode << 32 ^ hashCode << 16 ^ hashCode;

        return hc64 & (cap - 1);
    }

    /**
     * Looks for the row key in the index.
     * @param key Row key.
     * @return Entry row address.
     */
    private Entry findEntry(ValueRow key) {
        int hashCode = key.hashCode();
        long slot = slot(hashCode);
        long initialSlot = slot;

        Entry entry = readEntryFromIndexFile(slot);

        while (!entry.isEmpty()) {
            if (!entry.isRemoved() && hashCode == entry.hashCode()) { // 1. Check hashcode equals.
                Map.Entry<ValueRow, Value[]> row = rowStore.readRowFromFile(entry.rowAddress());

                assert row != null : "row=" + row;

                ValueRow keyFromDisk = row.getKey();

                if (key.equals(keyFromDisk)) // 2. Check the actual row equals the given key.
                    break;
            }

            slot = (slot + 1) % cap; // Check the next slot.

            if (slot == initialSlot)
                return null;

            entry = readEntryFromIndexFile(slot);
        }

        return entry.isEmpty() || entry.isRemoved() ? null : entry;
    }

    /**
     * Reads hash map entry from the underlying file.
     *
     * @param slot number of the entry to read. {@code Null} means sequential reading.
     * @return Byte buffer with data.
     */
    private Entry readEntryFromIndexFile(long slot) {
        return readEntryFromIndexFile(slot, fileIo);
    }

    /**
     * Reads hash map entry from the underlying file.
     *
     * @param slot number of the entry to read. {@code -1} means sequential reading.
     * @param fileCh File to read from.
     * @return Byte buffer with data.
     */
    private Entry readEntryFromIndexFile(long slot, FileIO fileCh) {
        try {
            if (slot != -1)
                gotoSlot(slot);
            else
                slot = fileCh.position() / Entry.ENTRY_BYTES;

            reusableBuff.clear();

            synchronized (this) {
                checkCancelled();

                fileCh.readFully(reusableBuff);
            }

            reusableBuff.flip();

            int hashCode = reusableBuff.getInt();

            // We store addr value incremented by 1 to distinguish 0 address from the empty address field.
            // On the disk 0 means empty, 1 means zero, y means y-1.
            long addr = reusableBuff.getLong() - 1;

            return new Entry(hashCode, addr, slot);
        }
        catch (IOException e) {
            U.closeQuiet(this);

            throw new IgniteException("Failed to read query result the from spill idx file. [slot=" + slot + ']', e);
        }
    }

    /**
     * Writes hash map entry to file.
     *
     * @param slot Entry slot.
     * @param hashCode Hash code.
     * @param addr Row address.
     */
    private void writeEntryToIndexFile(long slot, int hashCode, long addr) {
        try {
            gotoSlot(slot);

            reusableBuff.clear();
            reusableBuff.putInt(hashCode);

            // We store addr value incremented by 1 to distinguish 0 address from the empty address field.
            // On the disk 0 means empty, 1 means zero, y means y-1.
            reusableBuff.putLong(addr + 1);
            reusableBuff.flip();

            synchronized (this) {
                checkCancelled();

                fileIo.writeFully(reusableBuff);
            }
        }
        catch (IOException e) {
            U.closeQuiet(this);

            throw new IgniteException("Failed to write intermediate query result to the spill file.", e);
        }
    }

    /**
     * Sets arbitrary file position.
     *
     * @param slot Position to set.
     */
    private void gotoSlot(long slot) {
        try {
            synchronized (this) {
                checkCancelled();

                fileIo.position(slot * Entry.ENTRY_BYTES);
            }
        }
        catch (Exception e) {
            U.closeQuiet(this);

            throw new IgniteException("Failed to reset the index spill file, slot=" + slot, e);
        }
    }

    /**
     * Checks if the capacity of the hashtable is enough and extends it if needed.
     */
    private void ensureCapacity() {
        if (entriesCnt <= LOAD_FACTOR * cap)
            return;

        FileIO oldFileIo = fileIo;
        File oldIdxFile = idxFile;

        long oldSize = cap;

        try {
            initNewIndexFile(oldSize * 2);

            copyDataFromOldFile(oldFileIo, oldIdxFile, oldSize);
        }
        finally {
            U.closeQuiet(oldFileIo);
            oldIdxFile.delete();
        }
    }

    /**
     * Copies data from the old file to the extended new one.
     *
     * @param oldFile Old file channel.
     * @param oldIdxFile Old index file.
     * @param oldSize Old file size in slots.
     */
    private void copyDataFromOldFile(FileIO oldFile, File oldIdxFile, long oldSize) {
        try {
            entriesCnt = 0;

            oldFile.position(0);

            for (long i = 0; i < oldSize; i++) {
                Entry e = readEntryFromIndexFile(-1, oldFile);

                if (!e.isRemoved() && !e.isEmpty())
                    putEntryToFreeSlot(e.hashCode(), e.rowAddress());
            }
        }
        catch (IOException e) {
            U.closeQuiet(this);
            U.closeQuiet(oldFile);

            throw new IgniteException("Failed to extend hash index.", e);
        }
    }

    /**
     * Initiates new file.
     *
     * @param cap New file capacity in entries (rows). Should be the power of 2.
     */
    private void initNewIndexFile(long cap) {
        try {
            assert cap > 0 && (cap & (cap - 1)) == 0 : "cap=" + cap; // Should be the positive power of 2.

            if (cap > MAX_CAPACITY) {
                throw new IllegalArgumentException("Maximum capacity is exceeded [curCapacity=" + cap +
                    ", maxCapacity=" + MAX_CAPACITY + ']');
            }

            this.cap = cap;

            idxFile = new File(dir, spillFileName + "_idx_" + id++);

            idxFile.deleteOnExit();

            synchronized (this) {
                checkCancelled();

                fileIo = fileIOFactory.create(idxFile, memTracker, CREATE_NEW, READ, WRITE);

                // Write empty data to the end of the file to extend it.
                reusableBuff.clear();
                fileIo.write(reusableBuff, cap * Entry.ENTRY_BYTES);
            }
        }
        catch (IOException e) {
            U.closeQuiet(this);

            throw new IgniteException("Failed to create an index spill file for the intermediate query results.", e);
        }
    }

    /**
     * Checks if statement was closed (
     */
    private synchronized void checkCancelled() {
        if (closed)
            throw DbException.get(ErrorCode.STATEMENT_WAS_CANCELED);
    }

    /** {@inheritDoc} */
    @Override public void close() throws Exception {
        synchronized (this) {
            if (closed)
                return;

            U.closeQuiet(fileIo);

            closed = true;
        }

        idxFile.delete();
    }

    /**
     * @return Shallow copy.
     */
    ExternalResultHashIndex createShallowCopy() {
        return new ExternalResultHashIndex(this);
    }

    /**
     * Hash index entry.
     */
    private static class Entry {
        /** Size of the has index entry in bytes. */
        static final int ENTRY_BYTES = Integer.BYTES + Long.BYTES;

        /** Hash code. */
        private final int hashCode;

        /** Address of the row in the rows file. */
        private final long rowAddr;

        /** Entry slot number. */
        private final long slot;

        /**
         * @param hashCode Hash code.
         * @param rowAddr Address of the row in the rows file.
         * @param slot Entry slot number.
         */
        Entry(int hashCode, long rowAddr, long slot) {
            this.hashCode = hashCode;
            this.rowAddr = rowAddr;
            this.slot = slot;
        }

        /**
         * @return Hash code.
         */
        public int hashCode() {
            return hashCode;
        }

        /**
         * @return Address of the row in the rows file.
         */
        public long rowAddress() {
            return rowAddr & ~REMOVED_FLAG;
        }

        /**
         * @return Entry slot number.
         */
        public long slot() {
            return slot;
        }

        /**
         * @return Removed flag.
         */
        public boolean isRemoved() {
            return (rowAddr & REMOVED_FLAG) != 0L;
        }

        /**
         * @return Empty flag.
         */
        public boolean isEmpty() {
            return rowAddr == -1;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(Entry.class, this);
        }
    }
}
