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
import java.nio.channels.FileChannel;
import java.util.EnumSet;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.h2.value.Value;
import org.h2.value.ValueRow;

import static java.nio.file.StandardOpenOption.CREATE_NEW;
import static java.nio.file.StandardOpenOption.DELETE_ON_CLOSE;
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

    /** Index file directory. */
    private final String dir;

    /** Row store file directory. */
    private final String spillFileName;

    /** Current file id. It is incremented on each hashtable extension. */
    private int id;

    /** Index file. */
    private File idxFile;

    /** Index file channel. */
    private FileChannel idxFileCh;

    /** Row store. */
    private final SortedExternalResult rowStore;

    /** Reusable byte buffer for reading and writing. We use this only instance just for prevention GC pressure. */
    private final ByteBuffer reusableBuff = ByteBuffer.allocate(Entry.ENTRY_BYTES);

    /** Max HashMap size in entries - capacity. */
    private long cap;

    /** Total count of the stored entries. */
    private long entriesCnt;

    /**
     * @param spillFile File with rows.
     * @param rowStore External result being indexed by this hash index.
     * @param initSize Init hash map size.
     */
    ExternalResultHashIndex(File spillFile, SortedExternalResult rowStore, long initSize) {
        dir = spillFile.getParent();
        spillFileName = spillFile.getName();
        this.rowStore = rowStore;

        if (initSize <= MIN_CAPACITY)
            initSize = MIN_CAPACITY;

        long initCap = U.ceilPow2Long(initSize * 2);

        initNewIndexFile(initCap);
    }

    /**
     * Puts row into the hash index.
     *
     * @param key Row distinct key.
     * @param rowAddr Row address in the rows file.
     */
    public void put(ValueRow key, Long rowAddr) {
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
    public Long get(ValueRow key) {
        Entry entry = findEntry(key);

        return entry == null ? null : entry.rowAddress();
    }

    /**
     * Removes row from the hash index.
     *
     * @param key Row distinct key
     * @return Row address in the rows file.
     */
    public Long remove(ValueRow key) {
        Entry entry = findEntry(key);

        if (entry == null)
            return null;

        writeEntryToIndexFile(entry.slot(), key.hashCode(), -2); // row addr == -2 means removed row.

        entriesCnt--;

        return entry.rowAddress();
    }

    /** {@inheritDoc} */
    @Override public void close() throws Exception {
        U.closeQuiet(idxFileCh);
    }

    /**
     * Puts new index entry to the empty slot..
     *
     * @param hashCode Row key hashcode.
     * @param rowAddr Row address in the main row store.
     */
    private void putEntryToFreeSlot(int hashCode, Long rowAddr) {
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

        Entry entry;

        while ((entry = readEntryFromIndexFile(slot)) != null && !entry.isRemoved() && !entry.isEmpty()) {
            slot = slot((int)(slot + 1)); // Check next slot.

            if (slot == slot(hashCode))
                throw new RuntimeException("No free space left in the hash map.");
        }

        return slot;
    }

    /**
     * Ideal slot for the given hashcode: <code>slot = hashcode % capacity</code>.
     *
     * @param hashCode Hashcode.
     * @return Ideal slot.
     */
    private long slot(int hashCode) {
        return hashCode & (cap - 1);
    }

    /**
     * Looks for the row key in the index.
     * @param key Row key.
     * @return Entry row address.
     */
    private Entry findEntry(ValueRow key) {
        int hashCode = key.hashCode();
        long slot = slot(hashCode);

        Entry entry = readEntryFromIndexFile(slot);

        while (!entry.isEmpty()) {
            if (hashCode == entry.hashCode()) { // 1. Check hashcode equals.
                Value[] row = rowStore.readRowFromFile(entry.rowAddress());

                if (row != null) {
                    ValueRow keyFromDisk = rowStore.getRowKey(row);

                    if (key.equals(keyFromDisk)) // 2. Check the actual row equals the given key.
                        break;
                }
            }

            slot = slot((int)(slot + 1)); // Check the next slot.

            if (slot == slot(hashCode))
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
        return readEntryFromIndexFile(slot, idxFileCh);
    }

    /**
     * Reads hash map entry from the underlying file.
     *
     * @param slot number of the entry to read. {@code Null} means sequential reading.
     * @param fileCh File to read from.
     * @return Byte buffer with data.
     */
    private Entry readEntryFromIndexFile(Long slot, FileChannel fileCh) {
        try {
            if (slot != null)
                gotoSlot(slot);
            else
                slot = fileCh.position() / Entry.ENTRY_BYTES;

            reusableBuff.clear();

            while (reusableBuff.hasRemaining()) {
                int bytesRead = fileCh.read(reusableBuff);

                if (bytesRead <= 0)
                    throw new IOException("Can not read data from file: " + idxFile.getAbsolutePath());
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

            while (reusableBuff.hasRemaining()) {
                int bytesWritten = idxFileCh.write(reusableBuff);

                if (bytesWritten <= 0)
                    throw new IOException("Can not write data to file: " + idxFile.getAbsolutePath());
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
            idxFileCh.position(slot * Entry.ENTRY_BYTES);
        }
        catch (Exception e) {
            U.closeQuiet(idxFileCh);

            throw new IgniteException("Failed to reset the index spill file, slot=" + slot, e);
        }
    }

    /**
     * Checks if the capacity of the hashtable is enough and extends it if needed.
     */
    private void ensureCapacity() {
        if (entriesCnt > LOAD_FACTOR * cap) {
            FileChannel oldFile = idxFileCh;

            long oldSize = cap;

            initNewIndexFile(oldSize * 2);

            copyDataFromOldFile(oldFile, oldSize);
        }
    }

    /**
     * Copies data from the ols file to the extended new one.
     *
     * @param oldFile Old file channel.
     * @param oldSize Old file size in slots.
     */
    private void copyDataFromOldFile(FileChannel oldFile, long oldSize) {
        try {
            entriesCnt = 0;

            oldFile.position(0);

            for (long i = 0; i < oldSize; i++) {
                Entry e = readEntryFromIndexFile(null, oldFile);

                if (!e.isRemoved() && !e.isEmpty())
                    putEntryToFreeSlot(e.hashCode(), e.rowAddress());
            }
        }
        catch (IOException e) {
            U.closeQuiet(idxFileCh);
            U.closeQuiet(oldFile);

            throw new IgniteException("Failed to extend hash index.", e);
        }
        finally {
            U.closeQuiet(oldFile);
        }
    }

    /**
     * Initiates new file.
     *
     * @param cap Ne file capacity in entries (rows). Should be the power of 2.
     */
    private void initNewIndexFile(long cap) {
        try {
            assert cap > 0 && (cap & (cap - 1)) == 0; // Should be positive power of 2.

            this.cap = cap;

            idxFile = new File(dir, spillFileName + "_idx_" + id++);

            idxFileCh = FileChannel.open(idxFile.toPath(), EnumSet.of(CREATE_NEW, DELETE_ON_CLOSE, READ, WRITE));

            // Write empty data to the end of the file to extend it.
            reusableBuff.clear();
            idxFileCh.write(reusableBuff, cap * Entry.ENTRY_BYTES);
        }
        catch (IOException e) {
            throw new IgniteException("Failed to create an index spill file for the intermediate query results.", e);
        }
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
            return rowAddr;
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
            return rowAddr == -2;
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
