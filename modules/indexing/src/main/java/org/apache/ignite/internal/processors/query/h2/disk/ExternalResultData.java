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
import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.query.h2.H2MemoryTracker;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.h2.api.ErrorCode;
import org.h2.message.DbException;
import org.h2.store.Data;
import org.h2.store.DataHandler;
import org.h2.value.CompareMode;
import org.h2.value.Value;
import org.h2.value.ValueNull;
import org.h2.value.ValueRow;

import static java.nio.file.StandardOpenOption.CREATE_NEW;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;
import static org.apache.ignite.internal.processors.query.h2.QueryMemoryManager.DISK_SPILL_DIR;

/**
 * Spill file IO.
 */
@SuppressWarnings("ForLoopReplaceableByForEach")
public class ExternalResultData<T> implements AutoCloseable {
    /** File name generator. */
    private static final AtomicLong idGen = new AtomicLong();

    /** Serialized row header size. */
    private static final int ROW_HEADER_SIZE = Integer.BYTES * 2; // Row length in bytes + column count.

    /** Default row size in bytes. */
    private static final int DEFAULT_ROW_SIZE = 512;

    /** */
    private static final int TOMBSTONE = -1;

    /** */
    private static final int TOMBSTONE_OFFSET = Integer.BYTES;

    /** */
    private static final byte[] TOMBSTONE_BYTES = ByteBuffer.allocate(4).putInt(-1).array(); // integer -1.

    /** */
    private final Class<T> cls;

    /** Logger. */
    private final IgniteLogger log;

    /** File. */
    private final File file;

    /** Spill file IO. */
    private final FileIO fileIo;

    /** IO factory. */
    private final TrackableFileIoFactory fileIOFactory;

    /** Hash index for fast lookup of the distinct rows. RowKey -> Row address in the row store. */
    private final ExternalResultHashIndex hashIdx;

    /** */
    private long lastWrittenPos;

    /** Reusable byte buffer for reading and writing. We use this only instance just for prevention GC pressure. */
    private final Data writeBuff;

    /** Sorted chunks addresses on the disk. */
    private final Collection<Chunk> chunks = new ArrayList<>();

    /** Reusable header buffer. */
    private final ByteBuffer headReadBuff = ByteBuffer.allocate(ROW_HEADER_SIZE);

    /** Compare mode for reading aggregates. */
    private final CompareMode cmp;

    /** H2 data handler. */
    private final DataHandler hnd;

    /** Data buffer. */
    private ByteBuffer readBuff;

    private final H2MemoryTracker tracker;

    /** */
    private boolean closed;

    /**
     * @param log Logger.
     * @param workDir Work directory.
     * @param fileIOFactory File io factory.
     * @param locNodeId Node id.
     * @param useHashIdx Whether to use hash index.
     * @param initSize Initial size.
     * @param cls Class of stored data.
     * @param cmp Comparator for rows.
     * @param hnd Data handler.
     * @param tracker Memory tracker.
     */
    public ExternalResultData(IgniteLogger log,
        String workDir,
        TrackableFileIoFactory fileIOFactory,
        UUID locNodeId,
        boolean useHashIdx,
        long initSize,
        Class<T> cls,
        CompareMode cmp,
        DataHandler hnd,
        H2MemoryTracker tracker) {
        this.log = log;
        this.cls = cls;
        this.cmp = cmp;
        this.fileIOFactory = fileIOFactory;
        this.tracker = tracker;

        String fileName = "spill_" + locNodeId + "_" + idGen.incrementAndGet();
        try {
            file = new File(U.resolveWorkDirectory(
                workDir,
                DISK_SPILL_DIR,
                false
            ), fileName);

            synchronized (this) {
                checkCancelled();

                fileIo = fileIOFactory.create(file, tracker, CREATE_NEW, READ, WRITE);
            }

            if (log.isDebugEnabled())
                log.debug("Created spill file " + file.getName());

            this.hnd = hnd;

            writeBuff = Data.create(hnd, DEFAULT_ROW_SIZE, false);

            hashIdx = useHashIdx ?
                new ExternalResultHashIndex(fileIOFactory, file, this, initSize, tracker) : null;
        }
        catch (IgniteCheckedException | IgniteException | IOException e) {
            U.closeQuiet(this);

            throw new IgniteException(e);
        }
    }

    /**
     * @param parent Parent external data.
     */
    @SuppressWarnings("CopyConstructorMissesField")
    private ExternalResultData(ExternalResultData<T> parent) {
        try {
            log = parent.log;
            cmp = parent.cmp;
            cls = parent.cls;
            file = parent.file;
            fileIOFactory = parent.fileIOFactory;
            tracker = parent.tracker;

            synchronized (this) {
                checkCancelled();

                fileIo = fileIOFactory.create(file, tracker, READ);
            }

            writeBuff = parent.writeBuff;
            hnd = parent.hnd;
            hashIdx = parent.hashIdx != null ? parent.hashIdx.createShallowCopy() : null;
        }
        catch (IOException e) {
            throw new IgniteException("Failed to create external result data.", e);
        }
    }

    /**
     * Stores rows into the file.
     *
     * @param rows Rows to store.
     */
    public void store(Collection<Map.Entry<ValueRow, T[]>> rows) {
        long initFilePos = lastWrittenPos;

        setFilePosition(lastWrittenPos);

        for (Map.Entry<ValueRow, T[]> row : rows)
            writeToFile(row);

        chunks.add(new Chunk(initFilePos, lastWrittenPos));
    }

    /**
     * Writes row to file.
     *
     * @param row Row.
     */
    private void writeToFile(Map.Entry<ValueRow, T[]> row) {
        writeBuff.reset();

        Value rowKey = row.getKey() == null ? ValueNull.INSTANCE : row.getKey();
        T[] rowVal = row.getValue();

        assert rowVal != null;

        int valLen = nonNullsLength(rowVal);

        // 1. Write header.
        writeBuff.checkCapacity(ROW_HEADER_SIZE);
        writeBuff.writeInt(0); // Skip int position for row length in bytes.
        writeBuff.writeInt(valLen + 1); // Skip int position for columns count.

        // 2. Write row key.
        writeBuff.checkCapacity(writeBuff.getValueLen(rowKey));
        writeBuff.writeValue(rowKey);

        // 3. Write row value.
        int len = 0;

        for (int i = 0; i < valLen; i++)
            len += writeBuff.getValueLen(rowVal[i]);

        writeBuff.checkCapacity(len);

        for (int i = 0; i < valLen; i++)
            writeBuff.writeValue(rowVal[i]);

        writeBuff.setInt(0, writeBuff.length() - ROW_HEADER_SIZE);

        writeToFile(writeBuff);

        if (hashIdx != null)
            hashIdx.put(row.getKey(), lastWrittenPos);

        lastWrittenPos = currentFilePosition();
    }

    /**
     * @param row Row.
     * @return Length of non-null values.
     */
    private int nonNullsLength(T[] row) {
        for (int i = 0; i < row.length; i++) {
            if (row[i] == null)
                return i;
        }

        return row.length;
    }

    /**
     * Marks row as removed.
     *
     * @param row Row.
     * @return {@code True} if row was actually removed.
     */
    public boolean remove(ValueRow row) {
        assert hashIdx != null;
        assert row != null;

        long addr = hashIdx.remove(row);

        if (addr < 0)
            return false;

        markRemoved(addr);

        return true;
    }

    /**
     * Updates row header as removed.
     *
     * @param addr Row address.
     */
    private synchronized void markRemoved(long addr) {
        try {
            checkCancelled();

            fileIo.position(addr + TOMBSTONE_OFFSET); // Skip total length and go to the column count.
            fileIo.write(TOMBSTONE_BYTES, 0, TOMBSTONE_BYTES.length);
        }
        catch (IOException e) {
            close();

            throw new IgniteException("Failed to write tombstone to the spill file.", e);
        }
    }

    /**
     * Checks if row is contained by the file.
     *
     * @param row Row.
     * @return {@code True} if file contains the row.
     */
    public boolean contains(ValueRow row) {
        assert hashIdx != null;
        assert row != null;

        return hashIdx.contains(row);
    }

    /**
     * Reads key from the file by its key.
     *
     * @param key Row key.
     * @return Row.
     */
    public Map.Entry<ValueRow, T[]> get(ValueRow key) {
        assert hashIdx != null;
        assert key != null;

        long addr = hashIdx.get(key);

        if (addr < 0)
            return null;

        return readRowFromFile(addr);
    }

    /**
     * @return Next row.
     */
    Map.Entry<ValueRow, T[]> readRowFromFile() {
        return readRowFromFile(currentFilePosition());
    }

    /**
     * Reads row from the given position.
     *
     * @param pos Row position.
     * @return Row.
     */
    Map.Entry<ValueRow, T[]> readRowFromFile(long pos) {
        // 1. Read header.
        setFilePosition(pos);

        headReadBuff.clear();

        readFromFile(headReadBuff);

        headReadBuff.flip();

        int size = headReadBuff.getInt();
        int colCnt = headReadBuff.getInt();

        if (colCnt == TOMBSTONE) {
            setFilePosition(pos + size + ROW_HEADER_SIZE); // Skip row.

            return null;
        }

        // 2. Read body.
        // Preallocate double row size to minimize possible allocations for other rows.
        if (readBuff == null || readBuff.capacity() < size)
            readBuff = ByteBuffer.allocate(size * 2);

        readBuff.clear();
        readBuff.limit(size);

        readFromFile(readBuff);

        readBuff.flip();

        Data buff = Data.create(hnd, readBuff.array(), true);

        buff.setCompareMode(cmp);

        IgniteBiTuple<ValueRow, T[]> row = new IgniteBiTuple<>();

        Value rowKey = (Value)buff.readValue();

        if (rowKey == ValueNull.INSTANCE)
            row.set1(null);
        else
            row.set1((ValueRow)rowKey);

        T[] rowVal = (T[]) Array.newInstance(cls, colCnt - 1);

        for (int i = 0; i < colCnt - 1; i++)
            rowVal[i] = (T) buff.readValue();

        row.set2(rowVal);

        return row;
    }

    /**
     * Reads data from file into the buffer.
     *
     * @param buff Buffer.
     */
    private synchronized void readFromFile(ByteBuffer buff) {
        try {
            checkCancelled();

            fileIo.readFully(buff);
        }
        catch (IOException e) {
            close();

            throw new IgniteException("Failed to write intermediate query result to the spill file.", e);
        }
    }

    /**
     * Writes buffer to file.
     *
     * @param buff Buffer.
     * @return Bytes written.
     */
    private synchronized int writeToFile(Data buff) {
        try {
            checkCancelled();

            ByteBuffer byteBuff = ByteBuffer.wrap(buff.getBytes());

            byteBuff.limit(buff.length());

            fileIo.writeFully(byteBuff);

            return byteBuff.limit();
        }
        catch (IOException e) {
            close();

            throw new IgniteException("Failed to write intermediate query result to the spill file.", e);
        }
    }

    /**
     * Sets arbitrary file position.
     *
     * @param pos Position to set.
     */
    private synchronized void setFilePosition(long pos) {
        try {
            checkCancelled();

            fileIo.position(pos);
        }
        catch (IOException e) {
            close();

            throw new IgniteException("Failed to reset the spill file.", e);
        }
    }

    /**
     * @return Current absolute position in the file.
     */
    private synchronized long currentFilePosition() {
        try {
            checkCancelled();

            return fileIo.position();
        }
        catch (IOException e) {
            close();

            throw new IgniteException("Failed to access the spill file.", e);
        }
    }

    /**
     * Sets position in file on the beginning.
     */
    void rewindFile() {
        setFilePosition(0);
    }

    /**
     * @return File chunks.
     */
    Collection<Chunk> chunks() {
        return chunks;
    }

    /**
     * Checks if statement was closed (i.e. concurrently cancelled).
     */
    private void checkCancelled() {
        if (closed)
            throw DbException.get(ErrorCode.STATEMENT_WAS_CANCELED);
    }

    /** {@inheritDoc} */
    @Override public void close() {
        synchronized (this) {
            if (closed)
                return;

            U.closeQuiet(fileIo);

            closed = true;
        }

        U.closeQuiet(hashIdx);

        file.delete();

        if (log.isDebugEnabled())
            log.debug("Deleted spill file " + file.getName());
    }

    /**
     * @return Shallow copy.
     */
    ExternalResultData createShallowCopy() {
        return new ExternalResultData<>(this);
    }

    /**
     * Sorted rows chunk on the disk.
     */
    class Chunk {
        /** Start chunk position. */
        private final long start;

        /** End chunk position. */
        private final long end;

        /** Current position within the chunk */
        private long curPos;

        /** Current row. */
        private Map.Entry<ValueRow, T[]> curRow;

        /**
         * @param start Start position.
         * @param end End position.
         */
        Chunk(long start, long end) {
            this.start = start;
            this.curPos = start;
            this.end = end;
        }

        /**
         * @return {@code True} if next row is available within a chunk.
         */
        boolean next() {
            while (curPos < end) {
                curRow = readRowFromFile(curPos);

                curPos = currentFilePosition();

                if (curRow != null)
                    return true;
            }

            return false;
        }

        /**
         * Resets position in a chunk to the begin.
         */
        void reset() {
            curPos = start;
            curRow = null;
        }

        /**
         * @return Current row.
         */
        Map.Entry<ValueRow, T[]> currentRow() {
            return curRow;
        }

        /**
         * @return Chunk start position.
         */
        public long start() {
            return start;
        }
    }
}
