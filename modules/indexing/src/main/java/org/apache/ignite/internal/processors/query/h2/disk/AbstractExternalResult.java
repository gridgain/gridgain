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
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.query.h2.H2MemoryTracker;
import org.apache.ignite.internal.processors.query.h2.H2Utils;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.h2.result.ResultExternal;
import org.h2.store.Data;
import org.h2.value.Value;
import org.jetbrains.annotations.NotNull;

import static java.nio.file.StandardOpenOption.CREATE_NEW;
import static java.nio.file.StandardOpenOption.DELETE_ON_CLOSE;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;
import static org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing.DISK_SPILL_DIR;

/**
 * Basic class for external result. Contains common methods for file IO.
 */
@SuppressWarnings({"MissortedModifiers", "WeakerAccess", "ForLoopReplaceableByForEach"})
public abstract class AbstractExternalResult implements ResultExternal {
    /** File name generator. */
    private static final AtomicLong idGen = new AtomicLong();

    /** Initial rows byte buffer size. */
    protected static final int INIT_BUF_SIZE = 512;

    /** Serialized row header size. */
    protected static final int ROW_HEADER_SIZE = 8; // 2 x int: row length in bytes + column count.

    /** Current size in rows. */
    protected int size;

    /** File channel. */
    protected final FileChannel fileCh;

    /** File. */
    protected final File file;

    /** Memory tracker. */
    protected final H2MemoryTracker memTracker;

    /**
     * @param ctx Kernal context.
     * @param memTracker Memory tracker
     */
    protected AbstractExternalResult(GridKernalContext ctx, H2MemoryTracker memTracker) {
        try {
            this.memTracker = memTracker;

            String fileName = String.valueOf(idGen.incrementAndGet());

            file = new File(U.resolveWorkDirectory(
                ctx.config().getWorkDirectory(),
                DISK_SPILL_DIR,
                false
            ), fileName);

            fileCh = FileChannel.open(file.toPath(), EnumSet.of(CREATE_NEW,  DELETE_ON_CLOSE, READ, WRITE));
        }
        catch (IgniteCheckedException | IOException e) {
            throw new IgniteException("Failed to create a spill file for the intermediate query results.", e);
        }
    }

    /**
     * Reads full row from file starting from the current position.
     * @return Row.
     */
    protected Value[] readRowFromFile() {
        ByteBuffer hdr = readRowHeaderFromFile();

        int rowLen = hdr.getInt();
        int colCnt = hdr.getInt();

        if (colCnt == -1) {
            setFilePosition(currentFilePosition() + rowLen);

            return null; // Row has been deleted.
        }

        ByteBuffer rowBytes = readDataFromFile(rowLen);

        Data buff = Data.create(null, rowBytes.array(), true);

        Value[] row = new Value[colCnt];

        for (int i = 0; i < colCnt; i++)
            row[i] = buff.readValue();

        return row;
    }

    /**
     * Reads row header starting from the current position.
     *
     * @return Buffer with row header
     */
    @NotNull protected ByteBuffer readRowHeaderFromFile() {
        return readDataFromFile(ROW_HEADER_SIZE);
    }

    /**
     * Reads data from file.
     *
     * @param rowLen Data length to read.
     * @return Byte buffer with data.
     */
    @NotNull protected ByteBuffer readDataFromFile(int rowLen) {
        try {
            ByteBuffer rowBytes = ByteBuffer.allocate(rowLen); // 2 x integer for length in bytes and columns count.

            while (rowBytes.hasRemaining()) {
                int bytesRead = fileCh.read(rowBytes);

                if (bytesRead <= 0)
                    throw new IOException("Can not read data from file: " + file.getAbsolutePath());
            }

            rowBytes.flip();

            return rowBytes;
        }
        catch (IOException e) {
            close();

            throw new IgniteException("Failed to read query result the from spill file.", e);
        }
    }

    /**
     * @return New data buffer.
     */
    @NotNull protected Data createDataBuffer() {
        return Data.create(null, INIT_BUF_SIZE, true); // TODO init size
    }

    /**
     * Adds row to buffer.
     *
     * @param row Row.
     * @param buff Buffer.
     */
    protected void addRowToBuffer(Value[] row, Data buff) {
        int initPos = buff.length();

        buff.checkCapacity((int)(ROW_HEADER_SIZE + H2Utils.rowSizeInBytes(row) * 2));

        buff.writeInt(0); // Skip int position for row length in bytes.
        buff.writeInt(row.length); // Skip int position for columns count.

        for (int i = 0; i < row.length; i++)
            buff.writeValue(row[i]);

        int len = buff.length() - initPos - ROW_HEADER_SIZE;

        buff.setInt(initPos, len);
    }

    /**
     * Writes buffer to file.
     *
     * @param buff Buffer.
     * @return Bytes written.
     */
    protected int writeBufferToFile(Data buff) {
        try {
            ByteBuffer byteBuff = ByteBuffer.wrap(buff.getBytes());

            byteBuff.limit(buff.length());

            while (byteBuff.hasRemaining()) {
                int bytesWritten = fileCh.write(byteBuff);

                if (bytesWritten <= 0)
                    throw new IOException("Can not write data to file: " + file.getAbsolutePath());
            }

            return byteBuff.limit();
        }
        catch (IOException e) {
            close();

            throw new IgniteException("Failed to write intermediate query result to the spill file.", e);
        }
    }

    /**
     * Marks row in the file as removed by setting {@code -1} on it's {@code colCount} position.
     *
     * @param addr Row absolute address in the file.
     */
    protected void markRowRemoved(long addr) {
        setFilePosition(addr);

        ByteBuffer hdr = readRowHeaderFromFile();

        hdr.getInt(); // Skip row length.

        hdr.putInt(-1); // Put tombstone: -1 on columns count position.

        hdr.flip();

        setFilePosition(addr);

        try {
            while (hdr.hasRemaining()) {
                int bytesWritten = fileCh.write(hdr);

                if (bytesWritten <= 0)
                    throw new IOException("Can not write data to file: " + file.getAbsolutePath());
            }
        }
        catch (IOException e) {
            close();

            throw new IgniteException("Failed to remove row from the intermediate query result in the spill file.", e);
        }
    }

    /**
     * @return Current absolute position in the file.
     */
    protected long currentFilePosition() {
        try {
            return fileCh.position();
        }
        catch (IOException e) {
            close();

            throw new IgniteException("Failed to access the spill file.", e);
        }
    }

    /**
     * Sets position in file on the beginning.
     */
    protected void rewindFile() {
        setFilePosition(0);
    }

    /**
     * Sets arbitrary file position.
     *
     * @param pos Position to set.
     */
    protected void setFilePosition(long pos) {
        try {
            fileCh.position(pos);
        }
        catch (IOException e) {
            close();

            throw new IgniteException("Failed to reset the spill file.", e);
        }
    }

    /**
     * Closes the file.
     */
    @Override public void close() {
        U.closeQuiet(fileCh);
    }
}
