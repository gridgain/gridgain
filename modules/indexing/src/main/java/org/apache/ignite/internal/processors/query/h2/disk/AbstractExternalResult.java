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

/**
 * TODO: Add class description.
 */
@SuppressWarnings({"MissortedModifiers", "WeakerAccess", "ForLoopReplaceableByForEach"})
public abstract class AbstractExternalResult implements ResultExternal {
    private static final AtomicLong idGen = new AtomicLong();

    protected static final int INIT_BUF_SIZE = 512;

    protected static final int ROW_HEADER_SIZE = 8; // 2 x int

    protected int size;

    protected final FileChannel fileChannel;

    protected final File file;

    public AbstractExternalResult(GridKernalContext ctx) {
        try {
            String fileName = String.valueOf(idGen.incrementAndGet()); // TODO clean directory on node init

            file = new File(U.resolveWorkDirectory(
                ctx.config().getWorkDirectory(),
                "tmp/spill",
                false
            ), fileName);

            fileChannel = FileChannel.open(file.toPath(), EnumSet.of(CREATE_NEW,  DELETE_ON_CLOSE, READ, WRITE));
        }
        catch (IgniteCheckedException | IOException e) {
            throw new IgniteException("Failed to create a spill file for the intermediate query results.", e);
        }
    }


    protected Value[] readRowFromFile() {
        ByteBuffer hdr = readRowHeaderFromFile();

        int rowLen = hdr.getInt();
        int colCnt = hdr.getInt();

        if (colCnt == -1) {
            setFilePosition(currentFilePosition() + rowLen);

            return null; // Row has been deleted.
        }


        ByteBuffer rowBytes = readDataFromFile(rowLen);

        Data buff = Data.create(null, rowBytes.array(), true); // TODO handler and local time and initial size

        Value[] row = new Value[colCnt];

        for (int i = 0; i < colCnt; i++)
            row[i] = buff.readValue();

        return row;
    }

    @NotNull protected ByteBuffer readRowHeaderFromFile() {
        return readDataFromFile(ROW_HEADER_SIZE);
    }

    @NotNull protected ByteBuffer readDataFromFile(int rowLen) {
        try {
            ByteBuffer rowBytes = ByteBuffer.allocate(rowLen); // 2 x integer for length in bytes and columns count.

            // TODO while hasRemaining
            fileChannel.read(rowBytes);

            rowBytes.flip();

            return rowBytes;
        }
        catch (IOException e) {
            U.closeQuiet(fileChannel);

            throw new IgniteException("Failed to read query result the from spill file.", e);
        }
    }

    // TODO init size
    @NotNull protected Data createDataBuffer() {
        return Data.create(null, INIT_BUF_SIZE, true); // TODO handler and local time and initial size
    }

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

    protected int writeBufferToFile(Data buff) {
        try {
            buff.trim();

            // TODO while hasRemaining
            return fileChannel.write(ByteBuffer.wrap(buff.getBytes()));
        }
        catch (IOException e) {
            U.closeQuiet(fileChannel);

            throw new IgniteException("Failed to write intermediate query result to the spill file.", e);
        }
    }


    protected void markRowRemoved(long addr) {
        setFilePosition(addr);

        ByteBuffer hdr = readRowHeaderFromFile();

        hdr.getInt(); // Skip row length.

        hdr.putInt(-1); // Put tombstone: -1 on columns count position.

        hdr.flip();

        setFilePosition(addr);

        try {
            fileChannel.write(hdr);
        }
        catch (IOException e) {
            U.closeQuiet(fileChannel);

            throw new IgniteException("Failed to remove row from the intermediate query result in the spill file.", e);
        }
    }

    protected long currentFilePosition() {
        try {
            return fileChannel.position();
        }
        catch (IOException e) {
            U.closeQuiet(fileChannel);

            throw new IgniteException("Failed to access the spill file.", e);
        }
    }

    protected void rewindFile() {
        setFilePosition(0);
    }

    protected void setFilePosition(long pos) {
        try {
            fileChannel.position(pos);
        }
        catch (IOException e) {
            U.closeQuiet(fileChannel);

            throw new IgniteException("Failed to reset the spill file.", e);
        }
    }


    @Override public void close() {
        U.closeQuiet(fileChannel);
    }
}
