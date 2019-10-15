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

import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.query.h2.H2MemoryTracker;
import org.h2.result.ResultExternal;
import org.h2.result.ResultInterface;

/**
 * Basic class for external result. Contains common methods for file IO.
 */
@SuppressWarnings({"MissortedModifiers", "WeakerAccess", "ForLoopReplaceableByForEach"})
public abstract class AbstractExternalResult implements ResultExternal {

    /** Logger. */
    protected final IgniteLogger log;

    /** Current size in rows. */
    protected int size;

    /** Memory tracker. */
    protected final H2MemoryTracker memTracker;

    /** Parent result. */
    protected final AbstractExternalResult parent;

    /** Child results count. Parent result is closed only when all children are closed. */
    private int childCnt;

    /** */
    private boolean closed;

    protected final ExternalResultData data;

    /**
     * @param ctx Kernal context.
     * @param memTracker Memory tracker
     */
    protected AbstractExternalResult(GridKernalContext ctx, H2MemoryTracker memTracker, boolean useHashIdx, long initSize) {
        this.log = ctx.log(AbstractExternalResult.class);
        this.data = new ExternalResultData(log, ctx.config().getWorkDirectory(), ctx.query().fileIOFactory(),
            ctx.localNodeId(), useHashIdx, initSize);
        this.parent = null;
        this.memTracker = memTracker;

        if (memTracker != null)
            memTracker.registerCloseListener(this::close);
    }

    /**
     * Used for {@link ResultInterface#createShallowCopy(org.h2.engine.SessionInterface)} only.
     * @param parent Parent result.
     */
    protected AbstractExternalResult(AbstractExternalResult parent) {
        log = parent.log;
        size = parent.size;
        data = parent.data;// TODO create new over the same file
        this.parent = parent;
        memTracker = parent.memTracker;

        if (memTracker != null)
            memTracker.registerCloseListener(this::close); // TODO remove this
    }


    /**
     * @return {@code True} if it is need to spill rows to disk.
     */
    protected boolean needToSpill() {
        return !memTracker.reserved(0);
    }
//
//    /**
//     * Reads full row from file starting from the given position.
//     * @param addr Given position.
//     *
//     * @return Row.
//     */
//    protected Value[] readRowFromFile(long addr) {
//        setFilePosition(addr);
//
//        return readRowFromFile();
//    }

//    /**
//     * Reads full row from file starting from the current position.
//     *
//     * @return Row.
//     */
//    protected Value[] readRowFromFile() {
//        ByteBuffer hdr = readRowHeaderFromFile();
//
//        int rowLen = hdr.getInt();
//        int colCnt = hdr.getInt();
//
//        if (colCnt == -1) {
//            setFilePosition(currentFilePosition() + rowLen);
//
//            return null; // Row has been deleted.
//        }
//
//        ByteBuffer rowBytes = readDataFromFile(rowLen);
//
//        Data buff = Data.create(null, rowBytes.array(), true);
//
//        Value[] row = new Value[colCnt];
//
//        for (int i = 0; i < colCnt; i++)
//            row[i] = buff.readValue();
//
//        return row;
//    }
//
//    /**
//     * Reads row header starting from the current position.
//     *
//     * @return Buffer with row header
//     */
//    @NotNull protected ByteBuffer readRowHeaderFromFile() {
//        return readDataFromFile(ROW_HEADER_SIZE);
//    }
//
//    /**
//     * Reads data from file.
//     *
//     * @param rowLen Data length to read.
//     * @return Byte buffer with data.
//     */
//    @NotNull protected ByteBuffer readDataFromFile(int rowLen) {
//        try {
//            ByteBuffer rowBytes = ByteBuffer.allocate(rowLen); // TODO can we preallocate buffer?
//
//            fileIo.readFully(rowBytes);
//
//            rowBytes.flip();
//
//            return rowBytes;
//        }
//        catch (IOException e) {
//            close();
//
//            throw new IgniteException("Failed to read query result the from spill file.", e);
//        }
//    }
//
//    /**
//     * @param cap Buffer initial capacity.
//     * @return New data buffer.
//     */
//    @NotNull protected Data createDataBuffer(int cap) {
//        return Data.create(null, cap, true);
//    }
//
//    /**
//     * Adds row to buffer.
//     *
//     * @param row Row.
//     * @param buff Buffer.
//     */
//    protected void addRowToBuffer(Value[] row, Data buff) {
//        int initPos = buff.length();
//
//        buff.checkCapacity(rowSize(row));
//
//        buff.writeInt(0); // Skip int position for row length in bytes.
//        buff.writeInt(row.length); // Skip int position for columns count.
//
//        for (int i = 0; i < row.length; i++)
//            buff.writeValue(row[i]);
//
//        int len = buff.length() - initPos - ROW_HEADER_SIZE;
//
//        buff.setInt(initPos, len);
//    }
//
//    /**
//     * Writes buffer to file.
//     *
//     * @param buff Buffer.
//     * @return Bytes written.
//     */
//    protected int writeBufferToFile(Data buff) {
//        try {
//            ByteBuffer byteBuff = ByteBuffer.wrap(buff.getBytes());
//
//            byteBuff.limit(buff.length());
//
//            while (byteBuff.hasRemaining()) {
//                int bytesWritten = fileIo.write(byteBuff);
//
//                if (bytesWritten <= 0)
//                    throw new IOException("Can not write data to file: " + file.getAbsolutePath());
//            }
//
//            return byteBuff.limit();
//        }
//        catch (IOException e) {
//            close();
//
//            throw new IgniteException("Failed to write intermediate query result to the spill file.", e);
//        }
//    }
//
//    /**
//     * Marks row in the file as removed by setting {@code -1} on it's {@code colCount} position.
//     *
//     * @param addr Row absolute address in the file.
//     */
//    protected void markRowRemoved(long addr) {
//        setFilePosition(addr);
//
//        ByteBuffer hdr = readRowHeaderFromFile(); // TODO do not read from, use setFilePosition(addr + TOMBSTONE_OFFSET);
//
//        hdr.getInt(); // Skip row length.
//
//        hdr.putInt(-1); // Put tombstone: -1 on columns count position.
//
//        hdr.flip();
//
//        //boolean res = U.delete(file.getParentFile());
//
//       // System.out.println("deletion res=" + res);
//
//        setFilePosition(addr);
//
//        try {
//            while (hdr.hasRemaining()) {
//                int bytesWritten = fileIo.write(hdr);
//
//                if (bytesWritten <= 0)
//                    throw new IOException("Can not write data to file: " + file.getAbsolutePath());
//            }
//        }
//        catch (IOException e) {
//            close();
//
//            throw new IgniteException("Failed to remove row from the intermediate query result in the spill file.", e);
//        }
//    }
//
//    /**
//     * @return Current absolute position in the file.
//     */
//    protected long currentFilePosition() {
//        try {
//            return fileIo.position();
//        }
//        catch (IOException e) {
//            close();
//
//            throw new IgniteException("Failed to access the spill file.", e);
//        }
//    }
//
//    /**
//     * Sets position in file on the beginning.
//     */
//    protected void rewindFile() {
//        setFilePosition(0);
//    }
//
//    /**
//     * Sets arbitrary file position.
//     *
//     * @param pos Position to set.
//     */
//    protected void setFilePosition(long pos) {
//        try {
//            fileIo.position(pos);
//        }
//        catch (IOException e) {
//            close();
//
//            throw new IgniteException("Failed to reset the spill file.", e);
//        }
//    }

    /** */
    protected synchronized void onChildCreated() {
        childCnt++;
    }

    /** {@inheritDoc} */
    @Override public synchronized void close() {
        if (closed)
            return;

        closed = true;



        if (parent == null) {
            if (childCnt == 0)
                onClose();
        }
        else
            parent.closeChild();
    }

    /** */
    protected synchronized void closeChild() {
        if (--childCnt == 0 && closed)
            onClose();
    }

    /** */
    protected void onClose() {
        data.close();
        //file.delete();

//        if (log.isDebugEnabled())
//            log.debug("Deleted spill file "+ file.getName());
    }
//
//    /**
//     * @param row Row.
//     * @return Row size in bytes.
//     */
//    public static int rowSize(Value[] row) {
//        return (int)(ROW_HEADER_SIZE + H2Utils.rowSizeInBytes(row));
//    }
//
//    /**
//     * @param rows Rows.
//     * @return Rows size in bytes.
//     */
//    public static int rowSize(Collection<Value[]> rows) {
//        int size = 0;
//
//        for (Value[] row : rows)
//            size += rowSize(row);
//
//        return size;
//    }
}
