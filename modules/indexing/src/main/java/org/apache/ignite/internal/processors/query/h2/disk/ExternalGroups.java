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

import com.sun.xml.internal.messaging.saaj.util.ByteInputStream;
import com.sun.xml.internal.messaging.saaj.util.ByteOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.processors.query.h2.H2MemoryTracker;
import org.apache.ignite.internal.processors.query.h2.H2Utils;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.h2.expression.aggregate.AggregateSerializer;
import org.h2.store.Data;
import org.h2.value.ValueRow;
import org.jetbrains.annotations.NotNull;

import static java.nio.file.StandardOpenOption.CREATE_NEW;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;
import static org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing.DISK_SPILL_DIR;

/**
 * TODO: Add class description.
 */
public class ExternalGroups implements ExternalRowStore, AutoCloseable {
    /** File name generator. */
    private static final AtomicLong idGen = new AtomicLong();

    private static final int ROW_HEADER_SIZE = 4; // int

    /** Logger. */
    private final IgniteLogger log;

    /** Current size in rows. */
    private int size;

    /** File. */
    private final File file;

    /** Spill file IO.*/
    private volatile FileIO fileIo;

    private ExternalResultHashIndex idx;

    /** Memory tracker. */
    private final H2MemoryTracker memTracker;

    private long curDataPos;


    public ExternalGroups(GridKernalContext ctx, H2MemoryTracker memTracker) {
        this.log = ctx.log(ExternalGroups.class);

        try {
            String fileName = "spill_groups_" + ctx.localNodeId() + "_" + idGen.incrementAndGet();

            file = new File(U.resolveWorkDirectory(
                ctx.config().getWorkDirectory(),
                DISK_SPILL_DIR,
                false
            ), fileName);

            file.deleteOnExit();

            FileIOFactory fileIOFactory = ctx.query().fileIOFactory();

            fileIo = fileIOFactory.create(file, CREATE_NEW, READ, WRITE);

            if (log.isDebugEnabled())
                log.debug("Created grouped spill file "+ file.getName());

            idx = new ExternalResultHashIndex(ctx, file, this, 1);

            this.memTracker = memTracker;

            if (memTracker != null)
                memTracker.registerCloseListener(this::close);
        }
        catch (IgniteCheckedException | IOException e) {
            throw new IgniteException("Failed to create a spill file for the intermediate grouped query results.", e);
        }
    }

    public void put(ValueRow key, Object[] grpData) {// keySize | keybytes | aggs number | aggs data
        try {
            Long startPoint = idx.get(key);

            if (startPoint == null)
                startPoint = fileIo.size();

            fileIo.position(startPoint);

            ByteOutputStream out = new ByteOutputStream();

            Data keyBuff = Data.create(null, (int)(ROW_HEADER_SIZE + H2Utils.rowSizeInBytes(key.getList()) * 2), true);

            keyBuff.writeValue(key);

            DataOutputStream dataOut = new DataOutputStream(out);

            // 1. Write key size.
            dataOut.writeInt(keyBuff.length());

            // 2. Write key body.
            dataOut.write(keyBuff.getBytes(), 0, keyBuff.length());

            // 3. Write aggs number.
            dataOut.writeInt(grpData.length);

            // 4. Write aggs body.
            for (Object o : grpData)
                AggregateSerializer.writeAggregate(o, dataOut);

            ByteBuffer headBuff = ByteBuffer.allocate(ROW_HEADER_SIZE);

            int os = out.size();
            headBuff.putInt(os);
            headBuff.flip();

            // 0. Write total entry size.
            curDataPos += fileIo.write(headBuff);

            curDataPos += fileIo.write(out.getBytes(), 0, out.size());

            idx.put(key, startPoint);

            size++;
        }
        catch (IOException e) {
            close();

            throw new IgniteException("Failed to spill aggregate to disk.", e);
        }
    }


    public Object[] get(ValueRow key) {

        Long addr = idx.get(key);


        if (addr == null)
            return null;

        Object[] row = readRowFromFile(addr);

        return Arrays.copyOfRange(row, 1, row.length);
    }


    @Override public Object[] readRowFromFile(long addr) { // 0 - keyRow, others - aggs
        try {
            fileIo.position(addr);

            Object[] aggs = readAggregate();

            return aggs;
        }
        catch (IOException e) {
            close();

            throw new IgniteException("Failed to spill aggregate to disk.", e);
        }
    }

    @NotNull private Object[] readAggregate() throws IOException {
        // 0. Read total size
        ByteBuffer header = ByteBuffer.allocate(ROW_HEADER_SIZE);

        fileIo.readFully(header);

        header.flip();

        long rowSize = header.getInt();

        ByteBuffer body = ByteBuffer.allocate((int)rowSize);

        fileIo.readFully(body);

        body.flip();

        //1. Read key size
        int keySize = body.getInt();

        Data keyBuff = Data.create(null, keySize + 1, true);

        keyBuff.write(body.array(), 4, keySize);

        keyBuff.reset();
        //keyBuff.readInt();
        ValueRow key = (ValueRow)keyBuff.readValue();

        body.position(keySize + 4);

        ByteInputStream in = new ByteInputStream(body.array(), keySize + 4 + 4, body.limit());

        int aggsNum = body.getInt();

        Object[] aggs = new Object[aggsNum + 1]; // Aggs + key.

        aggs[0] = key;

        for (int i = 1; i <= aggsNum; i++)
            aggs[i] = AggregateSerializer.readAggregate(new DataInputStream(in));
        return aggs;
    }

    @Override public ValueRow getRowKey(Object[] row) {
        return (ValueRow)row[0];
    }

    public long size() {
        return size;
    }

    @Override public void close() {
        U.closeQuiet(fileIo);
        U.closeQuiet(idx);
        file.delete();


        if (log.isDebugEnabled())
            log.debug("Deleted groups spill file "+ file.getName());
    }

    public Iterator<T2<ValueRow, Object[]>> cursor() {

        try {
            fileIo.position(0);

            return new Iterator<T2<ValueRow, Object[]>>() {
                @Override public boolean hasNext() {
                    try {
                        return fileIo.position() < fileIo.size(); // TODO: CODE: implement.
                    }
                    catch (IOException e) {
                        close();

                        throw new IgniteException("Failed to read aggregates from the disk.", e);
                    }
                }

                @Override public T2<ValueRow, Object[]> next() {
                    try {
                        Object[] row = readAggregate();

                        return new T2<>((ValueRow)row[0], Arrays.copyOfRange(row, 1, row.length));
                    }
                    catch (IOException e) {
                        close();

                        throw new IgniteException("Failed to read aggregates from the disk.", e);
                    }
                }
            };
        }
        catch (IOException e) {
            close();

            throw new IgniteException("Failed to read aggregates from the disk.", e);
        }
    }
}
