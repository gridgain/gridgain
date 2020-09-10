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

package org.apache.ignite.internal.processors.odbc;

import org.apache.ignite.internal.binary.BinaryContext;
import org.apache.ignite.internal.binary.BinaryThreadLocalContext;
import org.apache.ignite.internal.binary.BinaryWriterExImpl;
import org.apache.ignite.internal.binary.streams.BinaryHeapOutputStream;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Class that efficiently handles client response.
 */
public class ClientListenerResponseBuffer {
    /** Stream. */
    BinaryHeapOutputStream outStream;

    /** Writer to write response contents. */
    BinaryWriterExImpl writer;

    /**
     * @param ctx Binary context.
     * @param initialSize Initial response buffer size.
     */
    public ClientListenerResponseBuffer(BinaryContext ctx, int initialSize) {
        outStream = new BinaryHeapOutputStream(initialSize);

        writer = new BinaryWriterExImpl(ctx, outStream, BinaryThreadLocalContext.get().schemaHolder(), null);

        // Reserving space for message size.
        writer.reserveInt();
    }

    /**
     * @return Writer to use to to write a response payload.
     */
    public BinaryWriterExImpl getPayloadWriter() {
        return writer;
    }

    /**
     * @return Response as ByteBuffer.
     */
    public ByteBuffer asByteBuffer() {
        writer.writeInt(0, outStream.position() - 4);

        ByteBuffer res = ByteBuffer.wrap(outStream.array(), 0, outStream.position());
        res.order(ByteOrder.LITTLE_ENDIAN);
        res.isDirect();

        return res;
    }
}
