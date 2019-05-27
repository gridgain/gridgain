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

package org.apache.ignite.internal.processors.cache.distributed;

import java.nio.ByteBuffer;
import org.apache.ignite.internal.processors.tracing.SerializedSpan;
import org.apache.ignite.internal.processors.tracing.impl.SerializedSpanAdapter;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

/**
 *
 */
public class SerializedSpanMessage implements Message {
    /** */
    public static final short TYPE_CODE = 174;

    /** */
    private byte[] serializedSpanBytes;

    /**
     * Empty constructor required by the {@code Message} interface.
     */
    public SerializedSpanMessage() {
        // No-op.
    }

    /**
     * @param serializedSpan Serialized span.
     */
    public SerializedSpanMessage(SerializedSpan serializedSpan) {
        serializedSpanBytes = serializedSpan.value();
    }

    /**
     * @return Transferred span context.
     */
    public SerializedSpan serializedSpan() {
        return new SerializedSpanAdapter(serializedSpanBytes);
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return TYPE_CODE;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 1;
    }

    /** {@inheritDoc} */
    @Override public void onAckReceived() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public boolean writeTo(ByteBuffer buf, MessageWriter writer) {
        writer.setBuffer(buf);

        if (!writer.isHeaderWritten()) {
            if (!writer.writeHeader(directType(), fieldsCount()))
                return false;

            writer.onHeaderWritten();
        }

        switch (writer.state()) {
            case 0:
                if (!writer.writeByteArray("serializedSpanBytes", serializedSpanBytes))
                    return false;

                writer.incrementState();

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean readFrom(ByteBuffer buf, MessageReader reader) {
        reader.setBuffer(buf);

        if (!reader.beforeMessageRead())
            return false;

        switch (reader.state()) {
            case 0:
                serializedSpanBytes = reader.readByteArray("serializedSpanBytes");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(SerializedSpanMessage.class);
    }
}
