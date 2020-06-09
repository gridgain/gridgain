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

package org.apache.ignite.internal.managers.communication;

import java.io.Externalizable;
import java.nio.ByteBuffer;
import java.util.UUID;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;

/**
 * Represents a security communication message.
 */
public class GridIoSecurityAwareMessage extends GridIoMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    public static final short TYPE_CODE = 174;

    /** Security subject id that will be used during message processing on an remote node. */
    private UUID secSubjId;

    /** Security context transmitting from node initiator of action. */
    private byte[] secCtx;

    /**
     * No-op constructor to support {@link Externalizable} interface.
     * This constructor is not meant to be used for other purposes.
     */
    public GridIoSecurityAwareMessage() {
        // No-op.
    }

    /**
     * @param secSubjId Security subject id.
     * @param plc Policy.
     * @param topic Communication topic.
     * @param topicOrd Topic ordinal value.
     * @param msg Message.
     * @param ordered Message ordered flag.
     * @param timeout Timeout.
     * @param skipOnTimeout Whether message can be skipped on timeout.
     * @param connIdx Desired {@link TcpCommunicationSpi} connection index if applicable.
     */
    public GridIoSecurityAwareMessage(
        UUID secSubjId,
        byte[] secSubject,
        byte plc,
        Object topic,
        int topicOrd,
        Message msg,
        boolean ordered,
        long timeout,
        boolean skipOnTimeout,
        int connIdx
    ) {
        super(plc, topic, topicOrd, msg, ordered, timeout, skipOnTimeout, connIdx);

        this.secSubjId = secSubjId;
        this.secCtx = secSubject;
    }

    /**
     * @return Security subject id.
     */
    UUID secSubjId() {
        return secSubjId;
    }

    /**
     * @return Security context
     */
    public byte[] getSecCtx() {
        return secCtx;
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return TYPE_CODE;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 10;
    }

    /** {@inheritDoc} */
    @Override public boolean writeTo(ByteBuffer buf, MessageWriter writer) {
        writer.setBuffer(buf);

        if (!super.writeTo(buf, writer))
            return false;

        if (!writer.isHeaderWritten()) {
            if (!writer.writeHeader(directType(), fieldsCount()))
                return false;

            writer.onHeaderWritten();
        }

        switch (writer.state()) {
            case 8:
                if (!writer.writeByteArray("secCtx", secCtx))
                    return false;

                writer.incrementState();

            case 9:
                if (!writer.writeUuid("secSubjId", secSubjId))
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

        if (!super.readFrom(buf, reader))
            return false;

        switch (reader.state()) {
            case 8:
                secCtx = reader.readByteArray("secCtx");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 9:
                secSubjId = reader.readUuid("secSubjId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(GridIoSecurityAwareMessage.class);
    }
}
