/*
 * Copyright 2020 GridGain Systems, Inc. and Contributors.
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
package org.apache.ignite.internal.processors.query.stat.messages;

import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageCollectionItemType;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

import java.nio.ByteBuffer;
import java.util.UUID;

/**
 * Request to cancel statistics collection.
 */
public class CancelStatisticsGatheringRequest implements Message {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    public static final short TYPE_CODE = 180;

    /** Gathering id. */
    private UUID gatId;

    /** Request id. */
    private UUID[] reqIds;

    /**
     * Default constructor.
     */
    public CancelStatisticsGatheringRequest() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param gatId Id of gathering to cancel.
     * @param reqIds Request ids to cancel.
     */
    public CancelStatisticsGatheringRequest(UUID gatId, UUID[] reqIds) {
        this.gatId = gatId;
        this.reqIds = reqIds;
    }

    /**
     * @return Id of gathering to cancel.
     */
    public UUID gatId() {
        return gatId;
    }

    /**
     * @return Request id.
     */
    public UUID[] reqIds() {
        return reqIds;
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
                if (!writer.writeUuid("gatId", gatId))
                    return false;

                writer.incrementState();

            case 1:
                if (!writer.writeObjectArray("reqIds", reqIds, MessageCollectionItemType.UUID))
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
                gatId = reader.readUuid("gatId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 1:
                reqIds = reader.readObjectArray("reqIds", MessageCollectionItemType.UUID, UUID.class);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(CancelStatisticsGatheringRequest.class);
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return TYPE_CODE;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 2;
    }

    /** {@inheritDoc} */
    @Override public void onAckReceived() {
        // No-op.
    }
}
