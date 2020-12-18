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
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

import java.io.Externalizable;
import java.nio.ByteBuffer;
import java.util.UUID;

/**
 * Request to cancel statistics collection.
 */
public class CancelStatsCollectionRequest implements Message {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    public static final short TYPE_CODE = 180;

    /** Request id. */
    private UUID colId;

    /** Request id. */
    private UUID[] reqIds;

    /**
     * {@link Externalizable} support.
     */
    public CancelStatsCollectionRequest() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param colId Id of collection to cancel.
     * @param reqIds Request ids to cancel.
     */
    public CancelStatsCollectionRequest(UUID colId, UUID[] reqIds) {
        this.colId = colId;
        this.reqIds = reqIds;
    }

    /**
     * @return Id of collection to cancel.
     */
    public UUID colId() {
        return colId;
    }

    /**
     * @return Request id.
     */
    public UUID[] reqIds() {
        return reqIds;
    }

    /** {@inheritDoc} */
    @Override public boolean writeTo(ByteBuffer buf, MessageWriter writer) {

        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean readFrom(ByteBuffer buf, MessageReader reader) {

        return reader.afterMessageRead(CancelStatsCollectionRequest.class);
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 0;
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
