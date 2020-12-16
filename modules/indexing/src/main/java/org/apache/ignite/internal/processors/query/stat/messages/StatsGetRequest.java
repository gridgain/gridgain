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

import org.apache.ignite.internal.GridDirectCollection;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageCollectionItemType;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

import java.io.Externalizable;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.UUID;

/**
 * Statistics request message.
 */
public class StatsGetRequest implements Message {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    public static final short TYPE_CODE = 182;

    /** Request id. */
    private UUID reqId;

    /** List of keys to supply statistics by. */
    @GridDirectCollection(StatsKeyMessage.class)
    private List<StatsKeyMessage> keys;

    /** {@inheritDoc} */
    @Override public void onAckReceived() {
        // No-op.
    }

    /**
     * {@link Externalizable} support.
     */
    public StatsGetRequest() {
    }

    /**
     * Constructor.
     *
     * @param reqId Request id.
     * @param keys Keys to get statistics by.
     */
    public StatsGetRequest(UUID reqId, List<StatsKeyMessage> keys) {
        this.reqId = reqId;
        this.keys = keys;
    }

    /**
     * @return Request id.
     */
    public UUID reqId() {
        return reqId;
    }

    /**
     * @return Keys to get statistics by.
     */
    public List<StatsKeyMessage> keys() {
        return keys;
    }

    /** {@inheritDoc} */
    @Override public boolean writeTo(ByteBuffer buf, MessageWriter writer) {

        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean readFrom(ByteBuffer buf, MessageReader reader) {

        return reader.afterMessageRead(StatsGetRequest.class);
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return TYPE_CODE;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 3;
    }
}
