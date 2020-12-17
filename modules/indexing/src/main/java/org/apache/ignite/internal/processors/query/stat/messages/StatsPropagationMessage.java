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

/**
 * Message to send statistics.
 */
public class StatsPropagationMessage implements Message {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    public static final short TYPE_CODE = 181;

    /** */
    @GridDirectCollection(StatsObjectData.class)
    private List<StatsObjectData> data;

    /** {@inheritDoc} */
    @Override public void onAckReceived() {
        // No-op.
    }

    /**
     * {@link Externalizable} support.
     */
    public StatsPropagationMessage() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param data List of objects statistics.
     */
    public StatsPropagationMessage(List<StatsObjectData> data) {
        this.data = data;
    }

    /**
     * @return List of objects statistics.
     */
    public List<StatsObjectData> data() {
        return data;
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
                if (!writer.writeCollection("data", data, MessageCollectionItemType.MSG))
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
                data = reader.readCollection("data", MessageCollectionItemType.MSG);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(StatsPropagationMessage.class);
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return TYPE_CODE;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 1;
    }
}
