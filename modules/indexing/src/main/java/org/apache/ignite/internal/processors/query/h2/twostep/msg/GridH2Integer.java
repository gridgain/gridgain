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

package org.apache.ignite.internal.processors.query.h2.twostep.msg;

import java.nio.ByteBuffer;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;
import org.gridgain.internal.h2.value.Value;
import org.gridgain.internal.h2.value.ValueInt;

/**
 * H2 Integer.
 */
public class GridH2Integer extends GridH2ValueMessage {
    /** */
    private int x;

    /**
     *
     */
    public GridH2Integer() {
        // No-op.
    }

    /**
     * @param val Value.
     */
    public GridH2Integer(Value val) {
        assert val.getType().getValueType() == Value.INT : val.getType();

        x = val.getInt();
    }

    /** {@inheritDoc} */
    @Override public Value value(GridKernalContext ctx) {
        return ValueInt.get(x);
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
            case 0:
                if (!writer.writeInt("x", x))
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
            case 0:
                x = reader.readInt("x");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(GridH2Integer.class);
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return -8;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 1;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object obj) {
        return obj == this || (obj != null && obj.getClass() == GridH2Integer.class && x == ((GridH2Integer)obj).x);

    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return x;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return String.valueOf(x);
    }
}
