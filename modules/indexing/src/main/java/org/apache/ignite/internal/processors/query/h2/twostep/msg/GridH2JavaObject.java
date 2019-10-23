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
import java.util.Collection;
import java.util.Map;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.binary.BinaryObjectImpl;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;
import org.h2.value.Value;
import org.h2.value.ValueJavaObject;

import static org.h2.util.StringUtils.convertBytesToHex;

/**
 * H2 Java Object.
 */
public class GridH2JavaObject extends GridH2ValueMessage {
    /** */
    private byte[] b;

    /**
     *
     */
    public GridH2JavaObject() {
        // No-op.
    }

    /**
     * @param val Value.
     */
    public GridH2JavaObject(Value val) {
        assert val.getType().getValueType() == Value.JAVA_OBJECT : val.getType();

        allowDetachForSimpleContainers(val.getObject());

        b = val.getBytesNoCopy();
    }

    /**
     * Allows detach for binary objects in containers to avoid excessive memory usage.
     *
     * @param obj Object to proccess.
     */
    private void allowDetachForSimpleContainers(Object obj) {
        if (obj instanceof Collection) {
            Collection col = (Collection)obj;

            for (Object x : col) {
                if (x instanceof BinaryObjectImpl)
                    ((BinaryObjectImpl)x).detachAllowed(true);
            }
        }
        else if (obj instanceof Object[]) {
            Object[] arr = (Object[])obj;

            for (Object x : arr) {
                if (x instanceof BinaryObjectImpl)
                    ((BinaryObjectImpl)x).detachAllowed(true);
            }
        }
        else if (obj instanceof Map) {
            Map map = (Map)obj;

            for (Object x0 : map.entrySet()) {
                Map.Entry x = (Map.Entry)x0;

                if (x.getKey() instanceof BinaryObjectImpl)
                    ((BinaryObjectImpl)x.getKey()).detachAllowed(true);

                if (x.getValue() instanceof BinaryObjectImpl)
                    ((BinaryObjectImpl)x.getValue()).detachAllowed(true);
            }
        }
    }

    /** {@inheritDoc} */
    @Override public Value value(GridKernalContext ctx) {
        return ValueJavaObject.getNoCopy(null, b, null);
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
                if (!writer.writeByteArray("b", b))
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
                b = reader.readByteArray("b");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(GridH2JavaObject.class);
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return -19;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 1;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "j_" + convertBytesToHex(b);
    }
}
