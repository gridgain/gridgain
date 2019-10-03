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

package org.apache.ignite.internal.binary;

import org.apache.ignite.binary.BinaryRawWriter;
import org.apache.ignite.internal.binary.streams.BinaryOutputStream;

/**
 * Binary writer implementation for protocol version 1. <p/>
 *
 * Layout for version 1 consists of 6 part:
 *
 * <pre>
 *      [header][class name][schema data section][raw data section][schema description][raw data offset]
 * </pre>
 *
 * where:
 *
 * <ul>
 *      <li>header -- mandatory part with length of 24 bytes:<br>
 *      <pre>
 *      [value type: 1][proto version: 1][flags: 2][type id: 4][hash code: 4][total length: 4][schema id: 4][raw offset or schema position: 4]
 *      </pre>
 *      , if there is raw data and there is no schema data then last 4 bytes will be offset of raw data,
 *      otherwise it will be offset of schema description;</li>
 *
 *      <li>class name -- optional part of variable length. Containts {@link Class#getName()} of written object.
 *      Presents only if current type is unregistered ({@code typeId == 0});</li>
 *
 *      <li>schema data section -- optional part of variable length. Contains values written with object schema.
 *      Presents if there are fields written to object;</li>
 *
 *      <li>raw data section -- optional part of variable length. Contains bytes of arbitrary format.
 *      Presents if there are raw bytes written by {@link BinaryRawWriter};</li>
 *
 *      <li>schema description -- optional part of variable length. Presents if schema data is present;</li>
 *
 *      <li>raw data offset -- optional part with length of 4 bytes. Contains offset to raw data section.
 *      Presents if schema data is present and there are raw bytes written by {@link BinaryRawWriter}.</li>
 * </ul>
 */
public class BinaryExWriterImplV1 extends BinaryAbstractWriter {
    /** Protocol version. */
    private static final byte PROTO_VER = 1;

    /**
     * @param ctx Context.
     * @param out Output stream.
     * @param handles Handles.
     */
    public BinaryExWriterImplV1(BinaryContext ctx, BinaryOutputStream out, BinaryWriterSchemaHolder schema,
        BinaryWriterHandles handles) {
        super(ctx, out, schema, handles);
    }

    /** {@inheritDoc} */
    @Override public void preWrite(boolean registered) {
        out.position(out.position() + GridBinaryMarshaller.HDR_LEN_V1);

        if (!registered)
            doWriteString(clsName);
    }

    /** {@inheritDoc} */
    @Override public void postWrite(boolean userType, boolean registered) {
        short flags = initFlags(userType);

        int effectiveSchemaId = 0;
        int schemaOrRawOff = 0;

        if (BinaryUtils.hasSchema(flags)) {
            schemaOrRawOff = out.position() - start;
            effectiveSchemaId = schemaId;

            flags |= writeSchema(userType);

            if (BinaryUtils.hasRaw(flags))
                out.writeInt(rawOffPos - start);
        }
        else if (BinaryUtils.hasRaw(flags))
            schemaOrRawOff = rawOffPos - start;

        int retPos = out.position();
        int effectiveTypeId = registered ? typeId : GridBinaryMarshaller.UNREGISTERED_TYPE_ID;
        int totalLen = retPos - start;

        out.unsafePosition(start);

        writeHeader(effectiveTypeId, flags, effectiveSchemaId, schemaOrRawOff, totalLen);

        out.unsafePosition(retPos);
    }

    /** */
    private void writeHeader(int typeId, short flags, int schemaId, int schemaOrRawOff, int totalLen) {
        out.unsafeWriteByte(GridBinaryMarshaller.OBJ);
        out.unsafeWriteByte(PROTO_VER);
        out.unsafeWriteShort(flags);
        out.unsafeWriteInt(typeId);

        out.unsafePosition(start + GridBinaryMarshaller.TOTAL_LEN_POS); // skip hashcode

        out.unsafeWriteInt(totalLen);
        out.unsafeWriteInt(schemaId);
        out.unsafeWriteInt(schemaOrRawOff);
    }

    /** {@inheritDoc} */
    @Override public byte version() {
        return PROTO_VER;
    }
}
