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

import org.apache.ignite.internal.binary.streams.BinaryOutputStream;

/**
 * Binary writer implementation.
 */
public class BinaryWriterExImplV1 extends BinaryAbstractWriterEx {
    /** Protocol version. */
    private static final byte PROTO_VER = 1;

    /**
     * @param ctx Context.
     * @param out Output stream.
     * @param handles Handles.
     */
    public BinaryWriterExImplV1(BinaryContext ctx, BinaryOutputStream out, BinaryWriterSchemaHolder schema,
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
