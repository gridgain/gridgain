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

import static org.apache.ignite.internal.binary.GridBinaryMarshaller.HDR_LEN_V2;

/**
 * Binary writer implementation.
 */
public class BinaryWriterExImplV2 extends BinaryAbstractWriterEx {
    /** Protocol version. */
    private static final byte PROTO_VER = 2;

    /**
     * @param ctx Context.
     * @param out Output stream.
     * @param handles Handles.
     */
    public BinaryWriterExImplV2(BinaryContext ctx, BinaryOutputStream out, BinaryWriterSchemaHolder schema,
        BinaryWriterHandles handles) {
        super(ctx, out, schema, handles);
    }


    /** {@inheritDoc} */
    @Override public void preWrite(boolean registered) {
        out.position(out.position() + HDR_LEN_V2);
    }


    /** {@inheritDoc} */
    @Override public void postWrite(boolean userType, boolean registered) {
        int dataLen = out.position() - start - HDR_LEN_V2;

        short flags = initFlags(userType);

        if (!BinaryUtils.hasSchema(flags) && registered)
            writeMeta(flags, registered, dataLen);

        if (BinaryUtils.hasSchema(flags))
            flags |= writeSchema(userType);

        int retPos = out.position();
        int effectiveTypeId = registered ? typeId : GridBinaryMarshaller.UNREGISTERED_TYPE_ID;
        int totalLen = retPos - start;

        out.unsafePosition(start);

        writeHeader(effectiveTypeId, flags, dataLen, totalLen);

        out.unsafePosition(retPos);
    }

    /** */
    private void writeHeader(int typeId, short flags, int dataLen, int totalLen) {
        out.unsafeWriteByte(GridBinaryMarshaller.OBJ);
        out.unsafeWriteByte(PROTO_VER);
        out.unsafeWriteShort(flags);
        out.unsafeWriteInt(typeId);

        out.unsafePosition(start + GridBinaryMarshaller.TOTAL_LEN_POS); // skip hash code

        out.unsafeWriteInt(totalLen);
        out.unsafeWriteInt(dataLen);
    }

    /** */
    private void writeMeta(short flags, boolean registered, int dataLen) {
        if (BinaryUtils.hasRaw(flags))
            out.writeInt(rawOffPos - start);

        if (BinaryUtils.hasSchema(flags)) {
            out.writeInt(fieldCnt != 0 ? schemaId : 0);

            out.writeInt(footerOffset(dataLen, registered, flags));
        }

        if (!registered)
            doWriteString(clsName);
    }

    /** */
    private int footerOffset(int dataLen, boolean registered, short flags) {
        return HDR_LEN_V2 + dataLen // meta section rigth after data
            + (BinaryUtils.hasRaw(flags) ? 4 : 0) // count raw offset
            + (!registered ? clsName.length() + 5 : 0); // count class name
    }

    /** {@inheritDoc} */
    @Override public byte version() {
        return PROTO_VER;
    }
}
