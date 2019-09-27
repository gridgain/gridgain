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

    /**
     * Perform pre-write. Reserves space for header and writes class name if needed.
     *
     * @param registered Whether type is registered.
     */
    @Override public void preWrite(boolean registered) {
        out.position(out.position() + GridBinaryMarshaller.HDR_LEN_V2);
    }

    /**
     * Perform post-write. Fills object header.
     *
     * @param userType User type flag.
     * @param registered Whether type is registered.
     */
    @Override public void postWrite(boolean userType, boolean registered) {
        short flags;
        boolean useCompactFooter;

        int dataLen = out.position() - start - GridBinaryMarshaller.HDR_LEN_V2;

        if (userType) {
            if (ctx.isCompactFooter()) {
                flags = BinaryUtils.FLAG_USR_TYP | BinaryUtils.FLAG_COMPACT_FOOTER;
                useCompactFooter = true;
            }
            else {
                flags = BinaryUtils.FLAG_USR_TYP;
                useCompactFooter = false;
            }
        }
        else {
            flags = 0;
            useCompactFooter = false;
        }

        int finalSchemaId;

        if (fieldCnt != 0) {
            finalSchemaId = schemaId;

            // Write the schema.
            flags |= BinaryUtils.FLAG_HAS_SCHEMA;
        }
        else
            finalSchemaId = 0;

        if (rawOffPos != 0)
            flags |= BinaryUtils.FLAG_HAS_RAW;

        if (hasMetaSection(typeId, flags) || !registered) {
            if (BinaryUtils.hasRaw(flags))
                out.writeInt(rawOffPos - start);

            int footerOffPos = 0;

            if (BinaryUtils.hasSchema(flags)) {
                out.writeInt(finalSchemaId);

                footerOffPos = out.position();

                out.position(footerOffPos + 4);
            }

            if (!registered)
                doWriteString(clsName);

            if (BinaryUtils.hasSchema(flags)) {
                int footerOff = out.position() - start;

                out.position(footerOffPos);

                out.writeInt(footerOff);

                out.position(start + footerOff);
            }
        }

        if (BinaryUtils.hasSchema(flags)) {
            int offByteCnt = schema.write(out, fieldCnt, useCompactFooter);

            if (offByteCnt == BinaryUtils.OFFSET_1)
                flags |= BinaryUtils.FLAG_OFFSET_ONE_BYTE;
            else if (offByteCnt == BinaryUtils.OFFSET_2)
                flags |= BinaryUtils.FLAG_OFFSET_TWO_BYTES;
        }

        int retPos = out.position();

        out.unsafePosition(start);

        out.unsafeWriteByte(GridBinaryMarshaller.OBJ);
        out.unsafeWriteByte(PROTO_VER);
        out.unsafeWriteShort(flags);
        out.unsafeWriteInt(registered ? typeId : GridBinaryMarshaller.UNREGISTERED_TYPE_ID);

        out.unsafePosition(start + GridBinaryMarshaller.TOTAL_LEN_POS); // skip hash code

        out.unsafeWriteInt(retPos - start);
        out.unsafeWriteInt(dataLen);

        out.unsafePosition(retPos);
    }

    /** {@inheritDoc} */
    @Override public byte version() {
        return PROTO_VER;
    }

    /** */
    private static boolean hasMetaSection(int typeId, short flags) {
        return typeId == GridBinaryMarshaller.UNREGISTERED_TYPE_ID || BinaryUtils.hasSchema(flags);
    }
}
