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

import static org.apache.ignite.internal.binary.GridBinaryMarshaller.HDR_LEN_V2;

/**
 * Binary writer implementation for protocol version 2. <p/>
 *
 * Layout for version 2 consists of 5 part:
 *
 * <pre>
 *      [header][schema data section][raw data section][meta data section][schema description]
 * </pre>
 *
 * where:
 *
 * <ul>
 *      <li>header -- mandatory part with length of 20 bytes:<br>
 *      <pre>
 *      [value type: 1][proto version: 1][flags: 2][type id: 4][hash code: 4][total length: 4][data length: 4]
 *      </pre>
 *      , data length is equals sum of length both schema and raw data sections</li>
 *
 *      <li>schema data section -- optional part of variable length. Contains values written with object schema.
 *      Presents if there are fields written to object.</li>
 *
 *      <li>raw data section -- optional part of variable length. Contains bytes of arbitrary format.
 *      Presents if there are raw bytes written by {@link BinaryRawWriter}</li>
 *
 *      <li>meta data section -- optional part of variable length. Presents if type is unregistered ({@code typeId == 0})
 *      or there is schema section. Has following format:<br>
 *      <pre>
 *      [raw offset: 4][schema id: 4][schema description offset: 4][class name: var len][update time: 8]
 *      </pre>
 *      , each part is optional.</li>
 *
 *      <li>schema description -- optional part of variable length. Presents if schema data is present {@link BinaryRawWriter}</li>
 * </ul>
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

        if (BinaryUtils.hasSchema(flags) || !registered)
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

    /** {@inheritDoc} */
    @Override protected short initFlags(boolean userType) {
        short flags = super.initFlags(userType);

        if (updateTime >= 0)
            flags |= BinaryUtils.FLAG_HAS_UPDATE_TIME;

        return flags;
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
            out.writeInt(schemaId);
            out.writeInt(footerOffset(dataLen, registered, flags));
        }

        if (!registered)
            doWriteString(clsName);

        if (BinaryUtils.hasUpdateTime(flags))
            out.writeLong(updateTime);
    }

    /** */
    private int footerOffset(int dataLen, boolean registered, short flags) {
        return HDR_LEN_V2 + dataLen // meta section rigth after data
            + (BinaryUtils.hasRaw(flags) ? 4 : 0) // count raw offset
            + (BinaryUtils.hasSchema(flags) ? 8 : 0) // count schema id + footer offset
            + (!registered ? clsName.length() + 5 : 0) // count class name
            + (BinaryUtils.hasUpdateTime(flags) ? 8 : 0); // count update time
    }

    /** {@inheritDoc} */
    @Override public byte version() {
        return PROTO_VER;
    }
}
