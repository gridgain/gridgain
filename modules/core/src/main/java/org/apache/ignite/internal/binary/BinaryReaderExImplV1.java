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

import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.internal.binary.streams.BinaryInputStream;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.binary.GridBinaryMarshaller.HDR_LEN_V1;
import static org.apache.ignite.internal.binary.GridBinaryMarshaller.UNREGISTERED_TYPE_ID;

/**
 * Binary reader implementation (protocol version 1).
 *
 * @see BinaryWriterExImplV1
 */
public class BinaryReaderExImplV1 extends BinaryAbstractReaderEx implements BinaryReaderEx {
    /** Protocol version. */
    private static final byte PROTO_VER = 1;

    /** Type ID. */
    private final int typeId;

    /** Flags. */
    private final short flags;

    /** Total length. */
    private final int totalLen;

    /** Data start offset. */
    private final int dataStartOff;

    /** Data length. */
    private final int dataLen;

    /** Footer start offset. */
    private final int footerStartOff;

    /** Schema id. */
    private final int schemaId;

    /** Raw offset. */
    private final int rawOff;

    /**
     * Constructor.
     *
     * @param ctx Context.
     * @param in Input stream.
     * @param ldr Class loader.
     * @param hnds Context.
     * @param skipHdrCheck Whether to skip header check.
     * @param forUnmarshal {@code True} if reader is need to unmarshal object.
     */
    public BinaryReaderExImplV1(BinaryContext ctx,
        BinaryInputStream in,
        ClassLoader ldr,
        @Nullable BinaryReaderHandles hnds,
        boolean skipHdrCheck,
        boolean forUnmarshal) {
        super(ctx, in, ldr, hnds);

        // Perform full header parsing in case of binary object.
        if (!skipHdrCheck && (in.readByte() == GridBinaryMarshaller.OBJ)) {
            byte ver = in.readByte();

            if (PROTO_VER != ver)
                throw new BinaryObjectException("Protocol version mismatch: required=" + PROTO_VER + ", actual=" + ver);

            // Read header content.
            flags = in.readShort();
            int typeId0 = in.readInt();

            in.readInt(); // Skip hash code.

            totalLen = in.readInt();
            schemaId = in.readInt();
            int schemaOrRawOff = in.readInt();

            footerStartOff = BinaryUtils.hasSchema(flags) ? start + schemaOrRawOff : objectEndOffset();

            if (BinaryUtils.hasRaw(flags))
                rawOff = start + (BinaryUtils.hasSchema(flags) ? in.readIntPositioned(rawOffsetPos()) : schemaOrRawOff);
            else
                rawOff = objectEndOffset();

            typeId = typeId0 == UNREGISTERED_TYPE_ID ? readTypeId(ctx, in, ldr, forUnmarshal) : typeId0;

            dataStartOff = start + HDR_LEN_V1 + (typeId0 == UNREGISTERED_TYPE_ID
                ? className(ctx).length() + 5 /* 1 for value type + 4 for string length */ : 0);

            dataLen = footerStartOff - dataStartOff;
        }
        else {
            typeId = 0;
            flags = 0;
            totalLen = 0;
            dataStartOff = 0;
            dataLen = 0;
            footerStartOff = 0;
            schemaId = 0;
            rawOff = 0;
        }

        streamPosition(start);
    }

    /** */
    private int objectEndOffset() {
        return start + totalLen;
    }

    /** */
    private int rawOffsetPos() {
        return objectEndOffset() - 4;
    }

    /** */
    private String className(BinaryContext ctx) {
        return ctx.metadata(typeId).typeName();
    }

    /**
     * @param ctx Context.
     * @param in In.
     * @param ldr Loader.
     * @param forUnmarshal For unmarshal.
     */
    private int readTypeId(BinaryContext ctx, BinaryInputStream in, ClassLoader ldr, boolean forUnmarshal) {
        in.position(start + HDR_LEN_V1);

        if (!forUnmarshal)
            return ctx.typeId(BinaryUtils.doReadClassName(in));

        // Registers class by type ID, at least locally if the cache is not ready yet.
        desc = ctx.registerClass(BinaryUtils.doReadClass(in, ctx, ldr, UNREGISTERED_TYPE_ID), false, false);

        return desc.typeId();
    }

    /** {@inheritDoc} */
    @Override public int length() {
        return totalLen;
    }

    /** {@inheritDoc} */
    @Override public int dataStartOffset() {
        return dataStartOff;
    }

    /** {@inheritDoc} */
    @Override public int dataLength() {
        return dataLen;
    }

    /** {@inheritDoc} */
    @Override public int footerStartOffset() {
        return footerStartOff;
    }

    /** {@inheritDoc} */
    @Override public int rawOffset() {
        return rawOff;
    }

    /** {@inheritDoc} */
    @Override public int schemaId() {
        return schemaId;
    }

    /** {@inheritDoc} */
    @Override public int typeId() {
        return typeId;
    }

    /** {@inheritDoc} */
    @Override public short flags() {
        return flags;
    }

    /** {@inheritDoc} */
    @Override public byte version() {
        return PROTO_VER;
    }

    /** {@inheritDoc} */
    @Override public int rawLength() {
        return start + HDR_LEN_V1 + dataLen - rawOff;
    }

    /** {@inheritDoc} */
    @Override public int footerLength() {
        return start + totalLen - footerStartOff;
    }
}
