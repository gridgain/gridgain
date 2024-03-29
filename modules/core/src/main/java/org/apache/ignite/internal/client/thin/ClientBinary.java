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

package org.apache.ignite.internal.client.thin;

import java.util.Collection;
import java.util.Map;
import org.apache.ignite.IgniteBinary;
import org.apache.ignite.IgniteException;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.binary.BinaryType;
import org.apache.ignite.internal.binary.BinaryClassDescriptor;
import org.apache.ignite.internal.binary.BinaryContext;
import org.apache.ignite.internal.binary.BinaryEnumObjectImpl;
import org.apache.ignite.internal.binary.BinaryMetadata;
import org.apache.ignite.internal.binary.BinaryUtils;
import org.apache.ignite.internal.binary.builder.BinaryObjectBuilderImpl;
import org.apache.ignite.internal.binary.streams.BinaryHeapInputStream;

/**
 * Thin client implementation of {@link IgniteBinary}.
 */
class ClientBinary implements IgniteBinary {
    /** Marshaller. */
    private final ClientBinaryMarshaller marsh;

    /**
     * Constructor.
     */
    ClientBinary(ClientBinaryMarshaller marsh) {
        this.marsh = marsh;
    }

    /** {@inheritDoc} */
    @Override public int typeId(String typeName) {
        return marsh.context().typeId(typeName);
    }

    /** {@inheritDoc} */
    @Override public <T> T toBinary(Object obj) {
        if (obj == null)
            return null;

        if (BinaryUtils.isBinaryType(obj.getClass()))
            return (T)obj;

        byte[] objBytes = marsh.marshal(obj);

        return (T)marsh.unmarshal(new BinaryHeapInputStream(objBytes));
    }

    /** {@inheritDoc} */
    @Override public BinaryObjectBuilder builder(String typeName) {
        if (typeName == null || typeName.isEmpty())
            throw new IllegalArgumentException("typeName");

        return new BinaryObjectBuilderImpl(marsh.context(), typeName);
    }

    /** {@inheritDoc} */
    @Override public BinaryObjectBuilder builder(BinaryObject binaryObj) {
        if (binaryObj == null)
            throw new NullPointerException("binaryObj");

        return BinaryObjectBuilderImpl.wrap(binaryObj);
    }

    /** {@inheritDoc} */
    @Override public BinaryType type(Class<?> cls) throws BinaryObjectException {
        if (cls == null)
            throw new NullPointerException("cls");

        return type(cls.getName());
    }

    /** {@inheritDoc} */
    @Override public BinaryType type(String typeName) {
        if (typeName == null || typeName.isEmpty())
            throw new IllegalArgumentException("typeName");

        int typeId = marsh.context().typeId(typeName);

        return type(typeId);
    }

    /** {@inheritDoc} */
    @Override public BinaryType type(int typeId) {
        return marsh.context().metadata(typeId);
    }

    /** {@inheritDoc} */
    @Override public Collection<BinaryType> types() {
        return marsh.context().metadata();
    }

    /** {@inheritDoc} */
    @Override public BinaryObject buildEnum(String typeName, int ord) {
        if (typeName == null || typeName.isEmpty())
            throw new IllegalArgumentException("typeName");

        BinaryContext ctx = marsh.context();

        int typeId = ctx.typeId(typeName);

        return new BinaryEnumObjectImpl(ctx, typeId, null, ord);
    }

    /** {@inheritDoc} */
    @Override public BinaryObject buildEnum(String typeName, String name) {
        if (typeName == null || typeName.isEmpty())
            throw new IllegalArgumentException("typeName");

        if (name == null || name.isEmpty())
            throw new IllegalArgumentException("name");

        BinaryContext ctx = marsh.context();

        int typeId = ctx.typeId(typeName);

        BinaryMetadata metadata = ctx.metadata0(typeId);

        if (metadata == null)
            throw new BinaryObjectException(
                String.format("Failed to get metadata for type [typeId=%s, typeName='%s']", typeId, typeName)
            );

        Integer ordinal = metadata.getEnumOrdinalByName(name);

        if (ordinal == null)
            throw new BinaryObjectException(String.format(
                "Failed to resolve enum ordinal by name [typeId=%s, typeName='%s', name='%s']",
                typeId,
                typeName,
                name
            ));

        return new BinaryEnumObjectImpl(ctx, typeId, null, ordinal);
    }

    /** {@inheritDoc} */
    @Override public BinaryType registerEnum(String typeName, Map<String, Integer> vals) {
        if (typeName == null || typeName.isEmpty())
            throw new IllegalArgumentException("typeName");

        BinaryContext ctx = marsh.context();

        int typeId = ctx.typeId(typeName);

        ctx.updateMetadata(typeId, new BinaryMetadata(typeId, typeName, null, null, null, true, vals), false);

        return ctx.metadata(typeId);
    }

    /** {@inheritDoc} */
    @Override public BinaryType registerClass(Class<?> cls) throws BinaryObjectException {
        BinaryClassDescriptor clsDesc = marsh.context().registerClass(cls, true, false);

        return marsh.context().metadata(clsDesc.typeId());
    }


    /** {@inheritDoc} */
    @Override public void removeType(int typeId) throws IgniteException {
        marsh.marshallerContext().removeType(typeId);
    }
}
