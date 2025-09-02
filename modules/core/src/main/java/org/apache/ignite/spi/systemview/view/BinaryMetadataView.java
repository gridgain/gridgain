/*
 * Copyright 2024 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.spi.systemview.view;

import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.internal.binary.BinaryMetadata;
import org.apache.ignite.internal.binary.BinarySchema;
import org.apache.ignite.internal.managers.systemview.walker.Order;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * {@link BinaryMetadata} representation for the {@link SystemView}.
 */
public class BinaryMetadataView {
    /** Meta. */
    private final BinaryMetadata meta;

    /** @param meta Meta. */
    public BinaryMetadataView(BinaryMetadata meta) {
        this.meta = meta;
    }

    /** @return Type id. */
    @Order
    public int typeId() {
        return meta.typeId();
    }

    /** @return Type name. */
    @Order(1)
    public String typeName() {
        return meta.typeName();
    }

    /** @return Affinity key field name. */
    @Order(2)
    public String affKeyFieldName() {
        return meta.affinityKeyFieldName();
    }

    /** @return Fields count. */
    @Order(3)
    public int fieldsCount() {
        return meta.fields().size();
    }

    /** @return Fields. */
    @Order(4)
    public String fields() {
        return U.toStringSafe(meta.fields());
    }

    /** @return Schema IDs registered for this type. */
    @Order(5)
    public String schemasIds() {
        List<Integer> ids = new ArrayList<>(meta.schemas().size());

        for (BinarySchema schema : meta.schemas())
            ids.add(schema.schemaId());

        return U.toStringSafe(ids);
    }

    /** @return {@code True} if this is enum type. */
    @Order(6)
    public boolean isEnum() {
        return meta.isEnum();
    }

    /** @return {@code True} if this is system type. */
    @Order(7)
    public boolean system() {
        return meta.system();
    }
}
