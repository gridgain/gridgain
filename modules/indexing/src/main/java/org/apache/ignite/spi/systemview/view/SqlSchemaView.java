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

package org.apache.ignite.spi.systemview.view;

import org.apache.ignite.internal.managers.systemview.walker.Order;
import org.apache.ignite.internal.processors.query.h2.H2Schema;

/**
 * Sql schema system view representation.
 */
public class SqlSchemaView {
    /** H2 schema. */
    private final H2Schema schema;

    /**
     * @param schema H2 schema.
     */
    public SqlSchemaView(H2Schema schema) {
        this.schema = schema;
    }

    /** @return Schema name. */
    @Order
    public String name() {
        return schema.schemaName();
    }

    /** @return {@code True} if schema is predefined, {@code false} otherwise. */
    public boolean predefined() {
        return schema.predefined();
    }
}
