/*
 * Copyright 2020 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.compatibility.sql.randomsql;

import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.compatibility.sql.randomsql.ast.TableRef;

/**
 * Context for building randomised query.
 */
class RandomisedQueryContext {
    /** Table available for column references. */
    private final List<TableRef> scopeTbls;

    /** Current schema. */
    private Schema schema;

    /**
     * @param schema Schema.
     */
    public RandomisedQueryContext(Schema schema) {
        this.schema = schema;

        scopeTbls = new ArrayList<>();
    }

    /**
     * Returns current schema this context were created with.
     *
     * @return Schema of this context.
     */
    public Schema schema() {
        return schema;
    }

    /**
     * Returns all tables in the scope.
     *
     * @return List of the available tables.
     */
    public List<TableRef> scopeTables() {
        return scopeTbls;
    }

    /**
     * Add table to the scope.
     *
     * @param tblRef Table.
     */
    public void addScopeTable(TableRef tblRef) {
        scopeTbls.add(tblRef);
    }
}
