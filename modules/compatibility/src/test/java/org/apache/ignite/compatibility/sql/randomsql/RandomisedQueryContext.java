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
import java.util.Objects;
import org.apache.ignite.compatibility.sql.randomsql.ast.TableRef;

/**
 * Context for building randomised query.
 */
@SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType")
class RandomisedQueryContext {
    /** Context of parent query. Used to build current query as a subquery. */
    private final RandomisedQueryContext parentCtx;

    /** Table available for column references. */
    private final List<TableRef> scopeTbls = new ArrayList<>();

    /** Query params. */
    private final List<Object> params = new ArrayList<>();

    /** */
    public RandomisedQueryContext() {
        parentCtx = null;
    }

    /**
     * Use this constructor to create context for subquery.
     *
     * @param parentCtx Parent context.
     */
    public RandomisedQueryContext(RandomisedQueryContext parentCtx) {
        this.parentCtx = Objects.requireNonNull(parentCtx, "parentCtx");
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

    /**
     * Add param to the query.
     *
     * @param obj Object.
     */
    public void addQueryParam(Object obj) {
        params.add(obj);
    }

    /**
     * @return Params for prepared statement.
     */
    public List<Object> queryParams() {
        return params;
    }

    /**
     * @return Context of parent query.
     */
    public RandomisedQueryContext parentContext() {
        return parentCtx;
    }
}
