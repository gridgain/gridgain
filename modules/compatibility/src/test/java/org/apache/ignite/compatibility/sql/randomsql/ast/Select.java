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

package org.apache.ignite.compatibility.sql.randomsql.ast;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.ignite.internal.util.typedef.F;

/**
 * SELECT query.
 */
public class Select implements Ast {
    /** */
    private List<Ast> cols = new ArrayList<>();

    /** */
    private Ast from;

    /** */
    private Ast where;

    /**
     * @param cols Column list.
     */
    public Select(Ast... cols) {
        this.cols.addAll(Arrays.asList(cols));
    }

    /**
     * Sets FROM expression.
     *
     * @param from From expression.
     */
    public Select from(Ast from) {
        this.from = from;

        return this;
    }

    /**
     * Sets WHERE expression.
     *
     * @param where From expression.
     */
    public Select where(Ast where) {
        this.where = where;

        return this;
    }

    /** {@inheritDoc} */
    @Override public void writeTo(StringBuilder out) {
        out.append("SELECT ");

        printColumnList(out);

        out.append(" FROM ");

        if (from != null)
            from.writeTo(out);

        out.append(" WHERE ");

        if (where != null)
            where.writeTo(out);
    }

    /**
     * @param out Out.
     */
    private void printColumnList(StringBuilder out) {
        if (F.isEmpty(cols)) {
            out.append("*");

            return;
        }

        for (int i = 0; i < cols.size(); i++) {
            cols.get(i).writeTo(out);

            if (i < cols.size() - 1)
                out.append(", ");
        }
    }
}
