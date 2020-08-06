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
import java.util.List;

/**
 * Table list.
 */
public class TableList implements Ast {
    /** */
    private final List<TableRef> tbls;

    /**
     * @param tbls Tables.
     */
    public TableList(List<TableRef> tbls) {
        this.tbls = new ArrayList<>(tbls);
    }

    /** {@inheritDoc} */
    @Override public void writeTo(StringBuilder out) {
        out.append(' ');

        for (int i = 0; i < tbls.size(); i++) {
            tbls.get(i).writeTo(out);

            if (i < tbls.size() - 1)
                out.append(", ");
        }

        out.append(' ');
    }
}
