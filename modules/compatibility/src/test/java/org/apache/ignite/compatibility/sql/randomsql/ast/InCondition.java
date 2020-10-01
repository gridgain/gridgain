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

/**
 * IN condition.
 */
public class InCondition implements Ast {
    /** */
    private final Ast left;

    /** */
    private final Ast[] right;

    /**
     * @param left Left operand.
     * @param right Possible values.
     */
    @SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType")
    public InCondition(Ast left, Ast[] right) {
        this.left = left;
        this.right = right;
    }

    /** {@inheritDoc} */
    @Override public void writeTo(StringBuilder out) {
        out.append('(');

        left.writeTo(out);

        out.append(" IN (");

        for (int i = 0; i < right.length; i++) {
            right[i].writeTo(out);

            if (i < right.length - 1)
                out.append(", ");
        }

        out.append("))");
    }
}
