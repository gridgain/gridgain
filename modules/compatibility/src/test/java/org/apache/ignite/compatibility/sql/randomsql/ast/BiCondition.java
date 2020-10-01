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
 * Binary condition.
 */
public class BiCondition implements Ast {
    /** */
    private final Ast left;

    /** */
    private final Ast right;

    /** */
    private final Operator op;

    /**
     * @param left Left operand.
     * @param right Right operand.
     * @param op Operator.
     */
    public BiCondition(Ast left, Ast right, Operator op) {
        this.left = left;
        this.right = right;
        this.op = op;
    }

    /** {@inheritDoc} */
    @Override public void writeTo(StringBuilder out) {
        out.append('(');

        left.writeTo(out);

        out.append(' ');

        op.writeTo(out);

        out.append(' ');

        right.writeTo(out);

        out.append(')');
    }
}
