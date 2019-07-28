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
package org.apache.ignite.internal.sql.calcite.physical;

import java.util.Iterator;
import java.util.List;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlBinaryOperator;
import org.apache.calcite.sql.SqlOperator;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.sql.calcite.expressions.BinaryCondition;
import org.apache.ignite.internal.sql.calcite.expressions.Condition;
import org.apache.ignite.internal.sql.calcite.expressions.Constant;
import org.apache.ignite.internal.sql.calcite.expressions.Expression;
import org.apache.ignite.internal.sql.calcite.expressions.FieldGetter;
import org.jetbrains.annotations.NotNull;

/**
 * TODO: Add class description.
 */
public class Filter extends PhysicalOperator {
    private final PhysicalOperator rowsSrc;
    private final Condition filterCondition;

    public Filter(PhysicalOperator rowsSrc, RexNode expression) {
        this.rowsSrc = rowsSrc;
        filterCondition = (Condition)buildFilterExpression(expression);
    }


    @NotNull @Override public Iterator<List<?>> iterator() {
        Iterator<List<?>> srcIt = rowsSrc.iterator();

        return new Iterator<List<?>>() {
            private List<?> cur = findNext();

            @Override public boolean hasNext() {
                return cur != null;
            }

            @Override public List<?> next() {
                List<?> res = cur;

                cur = findNext();

                return res;
            }

            private List<?> findNext() {
                while (srcIt.hasNext()) {
                    List<?> r = srcIt.next();

                    if (filterCondition.evaluate(r)) {
                        return r;
                    }
                }

                return null;
            }
        };
    }


    private Expression buildFilterExpression(RexNode expression) {
        if (expression instanceof RexCall) {
            RexCall e = (RexCall)expression;

            SqlOperator sqlOp = e.getOperator();

            if (sqlOp instanceof SqlBinaryOperator) {
                SqlBinaryOperator op = (SqlBinaryOperator)sqlOp;

                RexNode lOp = e.getOperands().get(0);
                RexNode rOp = e.getOperands().get(1);

                Expression lExp = buildFilterExpression(lOp);
                Expression rExp = buildFilterExpression(rOp);

                return new BinaryCondition(op.getKind(), lExp, rExp);
            }

            throw new IgniteException("Unsupported operator: " + sqlOp);
        }
        else if (expression instanceof RexInputRef)
            return new FieldGetter(((RexInputRef)expression).getIndex());
        else if (expression instanceof RexLiteral)
            return new Constant((RexLiteral)expression);

        throw new IgniteException("Unsupported filter condition: " + expression);
    }
}
