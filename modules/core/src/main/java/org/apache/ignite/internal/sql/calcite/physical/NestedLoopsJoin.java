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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.sql.calcite.expressions.Condition;
import org.jetbrains.annotations.NotNull;

import static org.apache.ignite.internal.sql.calcite.expressions.Condition.buildFilterCondition;

/**
 * TODO: Add class description.
 */
public class NestedLoopsJoin extends PhysicalOperator {
    private final PhysicalOperator leftSrc;
    private final PhysicalOperator rightSrc;
    private final ImmutableIntList leftJoinKeys;
    private final ImmutableIntList rightJoinKeys;
    private final Condition joinCond;
    private final JoinRelType joinType;

    public NestedLoopsJoin(PhysicalOperator leftSrc,
        PhysicalOperator rightSrc,
        ImmutableIntList leftJoinKeys,
        ImmutableIntList rightJoinKeys,
        RexNode joinCond,
        JoinRelType joinType) {
        this.leftSrc = leftSrc;
        this.rightSrc = rightSrc;
        this.leftJoinKeys = leftJoinKeys;
        this.rightJoinKeys = rightJoinKeys;
        this.joinCond = (Condition)buildFilterCondition(joinCond);
        this.joinType = joinType;

        assert leftJoinKeys.size() == rightJoinKeys.size() : "right=" + leftJoinKeys.size() + ",left=" + rightJoinKeys.size();

        if (joinType != JoinRelType.INNER)
            throw new IgniteException("Unsupported join type: " + joinType);

    }

    @NotNull @Override public Iterator<List<?>> iterator() {
        return new Iterator<List<?>>() {
            private Iterator<List<?>> leftIt = leftSrc.iterator();
            private Iterator<List<?>> rightIt = rightSrc.iterator();

            private List<?> curLeft;
            private List<?> curRight;

            // private List<?> curRow;

            {
                advance();
            }

            @Override public boolean hasNext() {
                assert (curLeft == null) == (curRight == null);

                return curLeft != null;
            }

            @Override public List<?> next() {
                if (curLeft == null)
                    throw new NoSuchElementException();

                List res = joinRows(curLeft, curRight);

                advance();

                return res;
            }

            private void advance() {
                while (leftIt.hasNext()) {
                    if (curLeft == null)
                        curLeft = leftIt.next();

                    while (rightIt.hasNext()) {
                        curRight = rightIt.next();

                        List<?> joinedRow = joinRows(curLeft, curRight); // TODO Refactor to not join rows twice.

                        // Empty join keys means Cartesian product.
                        if (leftJoinKeys.isEmpty() || joinCond.evaluate(joinedRow))
                            return;
                    }

                    curLeft = null;
                    rightIt = rightSrc.iterator();
                }

                curLeft = null;
                curRight = null;
            }
        };
    }

    private List<?> joinRows(List l, List r) {
        List<?> res = new ArrayList<>(l.size() + r.size());

        res.addAll(l);
        res.addAll(r);

        return res;
    }
}
