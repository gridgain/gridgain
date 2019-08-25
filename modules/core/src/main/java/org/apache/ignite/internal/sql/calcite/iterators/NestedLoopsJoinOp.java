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
package org.apache.ignite.internal.sql.calcite.iterators;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.sql.calcite.expressions.Condition;
import org.apache.ignite.lang.IgniteInClosure;
import org.jetbrains.annotations.NotNull;

/**
 * TODO: Add class description.
 */
public class NestedLoopsJoinOp extends PhysicalOperator {
    private final PhysicalOperator leftSrc;
    private final PhysicalOperator rightSrc;
    private final ImmutableIntList leftJoinKeys;
    private final ImmutableIntList rightJoinKeys;
    private final Condition joinCond;
    private final JoinRelType joinType;

    public NestedLoopsJoinOp(PhysicalOperator leftSrc,
        PhysicalOperator rightSrc,
        ImmutableIntList leftJoinKeys,
        ImmutableIntList rightJoinKeys,
        Condition joinCond,
        JoinRelType joinType) {
        this.leftSrc = leftSrc;
        this.rightSrc = rightSrc;
        this.leftJoinKeys = leftJoinKeys;
        this.rightJoinKeys = rightJoinKeys;
        this.joinCond = joinCond;
        this.joinType = joinType;

        assert leftJoinKeys.size() == rightJoinKeys.size() : "right=" + leftJoinKeys.size() + ",left=" + rightJoinKeys.size();

        if (joinType != JoinRelType.INNER)
            throw new IgniteException("Unsupported join type: " + joinType);

    }

    @Override public void init() {
        leftSrc.listen(new IgniteInClosure<IgniteInternalFuture<List<List<?>>>>() {
            @Override public void apply(IgniteInternalFuture<List<List<?>>> leftFut) {
                rightSrc.listen(new IgniteInClosure<IgniteInternalFuture<List<List<?>>>>() {
                    @Override public void apply(IgniteInternalFuture<List<List<?>>> rightFut) {
                        try {
                            List<List<?>> left = leftFut.get();
                            List<List<?>> right = rightFut.get();

                            execute(left, right);
                        }
                        catch (IgniteCheckedException e) {
                            onDone(e);
                        }
                    }
                });
            }
        });

        leftSrc.init();
        rightSrc.init();
    }

    @NotNull @Override public Iterator<List<?>> iterator(List<List<?>>... input) {
        return new Iterator<List<?>>() {
            private List<List<?>> leftRows = input[0];
            private List<List<?>> rightRows = input[1];

            private List<List<?>> result = new ArrayList<>();
            private Iterator<List<?>> resIt;

            {
                for (List<?> leftRow : leftRows) {
                    for (List<?> rightRow : rightRows) {
                        List joinedRow = joinRows(leftRow, rightRow);

                        if (leftJoinKeys.isEmpty() || joinCond.evaluate(joinedRow))
                            result.add(joinedRow);
                    }
                }

                resIt = result.iterator();
            }

            @Override public boolean hasNext() {
                return resIt.hasNext();
            }

            @Override public List<?> next() {
                return resIt.next();
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
