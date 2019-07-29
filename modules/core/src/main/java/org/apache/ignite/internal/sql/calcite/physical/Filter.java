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
import org.apache.calcite.rex.RexNode;
import org.apache.ignite.internal.sql.calcite.expressions.Condition;
import org.jetbrains.annotations.NotNull;

import static org.apache.ignite.internal.sql.calcite.expressions.Condition.buildFilterCondition;

/**
 * TODO: Add class description.
 */
public class Filter extends PhysicalOperator {
    private final PhysicalOperator rowsSrc;
    private final Condition filterCondition;

    public Filter(PhysicalOperator rowsSrc, RexNode expression) {
        this.rowsSrc = rowsSrc;
        filterCondition = (Condition)buildFilterCondition(expression);
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



}
