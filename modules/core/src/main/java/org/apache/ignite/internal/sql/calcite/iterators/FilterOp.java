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

import java.util.Iterator;
import java.util.List;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.sql.calcite.expressions.Condition;
import org.apache.ignite.lang.IgniteInClosure;

/**
 * TODO: Add class description.
 */
public class FilterOp extends PhysicalOperator {
    private final PhysicalOperator rowsSrc;
    private final Condition filterCondition;

    public FilterOp(PhysicalOperator rowsSrc, Condition filterCondition) {
        this.rowsSrc = rowsSrc;
        this.filterCondition = filterCondition;

    }

    @Override public Iterator<List<?>> iterator(List<List<?>> ... input) {
        Iterator<List<?>> srcIt = input[0].iterator();

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

    @Override public void init() {
        rowsSrc.listen(new IgniteInClosure<IgniteInternalFuture<List<List<?>>>>() {
            @Override public void apply(IgniteInternalFuture<List<List<?>>> fut) {
                try {
                    List<List<?>> input = fut.get();

                    execute(input);
                }
                catch (IgniteCheckedException e) {
                    onDone(e);
                }
            }
        });

        rowsSrc.init();
    }

}
