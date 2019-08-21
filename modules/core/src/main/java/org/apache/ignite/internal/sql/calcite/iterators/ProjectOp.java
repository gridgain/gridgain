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
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.lang.IgniteInClosure;

/**
 * TODO: Add class description.
 */
public class ProjectOp extends PhysicalOperator {

    private final List<RexNode> projects;

    public ProjectOp(PhysicalOperator rowsSrc, List<RexNode> projects) {
        this.projects = projects;

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
    }

    @Override public Iterator<List<?>> iterator(List<List<?>> ... input) {
        Iterator<List<?>> srcIter = input[0].iterator();
        return new Iterator<List<?>>() {
            @Override public boolean hasNext() {
                return srcIter.hasNext(); // TODO: CODE: implement.
            }

            @Override public List<?> next() {
                List<?> srcRow = srcIter.next();

                List projectedRow = new ArrayList<>(projects.size());

                for (RexNode projection : projects) {
                    if (!(projection instanceof RexInputRef))
                        throw new IgniteException("Unsupported projection type: " + projection.getClass());

                    int colId = ((RexInputRef)projection).getIndex();

                    Object obj = srcRow.get(colId);
                    projectedRow.add(obj);
                }

                return projectedRow; // TODO: CODE: implement.
            }
        }; // TODO: CODE: implement.
    }
}
