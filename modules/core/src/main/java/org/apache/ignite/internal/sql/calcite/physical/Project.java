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
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.ignite.IgniteException;
import org.jetbrains.annotations.NotNull;

/**
 * TODO: Add class description.
 */
public class Project extends PhysicalOperator {

    private final PhysicalOperator rowsSrc;
    private final List<RexNode> projects;

    public Project(PhysicalOperator rowsSrc, List<RexNode> projects) {
        this.rowsSrc = rowsSrc;
        this.projects = projects;
    }

    @NotNull @Override public Iterator<List<?>> iterator() {
        Iterator<List<?>> srcIter = rowsSrc.iterator();
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
