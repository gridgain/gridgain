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

package org.apache.ignite.internal.processors.query.h2;

import org.h2.engine.Session;
import org.h2.expression.Expression;
import org.h2.result.H2BaseLocalResult;
import org.h2.value.Value;

/**
 * H2 local result with memory tracker.
 */
public class H2ManagedLocalResult extends H2BaseLocalResult {
    /** Query memory tracker. */
    private QueryMemoryTracker mem;

    /** Allocated memory. */
    private long allocMem;

    /**
     * Constructor.
     *
     * @param ses the session
     * @param memTracker Query memory tracker.
     * @param expressions the expression array
     * @param visibleColCnt the number of visible columns
     */
    public H2ManagedLocalResult(Session ses, QueryMemoryTracker memTracker, Expression[] expressions,
        int visibleColCnt) {
        super(ses, expressions, visibleColCnt);

        this.mem = memTracker;
    }

    /** {@inheritDoc} */
    @Override protected void onUpdate(Value[] oldRow, Value[] row) {
        if (oldRow != null) {
            for (Value v : oldRow) {
                int size = v.getMemory();

                allocMem -= size;

                mem.free(size);
            }
        }

        for (Value v : row) {
            int size = v.getMemory();

            allocMem += size;

            mem.allocate(size);
        }
    }

    /** {@inheritDoc} */
    @Override public void close() {
        super.close();

        mem.free(allocMem);
    }
}
