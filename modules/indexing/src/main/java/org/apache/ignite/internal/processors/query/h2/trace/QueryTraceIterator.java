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

package org.apache.ignite.internal.processors.query.h2.trace;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Deserializes binary objects if needed.
 */
public class QueryTraceIterator implements Iterator<List<?>>, AutoCloseable {
    /** */
    private final Iterator<List<?>> iter;

    /** */
    private final IgniteH2SqlTrace trace;

    /** */
    private boolean hasNext = true;

    /**
     * @param iter Iterator.
     */
    public QueryTraceIterator(Iterator<List<?>> iter, IgniteH2SqlTrace trace) {
        this.iter = iter;
        this.trace = trace;
        try {
            while (iter.hasNext())
                iter.next();
        }
        finally {
            if (iter instanceof AutoCloseable)
                U.closeQuiet((AutoCloseable)iter);

            U.closeQuiet(this.trace);
        }
    }

    /** {@inheritDoc} */
    @Override public void close() throws Exception {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public boolean hasNext() {
        return hasNext;
    }

    /** {@inheritDoc} */
    @Override public List<?> next() {
        hasNext = false;

        return Collections.singletonList(trace.toString());
    }
}
