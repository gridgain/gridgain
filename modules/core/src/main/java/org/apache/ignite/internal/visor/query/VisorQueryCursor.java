/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.visor.query;

import java.util.Collection;
import java.util.Iterator;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.internal.processors.cache.QueryCursorImpl;
import org.apache.ignite.internal.processors.query.GridQueryFieldMetadata;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Wrapper for query cursor.
 */
public class VisorQueryCursor<T> implements Iterator<T>, AutoCloseable {
    /** */
    private final QueryCursor<T> cur;

    /** */
    private final Iterator<T> itr;

    /** Flag indicating that this cursor was read from last check. */
    private volatile boolean accessed;

    /**
     * @param cur Cursor.
     */
    public VisorQueryCursor(QueryCursor<T> cur) {
        this.cur = cur;

        itr = cur.iterator();
    }

    /** {@inheritDoc} */
    @Override public boolean hasNext() {
        return itr.hasNext();
    }

    /** {@inheritDoc} */
    @Override public T next() {
        return itr.next();
    }

    /** {@inheritDoc} */
    @Override public void remove() {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public void close() {
        cur.close();
    }

    /**
     * @return SQL Fields query result metadata.
     */
    @SuppressWarnings("unchecked")
    public Collection<GridQueryFieldMetadata> fieldsMeta() {
        return ((QueryCursorImpl)cur).fieldsMeta();
    }

    /**
     * @return Flag indicating that this future was read from last check..
     */
    public boolean accessed() {
        return accessed;
    }

    /**
     * @param accessed New accessed.
     */
    public void accessed(boolean accessed) {
        this.accessed = accessed;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorQueryCursor.class, this);
    }
}