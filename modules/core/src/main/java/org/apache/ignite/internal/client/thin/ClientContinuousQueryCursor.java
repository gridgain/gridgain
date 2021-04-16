/*
 * Copyright 2021 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.internal.client.thin;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.NotNull;

/**
 * Thin client continuous query cursor.
 */
class ClientContinuousQueryCursor<T> implements QueryCursor<T> {
    /** Initial query cursor. */
    private final QueryCursor<T> initQryCursor;

    /** Cache entry listener handler. */
    private final ClientCacheEntryListenerHandler<?, ?> lsnrHnd;

    /**
     * @param initQryCursor Initial query cursor.
     * @param lsnrHnd Cache entry listener handler.
     */
    ClientContinuousQueryCursor(QueryCursor<T> initQryCursor, ClientCacheEntryListenerHandler<?, ?> lsnrHnd) {
        this.initQryCursor = initQryCursor;
        this.lsnrHnd = lsnrHnd;
    }

    /** {@inheritDoc} */
    @Override public List<T> getAll() {
        return initQryCursor == null ? Collections.emptyList() : initQryCursor.getAll();
    }

    /** {@inheritDoc} */
    @Override public void close() {
        U.closeQuiet(initQryCursor);
        U.closeQuiet(lsnrHnd);
    }

    /** {@inheritDoc} */
    @NotNull @Override public Iterator<T> iterator() {
        return initQryCursor == null ? Collections.emptyIterator() : initQryCursor.iterator();
    }
}
