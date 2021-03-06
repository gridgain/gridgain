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

package org.apache.ignite.internal.processors.platform.client.cache;

import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.internal.binary.BinaryRawWriterEx;
import org.apache.ignite.internal.processors.platform.client.ClientConnectionContext;
import org.apache.ignite.internal.processors.platform.client.ClientSqlUtils;

import java.util.List;

/**
 * Query cursor holder.
  */
class ClientCacheFieldsQueryCursor extends ClientCacheQueryCursor<List> {
    /** Column count. */
    private final int columnCount;

    /**
     * Ctor.
     *
     * @param cursor   Cursor.
     * @param pageSize Page size.
     * @param ctx      Context.
     */
    ClientCacheFieldsQueryCursor(FieldsQueryCursor<List> cursor, int pageSize, ClientConnectionContext ctx) {
        super(cursor, pageSize, ctx);

        columnCount = cursor.getColumnsCount();
    }

    /** {@inheritDoc} */
    @Override void writeEntry(BinaryRawWriterEx writer, List e) {
        assert e.size() == columnCount;

        for (Object obj : e)
            ClientSqlUtils.writeSqlField(writer, obj, getProtocolContext());
    }
}
