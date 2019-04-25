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

package org.apache.ignite.internal.processors.platform.cache.query;

import javax.cache.Cache;
import org.apache.ignite.internal.binary.BinaryRawWriterEx;
import org.apache.ignite.internal.processors.cache.query.QueryCursorEx;
import org.apache.ignite.internal.processors.platform.PlatformContext;

/**
 * Interop cursor for regular queries.
 */
public class PlatformQueryCursor extends PlatformAbstractQueryCursor<Cache.Entry> {
    /**
     * Constructor.
     *
     * @param platformCtx Context.
     * @param cursor Backing cursor.
     * @param batchSize Batch size.
     */
    public PlatformQueryCursor(PlatformContext platformCtx, QueryCursorEx<Cache.Entry> cursor, int batchSize) {
        super(platformCtx, cursor, batchSize);
    }

    /** {@inheritDoc} */
    @Override protected void write(BinaryRawWriterEx writer, Cache.Entry val) {
        writer.writeObjectDetached(val.getKey());
        writer.writeObjectDetached(val.getValue());
    }
}
