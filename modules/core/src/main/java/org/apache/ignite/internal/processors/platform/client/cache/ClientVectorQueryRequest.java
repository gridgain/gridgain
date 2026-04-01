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

import org.apache.ignite.IgniteCache;
import org.apache.ignite.binary.BinaryRawReader;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.VectorQuery;
import org.apache.ignite.internal.processors.platform.client.ClientConnectionContext;
import org.apache.ignite.internal.processors.platform.client.ClientResponse;

/**
 * Vector query request.
 */
public class ClientVectorQueryRequest extends ClientCacheRequest {
    /** Page size. */
    private final int pageSize;

    /** Vector query. */
    private final VectorQuery qry;

    /**
     * Creates an instance of request.
     *
     * @param reader Reader.
     */
    public ClientVectorQueryRequest(BinaryRawReader reader) {
        super(reader);

        pageSize = reader.readInt();

        String type = reader.readString();
        String field = reader.readString();
        float[] clauseVector = reader.readFloatArray();
        int k = reader.readInt();
        float threshold = reader.readFloat();

        qry = new VectorQuery(type, field, clauseVector, k, threshold);
    }

    /** {@inheritDoc} */
    @Override public ClientResponse process(ClientConnectionContext ctx) {
        long t0 = System.nanoTime();

        IgniteCache<Object, Object> cache = cache(ctx);

        long t1 = System.nanoTime();

        try {
            QueryCursor cur = cache.query(qry);

            long t2 = System.nanoTime();

            ClientCacheEntryQueryCursor cliCur = new ClientCacheEntryQueryCursor(cur, pageSize, ctx);

            long cursorId = ctx.resources().put(cliCur);

            cliCur.id(cursorId);

            long t3 = System.nanoTime();

            org.apache.ignite.IgniteLogger log = ctx.kernalContext().log(ClientVectorQueryRequest.class);
            if (log.isTraceEnabled())
                log.trace("CVQ getCache=" + (t1 - t0) / 1000 + "us query=" + (t2 - t1) / 1000 +
                    "us cursor=" + (t3 - t2) / 1000 + "us total=" + (t3 - t0) / 1000 + "us");

            return new ClientCacheQueryResponse(requestId(), cliCur);
        }
        catch (Throwable e) {
            ctx.decrementCursors();

            throw e;
        }
    }
}
