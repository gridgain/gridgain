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

import java.util.concurrent.TimeUnit;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.internal.binary.BinaryRawReaderEx;
import org.apache.ignite.internal.processors.platform.client.ClientConnectionContext;
import org.apache.ignite.internal.processors.platform.client.ClientProtocolContext;
import org.apache.ignite.internal.processors.platform.client.ClientResponse;
import org.apache.ignite.internal.processors.platform.client.ClientSqlUtils;
import org.apache.ignite.internal.processors.platform.client.tx.ClientTxAwareRequest;

/**
 * Sql query request.
 */
@SuppressWarnings("unchecked")
public class ClientCacheSqlQueryRequest extends ClientCacheDataRequest implements ClientTxAwareRequest {
    /** Query. */
    private final SqlQuery qry;

    /**
     * Ctor.
     *
     * @param reader Reader.
     */
    public ClientCacheSqlQueryRequest(BinaryRawReaderEx reader, ClientProtocolContext protocolCtx) {
        super(reader);

        qry = new SqlQuery(reader.readString(), reader.readString())
                .setArgs(ClientSqlUtils.readQueryArgs(reader, protocolCtx))
                .setDistributedJoins(reader.readBoolean())
                .setLocal(reader.readBoolean())
                .setReplicatedOnly(reader.readBoolean())
                .setPageSize(reader.readInt())
                .setTimeout((int) reader.readLong(), TimeUnit.MILLISECONDS);
    }

    /** {@inheritDoc} */
    @Override public ClientResponse process(ClientConnectionContext ctx) {
        IgniteCache<Object, Object> cache = cache(ctx);

        ctx.incrementCursors();

        try {
            QueryCursor cur = cache.query(qry);

            ClientCacheEntryQueryCursor cliCur = new ClientCacheEntryQueryCursor(
                    cur, qry.getPageSize(), ctx);

            long cursorId = ctx.resources().put(cliCur);

            cliCur.id(cursorId);

            return new ClientCacheQueryResponse(requestId(), cliCur);
        }
        catch (Throwable e) {
            ctx.decrementCursors();

            throw e;
        }
    }
}
