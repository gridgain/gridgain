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

package org.gridgain.action.query;

import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Query holder registry.
 */
public class QueryHolderRegistry implements AutoCloseable {
    /** Query holder check interval. */
    private static final Duration QUERY_HOLDER_CHECK_INTERVAL = Duration.ofSeconds(30);

    /** Context. */
    private final GridKernalContext ctx;

    /** Query holders. */
    private final ConcurrentMap<String, QueryHolder> qryHolders;

    /** Execute service. */
    private final ScheduledExecutorService execSrvc;

    /** Logger. */
    private final IgniteLogger log;

    /**
     * @param ctx Context.
     * @param holderTtl Holder ttl.
     */
    public QueryHolderRegistry(GridKernalContext ctx, Duration holderTtl) {
        this.ctx = ctx;
        log = ctx.log(QueryHolderRegistry.class);

        qryHolders = ctx.grid().cluster().nodeLocalMap();
        execSrvc = Executors.newScheduledThreadPool(1);

        execSrvc.scheduleAtFixedRate(() -> {
            try {
                while (!Thread.currentThread().isInterrupted()) {
                    for (Map.Entry<String, QueryHolder> e : qryHolders.entrySet()) {
                        if (e.getValue().isExpired(holderTtl.toMillis()))
                            cancelQuery(e.getKey());
                    }
                }
            }
            catch (Exception e) {
                log.warning("Failed to cleanup the registry.", e);
            }
        }, 0, QUERY_HOLDER_CHECK_INTERVAL.toMillis(), TimeUnit.MILLISECONDS);
    }

    /**
     * @param qryId Query ID.
     *
     * @return Created query holder.
     */
    public QueryHolder createQueryHolder(String qryId) {
        QueryHolder qryHolder = new QueryHolder(qryId);
        qryHolders.put(qryId, qryHolder);

        return qryHolder;
    }

    /**
     * @param qryId Query ID.
     * @param cursor Cursor.
     *
     * @return Craeted cursor holder.
     */
    public CursorHolder addCursor(String qryId, FieldsQueryCursor<List<?>> cursor) {
        String cursorId = UUID.randomUUID().toString();
        CursorHolder curHolder = new CursorHolder(cursorId, cursor, cursor.iterator());

        qryHolders.computeIfPresent(qryId, (k, v) -> {
            v.addCursor(curHolder);

            return v;
        });

        return curHolder;
    }

    /**
     * @param qryId Query ID.
     * @param cursorId Cursor ID.
     *
     * @return Cursor holder by query ID and cursor ID.
     */
    public CursorHolder findCursor(String qryId, String cursorId) {
        if (!qryHolders.containsKey(qryId))
            return null;

        QueryHolder qryHolder = qryHolders.get(qryId);
        qryHolder.updateAccessTime();

        return qryHolder.getCursor(cursorId);
    }

    /**
     * @param qryId Query ID.
     * @param cursorId Cursor ID.
     */
    public void closeQueryCursor(String qryId, String cursorId) {
        qryHolders.computeIfPresent(qryId, (k, v) -> {
            v.closeCursor(cursorId);

            return v;
        });
    }

    /**
     * @param qryId Query id.
     */
    public void cancelQuery(String qryId) {
        if (F.isEmpty(qryId))
            return;

        qryHolders.computeIfPresent(qryId, (k, v) -> {
            U.closeQuiet(v);

            return null;
        });
    }

    /** {@inheritDoc} */
    @Override public void close() throws Exception {
        execSrvc.shutdownNow();
    }
}
