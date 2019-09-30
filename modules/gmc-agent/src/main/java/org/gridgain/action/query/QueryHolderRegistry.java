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
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.timeout.GridTimeoutObjectAdapter;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;

/**
 * Query holder registry.
 */
public class QueryHolderRegistry {
    /** Context. */
    private final GridKernalContext ctx;

    /** Query holders. */
    private final ConcurrentMap<String, QueryHolder> qryHolders;

    /** Logger. */
    private final IgniteLogger log;

    /** Holder ttl. */
    private final Duration holderTtl;

    /**
     * @param ctx Context.
     * @param holderTtl Holder ttl.
     */
    public QueryHolderRegistry(GridKernalContext ctx, Duration holderTtl) {
        this.ctx = ctx;
        this.holderTtl = holderTtl;
        log = ctx.log(QueryHolderRegistry.class);

        qryHolders = ctx.grid().cluster().nodeLocalMap();
    }

    /**
     * @param qryId Query ID.
     *
     * @return Created query holder.
     */
    public QueryHolder createQueryHolder(String qryId) {
        QueryHolder qryHolder = new QueryHolder(qryId);
        qryHolders.put(qryId, qryHolder);

        scheduleToRemove(qryId);

        return qryHolder;
    }

    /**
     * @param qryId Query ID.
     * @param cursorHolder Cursor.
     *
     * @return Saved cursor ID.
     */
    public String addCursor(String qryId, CursorHolder cursorHolder) {
        String cursorId = UUID.randomUUID().toString();

        qryHolders.computeIfPresent(qryId, (k, v) -> {
            v.addCursor(cursorId, cursorHolder);

            return v;
        });

        if (log.isDebugEnabled())
            log.debug("Cursor was addes to query holder [queryId=" + qryId + ", cursorId=" + cursorId + "]");

        return cursorId;
    }

    /**
     * @param qryId Query ID.
     * @return Cursor holder by query ID and cursor ID.
     */
    public CursorHolder findCursor(String qryId, String cursorId) {
        if (!qryHolders.containsKey(qryId))
            return null;

        QueryHolder qryHolder = qryHolders.get(qryId);
        qryHolder.setAccessed(true);

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

        if (log.isDebugEnabled())
            log.debug("Cursor was closed [queryId=" + qryId + ", cursorId=" + cursorId + "]");
    }

    /**
     * @param qryId Query id.
     */
    public void cancelQuery(String qryId) {
        if (F.isEmpty(qryId))
            return;

        qryHolders.computeIfPresent(qryId, (k, v) -> {
            if (log.isDebugEnabled())
                log.debug("Cancel query by id [queryId=" + qryId + "]");

            U.closeQuiet(v);

            return null;
        });
    }

    /**
     * @param qryId Query id.
     */
    private void scheduleToRemove(String qryId) {
        ctx.timeout().addTimeoutObject(new GridTimeoutObjectAdapter(holderTtl.toMillis()) {
            @Override public void onTimeout() {
                QueryHolder holder = qryHolders.get(qryId);

                if (holder != null) {
                    if (holder.isAccessed()) {
                        holder.setAccessed(false);

                        // Holder was accessed, we need to keep it for one more period.
                        scheduleToRemove(qryId);
                    }
                    else {
                        if (log.isDebugEnabled())
                            log.debug("Remove expire query holder, [queryId=" + qryId + "]");

                        // Remove stored query holder otherwise.
                        cancelQuery(qryId);
                    }
                }
            }
        });
    }
}
