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

package org.gridgain.action.controller;

import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.query.QueryCursorEx;
import org.apache.ignite.internal.processors.query.GridQueryFieldMetadata;
import org.apache.ignite.internal.processors.query.GridQueryProcessor;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.gridgain.action.annotation.ActionController;
import org.gridgain.action.query.CursorHolder;
import org.gridgain.action.query.QueryHolder;
import org.gridgain.action.query.QueryHolderRegistry;
import org.gridgain.dto.action.query.NextPageQueryArgument;
import org.gridgain.dto.action.query.QueryArgument;
import org.gridgain.dto.action.query.QueryResult;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.apache.ignite.internal.managers.communication.GridIoPolicy.MANAGEMENT_POOL;
import static org.gridgain.utils.QueryUtils.fetchSqlQueryRows;
import static org.gridgain.utils.QueryUtils.getColumns;
import static org.gridgain.utils.QueryUtils.prepareQuery;

/**
 * Query actions controller.
 */
@ActionController("QueryActions")
public class QueryActionsController {
    /** Context. */
    private final GridKernalContext ctx;

    /** Query registry. */
    private final QueryHolderRegistry qryRegistry;

    /** Query process. */
    private GridQueryProcessor qryProc;

    /** Logger. */
    private IgniteLogger log;

    /**
     * @param ctx Context.
     */
    public QueryActionsController(GridKernalContext ctx) {
        this.ctx = ctx;
        this.log = ctx.log(QueryActionsController.class);

        qryProc = ctx.query();
        qryRegistry = new QueryHolderRegistry(ctx, Duration.ofMinutes(5));
    }

    /**
     * Cancel query by query ID.
     *
     * @param qryId Query id.
     */
    public CompletableFuture<Void> cancel(String qryId) {
        qryRegistry.cancelQuery(qryId);

        return CompletableFuture.completedFuture(null);
    }

    /**
     * @param arg Argument.
     * @return Next page with result.
     */
    public CompletableFuture<QueryResult> nextPage(NextPageQueryArgument arg) {
        final CompletableFuture<QueryResult> fut = new CompletableFuture<>();

        ctx.closure().runLocalSafe(() -> {
            try {
                fut.complete(fetchResult(arg.getQueryId(), arg.getCursorId(), arg.getPageSize()));
            }
            catch (Throwable e) {
                fut.completeExceptionally(e);
            }
        }, MANAGEMENT_POOL);

        return fut;
    }

    /**
     * @param arg Argument.
     * @return List of query results.
     */
    public CompletableFuture<List<QueryResult>> execute(QueryArgument arg) {
        final CompletableFuture<List<QueryResult>> fut = new CompletableFuture<>();

        ctx.closure().runLocalSafe(() -> {
            String qryId = arg.getQueryId();
            qryRegistry.cancelQuery(qryId);

            QueryHolder qryHolder = qryRegistry.createQueryHolder(qryId);

            try {
                if (log.isDebugEnabled())
                    log.debug("Operation started with subject: " + ctx.security().securityContext().subject());

                SqlFieldsQuery qry = prepareQuery(arg);
                GridCacheContext cctx = !F.isEmpty(arg.getCacheName())
                        ? ctx.cache().cache(arg.getCacheName()).context()
                        : null;

                List<QueryResult> results = new ArrayList<>();
                for (FieldsQueryCursor<List<?>> cur : qryProc.querySqlFields(cctx, qry, null, true, false, qryHolder.getCancelHook())) {
                    CursorHolder curHolder = qryRegistry.addCursor(qryId, cur);
                    results.add(fetchResult(qryId, curHolder.getCursorId(), arg.getPageSize()));
                }

                fut.complete(results);
            }
            catch (Throwable e) {
                log.warning("Fail to execute query.", e);

                qryRegistry.cancelQuery(qryId);

                fut.completeExceptionally(e);
            }
        }, MANAGEMENT_POOL);

        return fut;
    }

    /**
     * @param qryId Query id.
     * @param cursorId Cursor id.
     * @param pageSize Page size.
     * @return Query result.
     */
    private QueryResult fetchResult(String qryId, String cursorId, int pageSize) {
        try {
            if (F.isEmpty(qryId))
                throw new IllegalArgumentException("Fail to execute query - query id can't be empty.");

            if (F.isEmpty(cursorId))
                throw new IllegalArgumentException("Fail to execute query - cursor id can't be empty.");

            CursorHolder curHolder = qryRegistry.findCursor(qryId, cursorId);
            if (curHolder == null)
                throw new IllegalArgumentException("Fail to execute query - can't find a cursor by id, [cursorId=" + cursorId + "]");

            long start = U.currentTimeMillis();

            List<GridQueryFieldMetadata> meta = ((QueryCursorEx)curHolder.getCursor()).fieldsMeta();

            if (meta == null)
                throw new IllegalArgumentException("Fail to execute query. No metadata available.");
            else {
                Iterator<List<?>> qryIter = curHolder.getIterator();
                List<Object[]> rows = fetchSqlQueryRows(qryIter, pageSize);
                boolean hasMore = qryIter.hasNext();

                if (!hasMore)
                    qryRegistry.closeQueryCursor(qryId, cursorId);

                return new QueryResult()
                    .setCursorId(cursorId)
                    .setHasMore(hasMore)
                    .setColumns(getColumns(meta))
                    .setRows(rows)
                    .setResultNodeId(ctx.localNodeId().toString())
                    .setDuration(U.currentTimeMillis() - start);
            }
        }
        catch (Throwable e) {
            throw e;
        }
    }
}
