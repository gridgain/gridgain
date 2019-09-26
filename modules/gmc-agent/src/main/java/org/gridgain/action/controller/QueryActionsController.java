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
import org.apache.ignite.internal.processors.query.GridQueryProcessor;
import org.apache.ignite.internal.util.typedef.F;
import org.gridgain.action.annotation.ActionController;
import org.gridgain.action.query.CursorHolder;
import org.gridgain.action.query.QueryHolder;
import org.gridgain.action.query.QueryHolderRegistry;
import org.gridgain.dto.action.query.NextPageQueryArgument;
import org.gridgain.dto.action.query.QueryArgument;
import org.gridgain.dto.action.query.QueryResult;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static java.util.Objects.requireNonNull;
import static org.apache.ignite.internal.managers.communication.GridIoPolicy.MANAGEMENT_POOL;
import static org.gridgain.utils.QueryUtils.fetchResult;
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
                String qryId = requireNonNull(arg.getQueryId(), "Failed to execute query due to empty query ID");
                String cursorId = requireNonNull(arg.getCursorId(), "Failed to execute query due to empty cursor ID");

                CursorHolder cursorHolder = qryRegistry.findCursor(qryId, cursorId);
                QueryResult res = fetchResult(cursorHolder, arg.getPageSize());

                if (!res.isHasMore())
                    qryRegistry.closeQueryCursor(qryId, cursorId);

                fut.complete(res);
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
    public CompletableFuture<List<QueryResult>> executeSqlQuery(QueryArgument arg) {
        final CompletableFuture<List<QueryResult>> fut = new CompletableFuture<>();

        ctx.closure().runLocalSafe(() -> {
            String qryId = arg.getQueryId();
            qryRegistry.cancelQuery(qryId);

            QueryHolder qryHolder = qryRegistry.createQueryHolder(qryId);

            try {
                if (log.isDebugEnabled())
                    log.debug("Execute query started with subject: " + ctx.security().securityContext().subject());

                SqlFieldsQuery qry = prepareQuery(arg);
                GridCacheContext cctx = F.isEmpty(arg.getCacheName())
                        ? null
                        : ctx.cache().cache(arg.getCacheName()).context();

                List<QueryResult> results = new ArrayList<>();
                for (FieldsQueryCursor<List<?>> cur : qryProc.querySqlFields(cctx, qry, null, true, false, qryHolder.getCancelHook())) {
                    CursorHolder cursorHolder = new CursorHolder(cur);
                    QueryResult res = fetchResult(cursorHolder, arg.getPageSize());
                    res.setResultNodeId(ctx.localNodeId().toString());

                    if (res.isHasMore())
                        res.setCursorId(qryRegistry.addCursor(qryId, cursorHolder));

                    results.add(res);
                }

                fut.complete(results);
            }
            catch (Throwable e) {
                log.warning("Failed to execute query.", e);

                qryRegistry.cancelQuery(qryId);

                fut.completeExceptionally(e);
            }
        }, MANAGEMENT_POOL);

        return fut;
    }
}
