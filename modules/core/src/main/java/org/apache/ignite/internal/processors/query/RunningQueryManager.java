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

package org.apache.ignite.internal.processors.query;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.SqlConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.managers.communication.GridIoPolicy;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryType;
import org.apache.ignite.internal.processors.closure.GridClosureProcessor;
import org.apache.ignite.internal.processors.metric.MetricRegistry;
import org.apache.ignite.internal.processors.metric.impl.AtomicLongMetric;
import org.apache.ignite.internal.processors.metric.impl.LongAdderMetric;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.systemview.view.SqlQueryHistoryView;
import org.apache.ignite.spi.systemview.view.SqlQueryView;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.cache.query.GridCacheQueryType.SQL;
import static org.apache.ignite.internal.processors.cache.query.GridCacheQueryType.SQL_FIELDS;
import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.metricName;

/**
 * Keep information about all running queries.
 */
public class RunningQueryManager {
    /** */
    public static final String SQL_QRY_VIEW = metricName("sql", "queries");

    /** */
    public static final String SQL_QRY_VIEW_DESC = "Running SQL queries.";

    /** */
    public static final String SQL_QRY_HIST_VIEW = metricName("sql", "queries", "history");

    /** */
    public static final String SQL_QRY_HIST_VIEW_DESC = "SQL queries history.";

    /** Name of the MetricRegistry which metrics measure stats of queries initiated by user. */
    public static final String SQL_USER_QUERIES_REG_NAME = "sql.queries.user";

    /** Dummy memory metric provider that returns only -1's. */
    // This provider used to highlight that query has no tracker at all.
    // It could be intentionally in case of streaming or text queries
    // and occasionally in case of uncounted circumstances
    // that requires followed investigation
    private static final GridQueryMemoryMetricProvider DUMMY_TRACKER = new GridQueryMemoryMetricProvider() {
        @Override public long reserved() {
            return -1;
        }

        @Override public long maxReserved() {
            return -1;
        }

        @Override public long writtenOnDisk() {
            return -1;
        }

        @Override public long maxWrittenOnDisk() {
            return -1;
        }

        @Override public long totalWrittenOnDisk() {
            return -1;
        }
    };

    /** */
    private final IgniteLogger log;

    /** */
    private final GridClosureProcessor closure;

    /** Keep registered user queries. */
    private final ConcurrentMap<Long, GridRunningQueryInfo> runs = new ConcurrentHashMap<>();

    /** Unique id for queries on single node. */
    private final AtomicLong qryIdGen = new AtomicLong();

    /** Local node ID. */
    private final UUID locNodeId;

    /** History size. */
    private final int histSz;

    /** Query history tracker. */
    private volatile QueryHistoryTracker qryHistTracker;

    /** Number of successfully executed queries. */
    private final LongAdderMetric successQrsCnt;

    /** Number of failed queries in total by any reason. */
    private final AtomicLongMetric failedQrsCnt;

    /**
     * Number of canceled queries. Canceled queries a treated as failed and counting twice: here and in {@link
     * #failedQrsCnt}.
     */
    private final AtomicLongMetric canceledQrsCnt;

    /**
     * Number of queries, failed due to OOM protection. {@link #failedQrsCnt} metric includes this value.
     */
    private final AtomicLongMetric oomQrsCnt;

    /** */
    private final List<Consumer<GridQueryStartedInfo>> qryStartedListeners = new CopyOnWriteArrayList<>();

    /** */
    private final List<Consumer<GridQueryFinishedInfo>> qryFinishedListeners = new CopyOnWriteArrayList<>();

    /**
     * Constructor.
     *
     * @param ctx Context.
     */
    public RunningQueryManager(GridKernalContext ctx) {
        log = ctx.log(RunningQueryManager.class);
        locNodeId = ctx.localNodeId();
        histSz = ctx.config().getSqlConfiguration().getSqlQueryHistorySize();
        closure = ctx.closure();

        qryHistTracker = new QueryHistoryTracker(histSz);

        MetricRegistry userMetrics = ctx.metric().registry(SQL_USER_QUERIES_REG_NAME);

        successQrsCnt = userMetrics.longAdderMetric("success",
            "Number of successfully executed user queries that have been started on this node.");

        failedQrsCnt = userMetrics.longMetric("failed", "Total number of failed by any reason (cancel, oom etc)" +
            " queries that have been started on this node.");

        canceledQrsCnt = userMetrics.longMetric("canceled", "Number of canceled queries that have been started " +
            "on this node. This metric number included in the general 'failed' metric.");

        oomQrsCnt = userMetrics.longMetric("failedByOOM", "Number of queries started on this node failed due to " +
            "out of memory protection. This metric number included in the general 'failed' metric.");

        ctx.systemView().registerView(SQL_QRY_VIEW, SQL_QRY_VIEW_DESC,
            SqlQueryView.class,
            runs.values(),
            SqlQueryView::new);

        ctx.systemView().registerView(SQL_QRY_HIST_VIEW, SQL_QRY_HIST_VIEW_DESC,
            SqlQueryHistoryView.class,
            qryHistTracker.queryHistory().values(),
            SqlQueryHistoryView::new);
    }

    /**
     * Register running query.
     *
     * @param qry Query text.
     * @param qryType Query type.
     * @param schemaName Schema name.
     * @param loc Local query flag.
     * @param cancel Query cancel. Should be passed in case query is cancelable, or {@code null} otherwise.
     * @return Id of registered query.
     */
    public Long register(String qry, GridCacheQueryType qryType, String schemaName, boolean loc,
        @Nullable GridQueryMemoryMetricProvider memTracker, @Nullable GridQueryCancel cancel,
        String qryInitiatorId) {
        long qryId = qryIdGen.incrementAndGet();

        if (qryInitiatorId == null)
            qryInitiatorId = SqlFieldsQuery.threadedQueryInitiatorId();

        GridRunningQueryInfo run = new GridRunningQueryInfo(
            qryId,
            locNodeId,
            qry,
            qryType,
            schemaName,
            System.currentTimeMillis(),
            cancel,
            loc,
            memTracker == null ? DUMMY_TRACKER : memTracker,
            qryInitiatorId
        );

        GridRunningQueryInfo preRun = runs.putIfAbsent(qryId, run);

        assert preRun == null : "Running query already registered [prev_qry=" + preRun + ", newQry=" + run + ']';

        if (log.isDebugEnabled()) {
            log.debug("User's query started [id=" + qryId + ", type=" + qryType + ", local=" + loc +
                ", qry=" + qry + ']');
        }

        if (!qryStartedListeners.isEmpty()) {
            GridQueryStartedInfo info = new GridQueryStartedInfo(
                run.id(),
                locNodeId,
                run.query(),
                run.queryType(),
                run.schemaName(),
                run.startTime(),
                run.local(),
                run.queryInitiatorId()
            );

            try {
                closure.runLocal(
                    () -> qryStartedListeners.forEach(lsnr -> {
                        try {
                            lsnr.accept(info);
                        }
                        catch (Exception ex) {
                            log.error("Listener fails during handling query started" +
                                " event [qryId=" + qryId + "]", ex);
                        }
                    }),
                    GridIoPolicy.PUBLIC_POOL
                );
            }
            catch (IgniteCheckedException ex) {
                throw new IgniteException(ex.getMessage(), ex);
            }
        }

        return qryId;
    }

    /**
     * Unregister running query.
     *
     * @param qryId id of the query, which is given by {@link #register register} method.
     * @param failReason exception that caused query execution fail, or {@code null} if query succeded.
     */
    public void unregister(Long qryId, @Nullable Throwable failReason) {
        if (qryId == null)
            return;

        boolean failed = failReason != null;

        GridRunningQueryInfo qry = runs.remove(qryId);

        // Attempt to unregister query twice.
        if (qry == null)
            return;

        if (qry.memoryMetricProvider() instanceof AutoCloseable)
            U.close((AutoCloseable)qry.memoryMetricProvider(), log);

        if (log.isDebugEnabled()) {
            log.debug("User's query " + (failReason == null ? "completed " : "failed ") +
                "[id=" + qryId + ", tracker=" + qry.memoryMetricProvider() +
                ", failReason=" + (failReason != null ? failReason.getMessage() : "null") + ']');
        }

        if (!qryFinishedListeners.isEmpty()) {
            GridQueryFinishedInfo info = new GridQueryFinishedInfo(
                qry.id(),
                locNodeId,
                qry.query(),
                qry.queryType(),
                qry.schemaName(),
                qry.startTime(),
                System.currentTimeMillis(),
                qry.local(),
                failed,
                qry.queryInitiatorId()
            );

            try {
                closure.runLocal(
                    () -> qryFinishedListeners.forEach(lsnr -> {
                        try {
                            lsnr.accept(info);
                        }
                        catch (Exception ex) {
                            log.error("Listener fails during handling query finished" +
                                    " event [qryId=" + qryId + "]", ex);
                        }
                    }),
                    GridIoPolicy.PUBLIC_POOL
                );
            }
            catch (IgniteCheckedException ex) {
                throw new IgniteException(ex.getMessage(), ex);
            }
        }

        //We need to collect query history and metrics only for SQL queries.
        if (isSqlQuery(qry)) {
            qry.runningFuture().onDone();

            qryHistTracker.collectHistory(qry, failed);

            if (!failed)
                successQrsCnt.increment();
            else {
                failedQrsCnt.increment();

                // We measure cancel metric as "number of times user's queries ended up with query cancelled exception",
                // not "how many user's KILL QUERY command succeeded". These may be not the same if cancel was issued
                // right when query failed due to some other reason.
                if (QueryUtils.wasCancelled(failReason))
                    canceledQrsCnt.increment();
                else if (QueryUtils.isLocalOrReduceOom(failReason))
                    oomQrsCnt.increment();
            }
        }
    }

    /**
     * Return SQL queries which executing right now.
     *
     * @return List of SQL running queries.
     */
    public List<GridRunningQueryInfo> runningSqlQueries() {
        List<GridRunningQueryInfo> res = new ArrayList<>();

        for (GridRunningQueryInfo run : runs.values()) {
            if (isSqlQuery(run))
                res.add(run);
        }

        return res;
    }

    /**
     * @param lsnr Listener.
     */
    public void registerQueryStartedListener(Consumer<GridQueryStartedInfo> lsnr) {
        A.notNull(lsnr, "lsnr");

        qryStartedListeners.add(lsnr);
    }

    /**
     * @param lsnr Listener.
     */
    @SuppressWarnings("SuspiciousMethodCalls")
    public boolean unregisterQueryStartedListener(Object lsnr) {
        A.notNull(lsnr, "lsnr");

        return qryStartedListeners.remove(lsnr);
    }

    /**
     * @param lsnr Listener.
     */
    public void registerQueryFinishedListener(Consumer<GridQueryFinishedInfo> lsnr) {
        A.notNull(lsnr, "lsnr");

        qryFinishedListeners.add(lsnr);
    }

    /**
     * @param lsnr Listener.
     */
    @SuppressWarnings("SuspiciousMethodCalls")
    public boolean unregisterQueryFinishedListener(Object lsnr) {
        A.notNull(lsnr, "lsnr");

        return qryFinishedListeners.remove(lsnr);
    }

    /**
     * Check belongs running query to an SQL type.
     *
     * @param runningQryInfo Running query info object.
     * @return {@code true} For SQL or SQL_FIELDS query type.
     */
    private boolean isSqlQuery(GridRunningQueryInfo runningQryInfo){
        return runningQryInfo.queryType() == SQL_FIELDS || runningQryInfo.queryType() == SQL;
    }

    /**
     * Return long running user queries.
     *
     * @param duration Duration of long query.
     * @return Collection of queries which running longer than given duration.
     */
    public Collection<GridRunningQueryInfo> longRunningQueries(long duration) {
        Collection<GridRunningQueryInfo> res = new ArrayList<>();

        long curTime = System.currentTimeMillis();

        for (GridRunningQueryInfo runningQryInfo : runs.values()) {
            if (runningQryInfo.longQuery(curTime, duration))
                res.add(runningQryInfo);
        }

        return res;
    }

    /**
     * Cancel query.
     *
     * @param qryId Query id.
     */
    public void cancel(Long qryId) {
        GridRunningQueryInfo run = runs.get(qryId);

        if (run != null)
            run.cancel();
    }

    /**
     * Cancel all executing queries and deregistering all of them.
     */
    public void stop() {
        Iterator<GridRunningQueryInfo> iter = runs.values().iterator();

        while (iter.hasNext()) {
            try {
                GridRunningQueryInfo r = iter.next();

                iter.remove();

                r.cancel();
            }
            catch (Exception ignore) {
                // No-op.
            }
        }
    }

    /**
     * Gets query history statistics. Size of history could be configured via {@link
     * SqlConfiguration#setSqlQueryHistorySize(int)}
     *
     * @return Queries history statistics aggregated by query text, schema and local flag.
     */
    public Map<QueryHistoryKey, QueryHistory> queryHistoryMetrics() {
        return qryHistTracker.queryHistory();
    }

    /**
     * Gets info about running query by their id.
     * @param qryId
     * @return Running query info or {@code null} in case no running query for given id.
     */
    public @Nullable GridRunningQueryInfo runningQueryInfo(Long qryId) {
        return runs.get(qryId);
    }

    /**
     * Reset query history.
     */
    public void resetQueryHistoryMetrics() {
        qryHistTracker = new QueryHistoryTracker(histSz);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(RunningQueryManager.class, this);
    }
}
