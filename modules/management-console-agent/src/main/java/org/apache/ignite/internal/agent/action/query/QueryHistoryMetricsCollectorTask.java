/*
 * Copyright 2020 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.internal.agent.action.query;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTaskAdapter;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.agent.dto.action.query.QueryDetailMetrics;
import org.apache.ignite.internal.processors.cache.GridCacheProcessor;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryDetailMetricsAdapter;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryDetailMetricsKey;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryType;
import org.apache.ignite.internal.processors.query.GridQueryIndexing;
import org.apache.ignite.internal.processors.query.QueryHistoryMetrics;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.resources.IgniteInstanceResource;

import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;
import static org.apache.ignite.internal.processors.cache.GridCacheUtils.isSystemCache;
import static org.apache.ignite.internal.processors.cache.query.GridCacheQueryType.SCAN;
import static org.apache.ignite.internal.processors.cache.query.GridCacheQueryType.SQL_FIELDS;

/**
 * Query history metrics collector task.
 */
@GridInternal
public class QueryHistoryMetricsCollectorTask extends ComputeTaskAdapter<Long, Collection<QueryDetailMetrics>> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid, Long arg) throws IgniteException {
       return subgrid.stream().collect(toMap(n -> new QueryHistoryMetricsCollectorJob(arg), identity()));
    }

    /** {@inheritDoc} */
    @Override public Collection<QueryDetailMetrics> reduce(List<ComputeJobResult> results) throws IgniteException {
        Map<GridCacheQueryDetailMetricsKey, QueryDetailMetrics> taskRes = new HashMap<>();

        for (ComputeJobResult res : results) {
            if (res.getException() != null)
                throw res.getException();

            res.<Map<GridCacheQueryDetailMetricsKey, QueryDetailMetrics>>getData()
                .forEach((key, value) -> taskRes.merge(key, value, QueryDetailMetrics::merge));
        }

        return taskRes.values();
    }

    /**
     * Query history metrics collector job.
     */
    private static class QueryHistoryMetricsCollectorJob extends ComputeJobAdapter {
        /** */
        private static final long serialVersionUID = 0L;

        /** Ignite. */
        @IgniteInstanceResource
        private IgniteEx ignite;

        /**
         * @param arg Argument.
         */
        public QueryHistoryMetricsCollectorJob(Long arg) {
            setArguments(arg);
        }

        /** {@inheritDoc} */
        @Override public Map<GridCacheQueryDetailMetricsKey, QueryDetailMetrics> execute() {
            long since = argument(0);

            GridQueryIndexing indexing = ignite.context().query().getIndexing();

            GridCacheProcessor cacheProc = ignite.context().cache();

            Stream<QueryDetailMetrics> cacheMetricsStream = cacheProc.cacheNames().stream()
                .filter(name -> !isSystemCache(name))
                .map(cacheProc::cache)
                .filter(cache -> cache != null && cache.context().started())
                .flatMap(cache -> cache.context().queries().detailMetrics().stream())
                .filter(m -> m.lastStartTime() > since && m.key().getQueryType() == SCAN)
                .map(this::toQueryDetailMetrics);

            if (indexing instanceof IgniteH2Indexing) {
                Collection<QueryHistoryMetrics> metrics = ((IgniteH2Indexing)indexing)
                    .runningQueryManager().queryHistoryMetrics().values();

                cacheMetricsStream = Stream.concat(
                    cacheMetricsStream,
                    metrics.stream().map(this::toQueryDetailMetrics)
                );
            }

            return cacheMetricsStream
                .collect(
                    toMap(
                        this::toMetricKey,
                        identity(),
                        QueryDetailMetrics::merge
                    )
                );
        }

        /**
         * @param m Query history metric.
         * @return Query detail metrics key.
         */
        private GridCacheQueryDetailMetricsKey toMetricKey(QueryDetailMetrics m) {
            return new GridCacheQueryDetailMetricsKey(GridCacheQueryType.valueOf(m.getQueryType()), m.getQuery());
        }

        /**
         * @param m Query history metric.
         * @return Query detail metrics adapter.
         */
        private QueryDetailMetrics toQueryDetailMetrics(QueryHistoryMetrics m) {
            return new QueryDetailMetrics()
                .setQuery(m.query())
                .setQueryType(SQL_FIELDS.name())
                .setExecutions(m.executions())
                .setFailures(m.failures())
                .setLastStartTime(m.lastStartTime())
                .setCompletions(m.executions() - m.failures())
                .setMaxTime(m.maximumTime())
                .setMinTime(m.minimumTime());
        }

        /**
         * @param m Metrics adapter.
         * @return Query detail metrics.
         */
        private QueryDetailMetrics toQueryDetailMetrics(GridCacheQueryDetailMetricsAdapter m) {
            return new QueryDetailMetrics()
                .setQuery(m.query())
                .setQueryType(m.queryType())
                .setExecutions(m.executions())
                .setFailures(m.failures())
                .setLastStartTime(m.lastStartTime())
                .setCompletions(m.completions())
                .setCache(m.cache())
                .setMaxTime(m.maximumTime())
                .setMinTime(m.minimumTime())
                .setTotalTime(m.totalTime());
        }
    }
}
