/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 * 
 * Commons Clause Restriction
 * 
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 * 
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 * 
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.processors.query;

import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;
import org.jsr166.ConcurrentLinkedDeque8;

/**
 * Query history metrics.
 */
public class QueryHistoryMetrics {
    /** Link to internal node in eviction deque. */
    @GridToStringExclude
    private final AtomicReference<ConcurrentLinkedDeque8.Node<QueryHistoryMetrics>> linkRef;

    /** Query history metrics immutable wrapper. */
    private volatile QueryHistoryMetricsValue val;

    /** Query history metrics group key. */
    private final QueryHistoryMetricsKey key;

    /**
     * Constructor with metrics.
     *
     * @param qry Textual query representation.
     * @param schema Schema name.
     * @param loc {@code true} for local query.
     * @param startTime Start time of query execution.
     * @param duration Duration of query execution.
     * @param failed {@code True} query executed unsuccessfully {@code false} otherwise.
     */
    public QueryHistoryMetrics(String qry, String schema, boolean loc, long startTime, long duration, boolean failed) {
        key = new QueryHistoryMetricsKey(qry, schema, loc);

        long failures = failed ? 1 : 0;

        val = new QueryHistoryMetricsValue(1, failures, duration, duration, startTime);

        linkRef = new AtomicReference<>();
    }

    /**
     * @return Metrics group key.
     */
    public QueryHistoryMetricsKey key() {
        return key;
    }

    /**
     * Aggregate new metrics with already existen.
     *
     * @param m Other metrics to take into account.
     * @return Aggregated metrics.
     */
    public QueryHistoryMetrics aggregateWithNew(QueryHistoryMetrics m) {
        val = new QueryHistoryMetricsValue(
            val.execs() + m.executions(),
            val.failures() + m.failures(),
            Math.min(val.minTime(), m.minimumTime()),
            Math.max(val.maxTime(), m.maximumTime()),
            Math.max(val.lastStartTime(), m.lastStartTime()));

        return this;
    }

    /**
     * @return Textual representation of query.
     */
    public String query() {
        return key.query();
    }

    /**
     * @return Schema.
     */
    public String schema() {
        return key.schema();
    }

    /**
     * @return {@code true} For query with enabled local flag.
     */
    public boolean local() {
        return key.local();
    }

    /**
     * Gets total number execution of query.
     *
     * @return Number of executions.
     */
    public long executions() {
        return val.execs();
    }

    /**
     * Gets number of times a query execution failed.
     *
     * @return Number of times a query execution failed.
     */
    public long failures() {
        return val.failures();
    }

    /**
     * Gets minimum execution time of query.
     *
     * @return Minimum execution time of query.
     */
    public long minimumTime() {
        return val.minTime();
    }

    /**
     * Gets maximum execution time of query.
     *
     * @return Maximum execution time of query.
     */
    public long maximumTime() {
        return val.maxTime();
    }

    /**
     * Gets latest query start time.
     *
     * @return Latest time query was stared.
     */
    public long lastStartTime() {
        return val.lastStartTime();
    }

    /**
     * @return Link to internal node in eviction deque.
     */
    @Nullable public ConcurrentLinkedDeque8.Node<QueryHistoryMetrics> link() {
        return linkRef.get();
    }

    /**
     * Atomically replace link to new.
     *
     * @param expLink Link which should be replaced.
     * @param updatedLink New link which should be set.
     * @return {@code true} If link has been updated.
     */
    public boolean replaceLink(ConcurrentLinkedDeque8.Node<QueryHistoryMetrics> expLink,
        ConcurrentLinkedDeque8.Node<QueryHistoryMetrics> updatedLink) {
        return linkRef.compareAndSet(expLink, updatedLink);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(QueryHistoryMetrics.class, this);
    }
}
