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

import java.util.UUID;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryType;
import org.apache.ignite.internal.processors.tracing.MTC;
import org.apache.ignite.internal.processors.tracing.Span;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Query descriptor.
 */
public class GridRunningQueryInfo {
    /** */
    private final long id;

    /** Originating Node ID. */
    private final UUID nodeId;

    /** */
    private final String qry;

    /** Query type. */
    private final GridCacheQueryType qryType;

    /** Schema name. */
    private final String schemaName;

    /** */
    private final long startTime;

    /** */
    @GridToStringExclude
    private final GridQueryCancel cancel;

    /** */
    private final boolean loc;

    /** */
    private final GridQueryMemoryMetricProvider memMetricProvider;

    /** */
    @GridToStringExclude
    private final QueryRunningFuture fut = new QueryRunningFuture();

    /** Originator. */
    private final String qryInitiatorId;

    /** Span of the running query. */
    private final Span span;

    /** Enforce join order flag. */
    private final boolean enforceJoinOrder;

    /** Lazy flag. */
    private final boolean lazy;

    /** Distributed joins flag. */
    private final boolean distributedJoins;

    /** Query label. */
    private final String label;

    /**
     * Constructor.
     *
     * @param id Query ID.
     * @param nodeId Originating node ID.
     * @param qry Query text.
     * @param qryType Query type.
     * @param schemaName Schema name.
     * @param startTime Query start time.
     * @param cancel Query cancel.
     * @param loc Local query flag.
     * @param qryInitiatorId Query's initiator identifier.
     * @param enforceJoinOrder Enforce join order flag.
     * @param lazy Lazy flag.
     * @param distributedJoins Distributed joins flag.
     */
    public GridRunningQueryInfo(
        long id,
        UUID nodeId,
        String qry,
        GridCacheQueryType qryType,
        String schemaName,
        long startTime,
        GridQueryCancel cancel,
        boolean loc,
        GridQueryMemoryMetricProvider memMetricProvider,
        String qryInitiatorId,
        boolean enforceJoinOrder,
        boolean lazy,
        boolean distributedJoins,
        String label
    ) {
        this.id = id;
        this.nodeId = nodeId;
        this.qry = qry;
        this.qryType = qryType;
        this.schemaName = schemaName;
        this.startTime = startTime;
        this.cancel = cancel;
        this.loc = loc;
        this.memMetricProvider = memMetricProvider;
        this.qryInitiatorId = qryInitiatorId;
        this.enforceJoinOrder = enforceJoinOrder;
        this.lazy = lazy;
        this.distributedJoins = distributedJoins;
        this.span = MTC.span();
        this.label = label;
    }

    /**
     * @return Query ID.
     */
    public long id() {
        return id;
    }

    /**
     * @return Global query ID.
     */
    public String globalQueryId() {
        return QueryUtils.globalQueryId(nodeId, id);
    }

    /**
     * @return Query text.
     */
    public String query() {
        return qry;
    }

    /**
     * @return Query type.
     */
    public GridCacheQueryType queryType() {
        return qryType;
    }

    /**
     * @return Schema name.
     */
    public String schemaName() {
        return schemaName;
    }

    /**
     * @return Query start time.
     */
    public long startTime() {
        return startTime;
    }

    /** */
    public GridQueryMemoryMetricProvider memoryMetricProvider() {
        return memMetricProvider;
    }

    /**
     * @param curTime Current time.
     * @param duration Duration of long query.
     * @return {@code true} if this query should be considered as long running query.
     */
    public boolean longQuery(long curTime, long duration) {
        return curTime - startTime > duration;
    }

    /**
     * Cancel query.
     */
    public void cancel() {
        if (cancel != null)
            cancel.cancel();
    }

    /**
     * @return Query running future.
     */
    public QueryRunningFuture runningFuture() {
        return fut;
    }

    /**
     * @return {@code true} if query can be cancelled.
     */
    public boolean cancelable() {
        return cancel != null;
    }

    /**
     * @return {@code true} if query is local.
     */
    public boolean local() {
        return loc;
    }

    /**
     * @return Originating node ID.
     */
    public UUID nodeId() {
        return nodeId;
    }

    /**
     * @return Query's originator string (client host+port, user name,
     * job name or any user's information about query initiator).
     */
    public String queryInitiatorId() {
        return qryInitiatorId;
    }

    /**
     * @return Distributed joins.
     */
    public boolean distributedJoins() {
        return distributedJoins;
    }

    /**{@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridRunningQueryInfo.class, this);
    }

    /**
     * @return Span of the running query.
     */
    public Span span() {
        return span;
    }

    /**
     * @return Enforce join order flag.
     */
    public boolean enforceJoinOrder() {
        return enforceJoinOrder;
    }

    /**
     * @return Lazy flag.
     */
    public boolean lazy() {
        return lazy;
    }

    /**
     * @return Query label.
     */
    public String label() {
        return label;
    }

}
