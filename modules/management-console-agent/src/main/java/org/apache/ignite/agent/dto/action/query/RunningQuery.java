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

package org.apache.ignite.agent.dto.action.query;

import org.apache.ignite.internal.processors.cache.query.GridCacheQueryType;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Descriptor of running query.
 */
public class RunningQuery {
    /** */
    private long id;

    /** Query text. */
    private String qry;

    /** Query type. */
    private GridCacheQueryType qryType;

    /** Schema name. */
    private String schemaName;

    /** */
    private long startTime;

    /** */
    private long duration;

    /** */
    private boolean cancellable;

    /** */
    private boolean loc;

    /**
     * @return Query ID.
     */
    public long getId() {
        return id;
    }

    /**
     * @param id ID.
     * @return {@code This} for chaining method calls.
     */
    public RunningQuery setId(long id) {
        this.id = id;

        return this;
    }

    /**
     * @return Query txt.
     */
    public String getQuery() {
        return qry;
    }


    /**
     * @param qry Query.
     * @return {@code This} for chaining method calls.
     */
    public RunningQuery setQuery(String qry) {
        this.qry = qry;

        return this;
    }

    /**
     * @return Query type.
     */
    public GridCacheQueryType getQueryType() {
        return qryType;
    }

    /**
     * @param qryType Query type.
     * @return {@code This} for chaining method calls.
     */
    public RunningQuery setQryType(GridCacheQueryType qryType) {
        this.qryType = qryType;

        return this;
    }

    /**
     * @return Schema name.
     */
    public String getSchemaName() {
        return schemaName;
    }

    /**
     * @param schemaName Schema name.
     * @return {@code This} for chaining method calls.
     */
    public RunningQuery setSchemaName(String schemaName) {
        this.schemaName = schemaName;

        return this;
    }

    /**
     * @return Query start time.
     */
    public long getStartTime() {
        return startTime;
    }

    /**
     * @param startTime Start time.
     * @return {@code This} for chaining method calls.
     */
    public RunningQuery setStartTime(long startTime) {
        this.startTime = startTime;

        return this;
    }

    /**
     * @return Query duration.
     */
    public long getDuration() {
        return duration;
    }

    /**
     * @param duration Duration.
     * @return {@code This} for chaining method calls.
     */
    public RunningQuery setDuration(long duration) {
        this.duration = duration;

        return this;
    }


    /**
     * @return {@code true} if query can be cancelled.
     */
    public boolean isCancellable() {
        return cancellable;
    }

    /**
     * @param cancellable Cancellable.
     * @return {@code This} for chaining method calls.
     */
    public RunningQuery setCancellable(boolean cancellable) {
        this.cancellable = cancellable;

        return this;
    }

    /**
     * @return {@code true} if query is local.
     */
    public boolean isLocal() {
        return loc;
    }

    /**
     * @param loc Local.
     * @return {@code This} for chaining method calls.
     */
    public RunningQuery setLocal(boolean loc) {
        this.loc = loc;

        return this;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(RunningQuery.class, this);
    }
}
