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

package org.apache.ignite.spi.systemview.view;

import java.util.Date;
import java.util.UUID;
import org.apache.ignite.internal.managers.systemview.walker.Order;
import org.apache.ignite.internal.processors.query.GridRunningQueryInfo;

/**
 * SQL query representation for a {@link SystemView}.
 */
public class SqlQueryView {
    /** Query. */
    private final GridRunningQueryInfo qry;

    /**
     * @param qry Query.
     */
    public SqlQueryView(GridRunningQueryInfo qry) {
        this.qry = qry;
    }

    /** @return Origin query node. */
    @Order(2)
    public UUID originNodeId() {
        return qry.nodeId();
    }

    /** @return Query ID. */
    @Order
    public String queryId() {
        return qry.globalQueryId();
    }

    /** @return Query text. */
    @Order(1)
    public String sql() {
        return qry.query();
    }

    /** @return Schema name. */
    public String schemaName() {
        return qry.schemaName();
    }

    /** @return Query start time. */
    @Order(3)
    public Date startTime() {
        return new Date(qry.startTime());
    }

    /** @return Query duration. */
    @Order(4)
    public long duration() {
        return System.currentTimeMillis() - qry.startTime();
    }

    /** @return {@code True} if query is local. */
    public boolean local() {
        return qry.local();
    }

    /**
     * Returns current allocated size of data on disk.
     *
     * @return Current allocated size of data on disk.
     */
    public long diskAllocationCurrent() {
        return qry.memoryMetricProvider().writtenOnDisk();
    }

    /**
     * Returns maximum allocated size of data on disk.
     *
     * @return Maximum allocated size of data on disk.
     */
    public long diskAllocationMax() {
        return qry.memoryMetricProvider().maxWrittenOnDisk();
    }

    /**
     * Returns total allocated size of data on disk.
     *
     * @return Total allocated size of data on disk.
     */
    public long diskAllocationTotal() {
        return qry.memoryMetricProvider().totalWrittenOnDisk();
    }

    /**
     * Returns current size of reserved memory.
     *
     * @return Current size of reserved memory.
     */
    public long memoryCurrent() {
        return qry.memoryMetricProvider().reserved();
    }

    /**
     * Returns maximum size of reserved memory.
     *
     * @return Maximum size of reserved memory.
     */
    public long memoryMax() {
        return qry.memoryMetricProvider().maxReserved();
    }

    /**
     * Returns query initiator ID.
     *
     * @return Query initiator ID.
     */
    public String initiatorId() {
        return qry.queryInitiatorId();
    }
}
