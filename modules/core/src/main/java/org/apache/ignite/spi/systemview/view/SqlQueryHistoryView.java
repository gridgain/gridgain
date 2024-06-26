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
import org.apache.ignite.internal.managers.systemview.walker.Order;
import org.apache.ignite.internal.processors.query.QueryHistory;

/**
 * SQL query history representation for a {@link SystemView}.
 */
public class SqlQueryHistoryView {
    /** Query history item. */
    private final QueryHistory qry;

    /**
     * @param qry Query history item.
     */
    public SqlQueryHistoryView(QueryHistory qry) {
        this.qry = qry;
    }

    /** @return Schema name. */
    @Order
    public String schemaName() {
        return qry.schema();
    }

    /** @return Query text. */
    @Order(1)
    public String sql() {
        return qry.query();
    }

    /** @return {@code True} if query local. */
    @Order(2)
    public boolean local() {
        return qry.local();
    }

    /** @return Number of executions of the query. */
    @Order(3)
    public long executions() {
        return qry.executions();
    }

    /** @return Number of failed execution of the query. */
    @Order(4)
    public long failures() {
        return qry.failures();
    }

    /** @return Minimal query duration. */
    @Order(5)
    public long durationMin() {
        return qry.minimumTime();
    }

    /** @return Maximum query duration. */
    @Order(6)
    public long durationMax() {
        return qry.maximumTime();
    }

    /** @return Last start time. */
    @Order(7)
    public Date lastStartTime() {
        return new Date(qry.lastStartTime());
    }

    /**
     * Returns maximum size of reserved memory.
     *
     * @return Maximum size of reserved memory.
     */
    @Order(12)
    public long memoryMax() {
        return qry.maxMemory();
    }

    /**
     * Returns minimum size of reserved memory.
     *
     * @return Minimum size of reserved memory.
     */
    @Order(13)
    public long memoryMin() {
        return qry.minMemory();
    }

    /**
     * @return Distributed joins.
     */
    @Order(14)
    public boolean distributedJoins() {
        return qry.distributedJoins();
    }

    /**
     * @return Enforce join order.
     */
    @Order(15)
    public boolean enforceJoinOrder() {
        return qry.enforceJoinOrder();
    }

    /**
     * @return Lazy flag.
     */
    @Order(16)
    public boolean lazy() {
        return qry.lazy();
    }

    /**
     * Returns minimum allocated size of data on disk.
     *
     * @return Minimum allocated size of data on disk.
     */
    @Order(9)
    public long diskAllocationMin() {
        return qry.minBytesAllocatedOnDisk();
    }

    /**
     * Returns maximum allocated size of data on disk.
     *
     * @return Maximum allocated size of data on disk.
     */
    @Order(8)
    public long diskAllocationMax() {
        return qry.maxBytesAllocatedOnDisk();
    }

    /**
     * Returns minimum of total allocated size of data on disk.
     *
     * @return Minimum of total allocated size of data on disk.
     */
    @Order(11)
    public long diskAllocationTotalMin() {
        return qry.minTotalBytesWrittenOnDisk();
    }

    /**
     * Returns maximum of total allocated size of data on disk.
     *
     * @return Maximum of total allocated size of data on disk.
     */
    @Order(10)
    public long diskAllocationTotalMax() {
        return qry.maxTotalBytesWrittenOnDisk();
    }
}
