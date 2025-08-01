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

package org.apache.ignite.internal.processors.query.h2;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.UUID;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.processors.query.RunningQueryManager;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlQueryParser;
import org.apache.ignite.internal.util.typedef.internal.LT;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.gridgain.internal.h2.command.Prepared;
import org.gridgain.internal.h2.engine.Session;
import org.jetbrains.annotations.Nullable;

/**
 * Base H2 query info with commons for MAP, LOCAL, REDUCE queries.
 */
public class H2QueryInfo {
    /** Query id assigned by {@link RunningQueryManager}. */
    private final @Nullable Long runningQryId;

    /** Query label. */
    private final @Nullable String label;

    /** Type. */
    private final QueryType type;

    /** Begin timestamp. */
    private final long beginTs;

    /** The most recent point in time when the tracking of a long query was suspended. */
    private volatile long lastSuspendTs;

    /** External wait time. */
    private volatile long extWait;

    /** Long query time tracking suspension flag. */
    private volatile boolean isSuspended;

    /** Query schema. */
    private final String schema;

    /** Query SQL. */
    private final String sql;

    /** Enforce join order. */
    private final boolean enforceJoinOrder;

    /** Join batch enabled (distributed join). */
    private final boolean distributedJoin;

    /** Lazy mode. */
    private final boolean lazy;

    /** Prepared statement. */
    private final Prepared stmt;

    /** Originator node uid. */
    private final UUID node;

    /**
     * @param type Query type.
     * @param stmt Query statement.
     * @param sql Query statement.
     * @param node Originator node.
     * @param runningQryId Query id assigned by {@link RunningQueryManager}.
     * @param label Query label.
     */
    public H2QueryInfo(QueryType type, PreparedStatement stmt, String sql, ClusterNode node,
        @Nullable Long runningQryId, @Nullable String label) {
        try {
            assert stmt != null;
            this.type = type;
            this.sql = sql;
            this.node = node.id();
            this.runningQryId = runningQryId;

            beginTs = U.currentTimeMillis();

            schema = stmt.getConnection().getSchema();

            Session s = H2Utils.session(stmt.getConnection());

            enforceJoinOrder = s.isForceJoinOrder();
            distributedJoin = s.isJoinBatchEnabled();
            lazy = s.isLazyQueryExecution();
            this.stmt = GridSqlQueryParser.prepared(stmt);
            this.label = label;
        }
        catch (SQLException e) {
            throw new IgniteSQLException("Cannot collect query info", IgniteQueryErrorCode.UNKNOWN, e);
        }
    }

    /**
     * Print info specified by children.
     *
     * @param msg Message string builder.
     */
    protected void printInfo(StringBuilder msg) {
        // No-op.
    }

    /**
     * @return Query execution time.
     */
    public long time() {
        return (isSuspended ? lastSuspendTs : U.currentTimeMillis()) - beginTs - extWait;
    }

    /** */
    public synchronized void suspendTracking() {
        if (!isSuspended) {
            isSuspended = true;

            lastSuspendTs = U.currentTimeMillis();
        }
    }

    /** */
    public synchronized void resumeTracking() {
        if (isSuspended) {
            isSuspended = false;

            extWait += U.currentTimeMillis() - lastSuspendTs;
        }
    }

    /** @return Query id assigned by {@link RunningQueryManager}. */
    public Long runningQueryId() {
        return runningQryId;
    }

    /**
     * @return Node originator uid.
     */
    public UUID node() {
        return node;
    }

    /** */
    public long extWait() {
        return extWait;
    }

    /**
     * @return Query type.
     */
    public QueryType type() {
        return type;
    }

    /**
     * @param log Logger.
     * @param msg Log message
     * @param additionalInfo Additional query info.
     */
    public void printLogMessage(IgniteLogger log, String msg, String additionalInfo) {
        String globalQueryId = runningQryId == null ? "(unknown)" // compatibility with old versions.
            : QueryUtils.globalQueryId(node, runningQryId);

        StringBuilder msgSb = new StringBuilder(msg)
            .append(" [globalQueryId=").append(globalQueryId);

        if (label != null)
            msgSb.append(", label=").append(label);

        if (additionalInfo != null)
            msgSb.append(", ").append(additionalInfo);

        msgSb
            .append(", duration=").append(time()).append("ms")
            .append(", type=").append(type)
            .append(", distributedJoin=").append(distributedJoin)
            .append(", enforceJoinOrder=").append(enforceJoinOrder)
            .append(", lazy=").append(lazy)
            .append(", schema=").append(schema)
            .append(", sql='").append(sql)
            .append("', plan=").append(stmt.getPlanSQL(false));

        printInfo(msgSb);

        msgSb.append(']');

        // Include 'sql' text into key for compatibility with older versions.
        String throttleKey = globalQueryId + "#" + (runningQryId == null ? sql : type);
        LT.warn(log, throttleKey, msgSb.toString());
    }

    /** */
    public boolean isSuspended() {
        return isSuspended;
    }

    /**
     * Returns description of this query info.
     */
    public String description() {
        return "H2QueryInfo ["
            + "type=" + type
            + ", globalQueryId=" + QueryUtils.globalQueryId(node, runningQryId)
            + ", beginTs=" + beginTs
            + ", distributedJoin=" + distributedJoin
            + ", enforceJoinOrder=" + enforceJoinOrder
            + ", lazy=" + lazy
            + ", schema=" + schema
            + ", sql='" + sql + "']";
    }

    /**
     * Query type.
     */
    public enum QueryType {
        LOCAL,
        MAP,
        REDUCE
    }
}
