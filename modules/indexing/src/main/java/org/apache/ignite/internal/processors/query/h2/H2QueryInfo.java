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
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.RunningQueryManager;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlQueryParser;
import org.apache.ignite.internal.util.typedef.internal.LT;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.h2.command.Prepared;
import org.h2.engine.Session;

/**
 * Base H2 query info with commons for MAP, LOCAL, REDUCE queries.
 */
public class H2QueryInfo {
    /** Query id assigned by {@link RunningQueryManager}. */
    private final Long runningQryId;

    /** Type. */
    private final QueryType type;

    /** Begin timestamp. */
    private final long beginTs;

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

    /**
     * @param type Query type.
     * @param stmt Query statement.
     * @param sql Query statement.
     * @param runningQryId Query id assigned by {@link RunningQueryManager}.
     */
    public H2QueryInfo(QueryType type, PreparedStatement stmt, String sql, Long runningQryId) {
        try {
            assert stmt != null;

            this.type = type;
            this.sql = sql;
            this.runningQryId = runningQryId;

            beginTs = U.currentTimeMillis();

            schema = stmt.getConnection().getSchema();

            Session s = H2Utils.session(stmt.getConnection());

            enforceJoinOrder = s.isForceJoinOrder();
            distributedJoin = s.isJoinBatchEnabled();
            lazy = s.isLazyQueryExecution();
            this.stmt = GridSqlQueryParser.prepared(stmt);
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
        return U.currentTimeMillis() - beginTs;
    }

    /**
     * @param log Logger.
     * @param msg Log message
     * @param additionalInfo Additional query info.
     */
    public void printLogMessage(IgniteLogger log, String msg, String additionalInfo) {
        printLogMessage(log, null, msg, additionalInfo);
    }

    /** @return Query id assigned by {@link RunningQueryManager}. */
    public Long runningQueryId() {
        return runningQryId;
    }

    /**
     * @param log Logger.
     * @param msg Log message
     * @param connMgr Connection manager.
     * @param additionalInfo Additional query info.
     */
    public void printLogMessage(IgniteLogger log, ConnectionManager connMgr, String msg, String additionalInfo) {
        StringBuilder msgSb = new StringBuilder(msg + " [");

        if (additionalInfo != null)
            msgSb.append(additionalInfo).append(", ");

        msgSb.append("duration=").append(time()).append("ms")
            .append(", type=").append(type)
            .append(", distributedJoin=").append(distributedJoin)
            .append(", enforceJoinOrder=").append(enforceJoinOrder)
            .append(", lazy=").append(lazy)
            .append(", schema=").append(schema);

        msgSb.append(", sql='")
            .append(sql);

        msgSb.append("', plan=").append(stmt.getPlanSQL(false));

        printInfo(msgSb);

        msgSb.append(']');

        LT.warn(log, msgSb.toString());
    }

    /**
     * Returns description of this query info.
     */
    public String description() {
        return "H2QueryInfo ["
            + "type=" + type
            + ", runningQryId=" + runningQryId
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
