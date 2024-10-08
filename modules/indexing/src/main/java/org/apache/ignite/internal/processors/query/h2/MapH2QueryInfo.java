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
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.processors.query.RunningQueryManager;
import org.jetbrains.annotations.Nullable;

/**
 * Map query info.
 */
public class MapH2QueryInfo extends H2QueryInfo {
    /** Request id. */
    private final long reqId;

    /** Segment. */
    private final int segment;

    /**
     * @param stmt Query statement.
     * @param sql Query statement.
     * @param node Originator node.
     * @param reqId Request ID.
     * @param segment Segment.
     * @param runningQryId Query id assigned by {@link RunningQueryManager}.
     * @param label Query label.
     */
    public MapH2QueryInfo(PreparedStatement stmt, String sql,
        ClusterNode node, long reqId, int segment, @Nullable Long runningQryId, @Nullable String label) {
        super(QueryType.MAP, stmt, sql, node, runningQryId, label);

        this.reqId = reqId;
        this.segment = segment;
    }

    /** {@inheritDoc} */
    @Override protected void printInfo(StringBuilder msg) {
        msg.append(", reqId=").append(reqId)
            .append(", segment=").append(segment);
    }
}
