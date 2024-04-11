/*
 * Copyright 2024 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.internal;

import java.util.Collections;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.mxbean.QueryMXBean;
import org.apache.ignite.resources.IgniteInstanceResource;

import static org.apache.ignite.internal.sql.command.SqlKillQueryCommand.parseGlobalQueryId;

/**
 * QueryMXBean implementation.
 */
public class QueryMXBeanImpl implements QueryMXBean {
    /** Global query id format. */
    public static final String EXPECTED_GLOBAL_QRY_ID_FORMAT = "Global query id should have format " +
        "'{node_id}_{query_id}', e.g. '6fa749ee-7cf8-4635-be10-36a1c75267a7_54321'";

    /** Kernal context. */
    private final GridKernalContext ctx;

    /** Logger. */
    private final IgniteLogger log;

    /**
     * @param ctx Context.
     */
    public QueryMXBeanImpl(GridKernalContext ctx) {
        this.ctx = ctx;
        this.log = ctx.log(QueryMXBeanImpl.class);
    }

    /** {@inheritDoc} */
    @Override public void cancelContinuous(String originNodeId, String routineId) {
        A.notNullOrEmpty(originNodeId, "originNodeId");
        A.notNullOrEmpty(routineId, "routineId");

        if (log.isInfoEnabled())
            log.info("Killing continuous query[routineId=" + routineId + ", originNodeId=" + originNodeId + ']');

        cancelContinuous(UUID.fromString(originNodeId), UUID.fromString(routineId));
    }

    /** {@inheritDoc} */
    @Override public void cancelSQL(String id) {
        A.notNull(id, "id");

        if (log.isInfoEnabled())
            log.info("Killing sql query[id=" + id + ']');

        try {
            T2<UUID, Long> ids = parseGlobalQueryId(id);

            if (ids == null)
                throw new IllegalArgumentException("Expected global query id. " + EXPECTED_GLOBAL_QRY_ID_FORMAT);

            cancelSQL(ids.get1(), ids.get2());
        }
        catch (IgniteException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Executes scan query cancel on all cluster nodes.
     *
     * @param originNodeId Originating node id.
     */
    public void cancelSQL(UUID originNodeId, long qryId) {
        ctx.grid().compute(ctx.grid().cluster().forNodeId(originNodeId))
            .broadcast(new CancelSQLOnInitiator(), qryId);
    }

    /**
     * Kills continuous query by the identifier.
     *
     * @param originNodeId Originating node id.
     * @param routineId Routine id.
     */
    public void cancelContinuous(UUID originNodeId, UUID routineId) {
        ctx.grid().compute(ctx.grid().cluster().forNodeId(originNodeId))
            .broadcast(new CancelContinuousOnInitiator(), routineId);
    }

    /**
     * Cancel SQL on initiator closure.
     */
    private static class CancelSQLOnInitiator implements IgniteClosure<Long, Void> {
        /**  */
        private static final long serialVersionUID = 0L;

        /** Auto-injected grid instance. */
        @IgniteInstanceResource
        private transient IgniteEx ignite;

        /** {@inheritDoc} */
        @Override public Void apply(Long qryId) {
            ignite.context().query().cancelQueries(Collections.singleton(qryId));

            return null;
        }
    }

    /**
     * Cancel continuous on initiator closure.
     */
    private static class CancelContinuousOnInitiator implements IgniteClosure<UUID, Void> {
        /**  */
        private static final long serialVersionUID = 0L;

        /** Auto-injected grid instance. */
        @IgniteInstanceResource
        private transient IgniteEx ignite;

        /** {@inheritDoc} */
        @Override public Void apply(UUID routineId) {
            IgniteInternalFuture<?> fut = ignite.context().continuous().stopRoutine(routineId);

            try {
                fut.get();
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException(e);
            }

            return null;
        }
    }
}
