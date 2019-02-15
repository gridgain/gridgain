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

package org.apache.ignite.internal.visor.query;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.processors.cache.IgniteCacheProxy;
import org.apache.ignite.internal.processors.query.GridQueryFieldMetadata;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.processors.task.GridVisorManagementTask;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorEither;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorOneNodeTask;
import org.apache.ignite.internal.visor.util.VisorExceptionWrapper;

import static org.apache.ignite.internal.visor.query.VisorQueryUtils.SQL_QRY_NAME;
import static org.apache.ignite.internal.visor.query.VisorQueryUtils.fetchSqlQueryRows;
import static org.apache.ignite.internal.visor.query.VisorQueryUtils.scheduleResultSetHolderRemoval;

/**
 * Task for execute SQL fields query and get first page of results.
 */
@GridInternal
@GridVisorManagementTask
public class VisorQueryTask extends VisorOneNodeTask<VisorQueryTaskArg, VisorEither<VisorQueryResult>> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorQueryJob job(VisorQueryTaskArg arg) {
        return new VisorQueryJob(arg, debug);
    }

    /**
     * Job for execute SCAN or SQL query and get first page of results.
     */
    private static class VisorQueryJob extends VisorJob<VisorQueryTaskArg, VisorEither<VisorQueryResult>> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * Create job with specified argument.
         *
         * @param arg Job argument.
         * @param debug Debug flag.
         */
        private VisorQueryJob(VisorQueryTaskArg arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected VisorEither<VisorQueryResult> run(final VisorQueryTaskArg arg) {
            try {
                UUID nid = ignite.localNode().id();

                SqlFieldsQuery qry = new SqlFieldsQuery(arg.getQueryText());
                qry.setPageSize(arg.getPageSize());
                qry.setLocal(arg.isLocal());
                qry.setDistributedJoins(arg.isDistributedJoins());
                qry.setCollocated(arg.isCollocated());
                qry.setEnforceJoinOrder(arg.isEnforceJoinOrder());
                qry.setReplicatedOnly(arg.isReplicatedOnly());
                qry.setLazy(arg.getLazy());

                long start = U.currentTimeMillis();

                List<FieldsQueryCursor<List<?>>> qryCursors;

                String cacheName = arg.getCacheName();

                if (F.isEmpty(cacheName))
                    qryCursors = ignite.context().query().querySqlFields(qry, true, false);
                else {
                    IgniteCache<Object, Object> c = ignite.cache(cacheName);

                    if (c == null)
                        throw new SQLException("Fail to execute query. Cache not found: " + cacheName);

                    qryCursors = ((IgniteCacheProxy)c.withKeepBinary()).queryMultipleStatements(qry);
                }

                // In case of multiple statements leave opened only last cursor.
                for (int i = 0; i < qryCursors.size() - 1; i++)
                    U.closeQuiet(qryCursors.get(i));

                // In case of multiple statements return last cursor as result.
                VisorQueryCursor<List<?>> cur = new VisorQueryCursor<>(F.last(qryCursors));

                Collection<GridQueryFieldMetadata> meta = cur.fieldsMeta();

                if (meta == null)
                    return new VisorEither<>(
                        new VisorExceptionWrapper(new SQLException("Fail to execute query. No metadata available.")));
                else {
                    List<VisorQueryField> names = new ArrayList<>(meta.size());

                    for (GridQueryFieldMetadata col : meta)
                        names.add(new VisorQueryField(col.schemaName(), col.typeName(),
                            col.fieldName(), col.fieldTypeName()));

                    List<Object[]> rows = fetchSqlQueryRows(cur, arg.getPageSize());

                    // Query duration + fetch duration.
                    long duration = U.currentTimeMillis() - start;

                    boolean hasNext = cur.hasNext();

                    // Generate query ID to store query cursor in node local storage.
                    String qryId = SQL_QRY_NAME + "-" + UUID.randomUUID();

                    if (hasNext) {
                        ignite.cluster().<String, VisorQueryCursor<List<?>>>nodeLocalMap().put(qryId, cur);

                        scheduleResultSetHolderRemoval(qryId, ignite);
                    }
                    else
                        cur.close();

                    return new VisorEither<>(new VisorQueryResult(nid, qryId, names, rows, hasNext, duration));
                }
            }
            catch (Throwable e) {
                return new VisorEither<>(new VisorExceptionWrapper(e));
            }
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(VisorQueryJob.class, this);
        }
    }
}
