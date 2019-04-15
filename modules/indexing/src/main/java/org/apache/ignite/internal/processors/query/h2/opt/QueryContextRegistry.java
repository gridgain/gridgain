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

package org.apache.ignite.internal.processors.query.h2.opt;

import org.jetbrains.annotations.Nullable;

import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Registry of all currently available query contexts.
 */
public class QueryContextRegistry {
    /** Current local context. */
    private final ThreadLocal<QueryContext> locCtx = new ThreadLocal<>();

    /** Shared contexts. */
    private final ConcurrentMap<QueryContextKey, QueryContext> sharedCtxs = new ConcurrentHashMap<>();

    /**
     * Access current thread local query context (if it was set).
     *
     * @return Current thread local query context or {@code null} if the query runs outside of Ignite context.
     */
    @Nullable public QueryContext getThreadLocal() {
        return locCtx.get();
    }

    /**
     * Sets current thread local context. This method must be called when all the non-volatile properties are
     * already set to ensure visibility for other threads.
     *
     * @param x Query context.
     */
    public void setThreadLocal(QueryContext x) {
        assert locCtx.get() == null;

        locCtx.set(x);
    }

    /**
     * Drops current thread local context.
     */
    public void clearThreadLocal() {
        locCtx.remove();
    }

    /**
     * Access query context from another thread.
     *
     * @param nodeId The node who initiated the query.
     * @param qryId The query ID.
     * @param segmentId Index segment ID.
     * @return Query context.
     */
    @Nullable public QueryContext getShared(UUID nodeId, long qryId, int segmentId) {
        return sharedCtxs.get(new QueryContextKey(nodeId, qryId, segmentId));
    }

    /**
     * Sets current thread local context. This method must be called when all the non-volatile properties are
     * already set to ensure visibility for other threads.
     *
     * @param ctx Query context.
     */
    public void setShared(UUID nodeId, long qryId, QueryContext ctx) {
        assert ctx.distributedJoinContext() != null;

        QueryContextKey key = new QueryContextKey(nodeId, qryId, ctx.segment());

        QueryContext oldCtx = sharedCtxs.putIfAbsent(key, ctx);

        assert oldCtx == null;
    }

    /**
     * Clear shared context.
     *
     * @param nodeId The node who initiated the query.
     * @param qryId The query ID.
     * @return {@code True} if context was found.
     */
    public boolean clearShared(UUID nodeId, long qryId) {
        boolean res = false;

        for (QueryContextKey key : sharedCtxs.keySet()) {
            if (key.nodeId().equals(nodeId) && key.queryId() == qryId)
                res |= doClear(key, false);
        }

        return res;
    }

    /**
     * Clear shared contexts on local node stop.
     */
    public void clearSharedOnLocalNodeStop() {
        for (QueryContextKey key : sharedCtxs.keySet())
            doClear(key, true);
    }

    /**
     * Clear shared contexts on remote node stop.
     *
     * @param nodeId Remote node ID.
     */
    public void clearSharedOnRemoteNodeStop(UUID nodeId) {
        for (QueryContextKey key : sharedCtxs.keySet()) {
            if (key.nodeId().equals(nodeId))
                doClear(key, false);
        }
    }

    /**
     * @param key Context key.
     * @param nodeStop Node is stopping.
     * @return {@code True} if context was found.
     */
    private boolean doClear(QueryContextKey key, boolean nodeStop) {
        QueryContext ctx = sharedCtxs.remove(key);

        if (ctx == null)
            return false;

        ctx.clearContext(nodeStop);

        return true;
    }
}
