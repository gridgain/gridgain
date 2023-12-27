/*
 * Copyright 2022 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.internal.processors.configuration.distributed;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.subscription.GridInternalSubscriptionProcessor;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.cluster.DistributedConfigurationUtils.makeUpdateListener;
import static org.apache.ignite.internal.processors.configuration.distributed.DistributedBooleanProperty.detachedBooleanProperty;
import static org.apache.ignite.internal.processors.configuration.distributed.DistributedIntegerProperty.detachedIntegerProperty;

/**
 * Thin client distributed configuration.
 */
public class DistributedThinClientConfiguration {
    /** Default connection count limit. */
    public static final int DFLT_MAX_CONNECTIONS_PER_NODE = 0;

    /** Message of thin client distributed config parameter was changed. */
    private static final String PROPERTY_UPDATE_MESSAGE =
            "ThinClientProperty parameter '%s' was changed from '%s' to '%s'";

    /** . */
    private final DistributedChangeableProperty<Boolean> showStackTrace =
            detachedBooleanProperty("thinClientProperty.showStackTrace");

    /**
     * Maximum allowed number of active thin client connections per node.
     * <p>
     * Defaults to {@link #DFLT_MAX_CONNECTIONS_PER_NODE}.
     * This applies to any connections that use thin client protocol: thin clients, ODBC, thin JDBC connections.
     * The total number of all connections (ODBC+JDBC+thin client) can not exceed this limit.
     * Zero or negative value means no limit.
     */
    private final DistributedChangeableProperty<Integer> maxConnectionsPerNode =
            detachedIntegerProperty("maxConnectionsPerNode");

    /** */
    private final IgniteLogger log;

    /**
     * @param ctx Kernal context.
     */
    public DistributedThinClientConfiguration(
        GridKernalContext ctx
    ) {
        log = ctx.log(DistributedThinClientConfiguration.class);

        GridInternalSubscriptionProcessor isp = ctx.internalSubscriptionProcessor();

        isp.registerDistributedConfigurationListener(
            new DistributedConfigurationLifecycleListener() {
                @Override public void onReadyToRegister(DistributedPropertyDispatcher dispatcher) {
                    showStackTrace.addListener(makeUpdateListener(PROPERTY_UPDATE_MESSAGE, log));
                    maxConnectionsPerNode.addListener(makeUpdateListener(PROPERTY_UPDATE_MESSAGE, log));

                    dispatcher.registerProperties(showStackTrace);
                    dispatcher.registerProperties(maxConnectionsPerNode);
                }
            }
        );
    }

    /**
     * @param showStack If {@code true} shows full stack trace on the client side.
     * @return Future for update operation.
     */
    public GridFutureAdapter<?> updateThinClientSendServerStackTraceAsync(boolean showStack) throws IgniteCheckedException {
        return showStackTrace.propagateAsync(showStack);
    }

    /**
     * @return If {@code true}, thin client response will include full stack trace when exception occurs.
     * When {@code false}, only top level exception message is included.
     */
    @Nullable public Boolean sendServerExceptionStackTraceToClient() {
        return showStackTrace.get();
    }

    /**
     * Cluster wide update of {@link #maxConnectionsPerNode}.
     *
     * @param limit New value of {@link #maxConnectionsPerNode}.
     * @return Future for {@link #maxConnectionsPerNode} update operation.
     * @throws IgniteCheckedException If failed during cluster wide update.
     */
    public GridFutureAdapter<?> updateMaxConnectionsPerNodeAsync(int limit) throws IgniteCheckedException {
        return maxConnectionsPerNode.propagateAsync(limit);
    }

    /**
     * Local update of {@link #maxConnectionsPerNode}.
     *
     * @param limit New value of {@link #maxConnectionsPerNode}.
     */
    public void updateMaxConnectionsPerNodeLocal(int limit) {
        maxConnectionsPerNode.localUpdate(limit);
    }

    /**
     * @return Maximum allowed number of active client connections per node. See {@link #maxConnectionsPerNode}.
     */
    public int maxConnectionsPerNode() {
        Integer value = maxConnectionsPerNode.getOrDefault(DFLT_MAX_CONNECTIONS_PER_NODE);
        assert value != null;

        return value;
    }
}
