/*
 * Copyright 2023 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.internal.cluster;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.configuration.distributed.DistributedChangeableProperty;
import org.apache.ignite.internal.processors.configuration.distributed.DistributedConfigurationLifecycleListener;
import org.apache.ignite.internal.processors.configuration.distributed.DistributedPropertyDispatcher;
import org.apache.ignite.internal.processors.metastorage.ReadableDistributedMetaStorage;
import org.apache.ignite.internal.util.future.GridFutureAdapter;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_MAX_THIN_CONNECTIONS_PER_NODE;
import static org.apache.ignite.IgniteSystemProperties.getInteger;
import static org.apache.ignite.internal.cluster.DistributedConfigurationUtils.makeUpdateListener;
import static org.apache.ignite.internal.cluster.DistributedConfigurationUtils.setDefaultValue;
import static org.apache.ignite.internal.processors.configuration.distributed.DistributedIntegerProperty.detachedIntegerProperty;

/**
 * Distributed Client Listener configuration.
 */
public class DistributedClientListenerConfiguration {
    /** Default connection count limit. */
    public static final int DFLT_MAX_CONNECTIONS_PER_NODE = 0;

    /** Property update message. */
    private static final String PROPERTY_UPDATE_MESSAGE =
        "Client listener parameter '%s' was changed from '%s' to '%s'";

    /** Default value of {@link #maxConnectionsPerNode}. */
    private final int dfltMaxConnectionsPerNode =
        getInteger(IGNITE_MAX_THIN_CONNECTIONS_PER_NODE, DFLT_MAX_CONNECTIONS_PER_NODE);

    /**
     * Maximum allowed number of active thin client connections per node.
     * <p>
     * Defaults to {@link #DFLT_MAX_CONNECTIONS_PER_NODE}.
     * This applies to any connections that use thin client protocol: thin clients, ODBC, thin JDBC connections.
     * The total number of all connections (ODBC+JDBC+thin client) can not exceed this limit.
     * Must be non-negative integer value. Zero means no limit.
     */
    private final DistributedChangeableProperty<Integer> maxConnectionsPerNode =
        detachedIntegerProperty("maxConnectionsPerNode");

    /**
     * @param ctx Kernal context.
     * @param log Log.
     */
    public DistributedClientListenerConfiguration(GridKernalContext ctx, IgniteLogger log) {
        ctx.internalSubscriptionProcessor().registerDistributedConfigurationListener(
            new DistributedConfigurationLifecycleListener() {
                @Override public void onReadyToRegister(DistributedPropertyDispatcher dispatcher) {
                    maxConnectionsPerNode.addListener(makeUpdateListener(PROPERTY_UPDATE_MESSAGE, log));

                    dispatcher.registerProperties(maxConnectionsPerNode);
                }

                @Override public void onReadyToWrite() {
                    if (ReadableDistributedMetaStorage.isSupported(ctx)) {
                        setDefaultValue(maxConnectionsPerNode, dfltMaxConnectionsPerNode, log);
                    } else {
                        log.warning("Distributed metastorage is not supported. " +
                            "All distributed Client Listener configuration parameters are unavailable. " +
                            "Default values will be set.");

                        maxConnectionsPerNode.localUpdate(dfltMaxConnectionsPerNode);
                    }
                }
            }
        );
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
    public Integer maxConnectionsPerNode() {
        return maxConnectionsPerNode.getOrDefault(dfltMaxConnectionsPerNode);
    }
}
