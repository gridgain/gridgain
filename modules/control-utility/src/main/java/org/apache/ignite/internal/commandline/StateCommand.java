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

package org.apache.ignite.internal.commandline;

import java.util.UUID;
import java.util.logging.Logger;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.internal.IgniteFeatures;
import org.apache.ignite.internal.client.GridClient;
import org.apache.ignite.internal.client.GridClientClusterState;
import org.apache.ignite.internal.client.GridClientConfiguration;
import org.apache.ignite.internal.client.GridClientException;
import org.apache.ignite.internal.client.GridClientNode;

import static org.apache.ignite.internal.SupportFeaturesUtils.IGNITE_CLUSTER_ID_AND_TAG_FEATURE;
import static org.apache.ignite.internal.SupportFeaturesUtils.isFeatureEnabled;
import static org.apache.ignite.internal.commandline.CommandList.STATE;

/**
 * Command to print cluster state.
 */
public class StateCommand extends AbstractCommand<Void> {
    /** {@inheritDoc} */
    @Override public void printUsage(Logger logger) {
        Command.usage(logger, "Print current cluster state:", STATE);
    }

    /**
     * Print cluster state.
     *
     * @param clientCfg Client configuration.
     * @throws Exception If failed to print state.
     */
    @Override public Object execute(GridClientConfiguration clientCfg, Logger log) throws Exception {
        try (GridClient client = Command.startClient(clientCfg)) {
            GridClientClusterState state = client.state();

            if (isFeatureEnabled(IGNITE_CLUSTER_ID_AND_TAG_FEATURE) && allServersSupportClusterIdAndTag(client)) {
                UUID id = state.id();
                String tag = state.tag();

                log.info("Cluster  ID: " + id);
                log.info("Cluster tag: " + tag);

                log.info(CommandHandler.DELIM);
            }

            ClusterState clusterState = state.state();

            switch (clusterState) {
                case ACTIVE:
                    log.info("Cluster is active");

                    break;

                case INACTIVE:
                    log.info("Cluster is inactive");

                    break;

                case ACTIVE_READ_ONLY:
                    log.info("Cluster is active (read-only)");

                    break;

                default:
                    throw new IllegalStateException("Unknown state: " + clusterState);
            }
        }
        catch (Throwable e) {
            if (!CommandHandler.isAuthError(e))
                log.severe("Failed to get cluster state.");

            throw e;
        }

        return null;
    }

    /**
     * Checks that all servers support Cluster ID and tag feature.
     *
     * @param client GridClient to obtain topology.
     * @return {@code True} if all servers support the feature and {@code false} otherwise.
     * @throws GridClientException If failed to obtain topology.
     */
    private boolean allServersSupportClusterIdAndTag(GridClient client) throws GridClientException {
        for (GridClientNode node : client.compute().nodes()) {
            if (!node.isClient()
                && !node.isDaemon()
                && !node.supports(IgniteFeatures.CLUSTER_ID_AND_TAG))
                return false;
        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public Void arg() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return STATE.toCommandName();
    }
}
