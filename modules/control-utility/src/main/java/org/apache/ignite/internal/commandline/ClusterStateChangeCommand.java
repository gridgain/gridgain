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

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.internal.client.GridClient;
import org.apache.ignite.internal.client.GridClientClusterState;
import org.apache.ignite.internal.client.GridClientConfiguration;
import org.apache.ignite.internal.client.GridClientException;
import org.apache.ignite.internal.client.GridClientNode;
import org.apache.ignite.internal.commandline.baseline.BaselineArguments;
import org.apache.ignite.internal.commandline.baseline.BaselineSubcommands;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.visor.baseline.VisorBaselineNode;
import org.apache.ignite.internal.visor.baseline.VisorBaselineTask;
import org.apache.ignite.internal.visor.baseline.VisorBaselineTaskArg;
import org.apache.ignite.internal.visor.baseline.VisorBaselineTaskResult;

import static java.util.stream.Collectors.toSet;
import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.cluster.ClusterState.ACTIVE_READ_ONLY;
import static org.apache.ignite.cluster.ClusterState.INACTIVE;
import static org.apache.ignite.internal.IgniteFeatures.CLUSTER_READ_ONLY_MODE;
import static org.apache.ignite.internal.IgniteFeatures.SAFE_CLUSTER_DEACTIVATION;
import static org.apache.ignite.internal.commandline.CommandList.SET_STATE;
import static org.apache.ignite.internal.commandline.CommandLogger.optional;
import static org.apache.ignite.internal.commandline.CommandLogger.or;
import static org.apache.ignite.internal.commandline.CommonArgParser.CMD_AUTO_CONFIRMATION;
import static org.apache.ignite.internal.commandline.TaskExecutor.executeTaskByNameOnNode;
import static org.apache.ignite.internal.commandline.util.TopologyUtils.coordinatorId;

/**
 * Command to change cluster state.
 */
public class ClusterStateChangeCommand extends AbstractCommand<ClusterState> {
    /** Flag of forced cluster deactivation. */
    public static final String FORCE_COMMAND = "--force";

    /** New cluster state */
    private ClusterState state;

    /** Cluster name. */
    private String clusterName;

    /** If {@code true}, cluster deactivation will be forced. */
    private boolean forceDeactivation;

    /** Offline nodes. */
    private Set<String> offlineNodes = new HashSet<>();

    /** Flag to check partial activation */
    private boolean partialActivationCheckComplete = false;

    /** {@inheritDoc} */
    @Override public void printUsage(Logger log) {
        Map<String, String> params = new LinkedHashMap<>();

        params.put(ACTIVE.toString(), "Activate cluster. Cache updates are allowed.");
        params.put(INACTIVE.toString(), "Deactivate cluster.");
        params.put(ACTIVE_READ_ONLY.toString(), "Activate cluster. Cache updates are denied.");

        Command.usage(log, "Change cluster state:", SET_STATE, params, or((Object[])ClusterState.values()),
            optional(FORCE_COMMAND), optional(CMD_AUTO_CONFIRMATION));
    }

    /** {@inheritDoc} */
    @Override public void prepareConfirmation(GridClientConfiguration clientCfg) throws Exception {
        try (GridClient client = Command.startClient(clientCfg)) {
            GridClientClusterState clientState = client.state();
            
            if (!clientState.state().equals(INACTIVE))
                clusterName = clientState.clusterName();

            if (state == ACTIVE)
                partialActivationCheckComplete = partialActivationCheck(client, clientCfg);
        }
    }

    /** {@inheritDoc} */
    @Override public String confirmationPrompt() {
        return "Warning: the command will change state of cluster with name \"" + clusterName + "\" to " + state + ".";
    }

    /** {@inheritDoc} */
    @Override public Object execute(GridClientConfiguration clientCfg, Logger log) throws Exception {
        try (GridClient client = Command.startClient(clientCfg)) {
            Set<GridClientNode> serverNodes = client.compute().nodes().stream()
                .filter(n -> !n.isClient() && !n.isDaemon())
                .collect(toSet());

            Set<GridClientNode> supportedServerNodes = serverNodes.stream()
                .filter(n -> n.supports(CLUSTER_READ_ONLY_MODE))
                .collect(toSet());

            Set<GridClientNode> notSupportedSafeDeactivation = supportedServerNodes.stream()
                .filter(n -> !n.supports(SAFE_CLUSTER_DEACTIVATION))
                .collect(toSet());

            if (!supportedServerNodes.equals(serverNodes) && state == ACTIVE_READ_ONLY)
                throw new IgniteException("Not all nodes in cluster supports cluster state " + state);

            if (!notSupportedSafeDeactivation.isEmpty() && state == INACTIVE && !forceDeactivation) {
                throw new GridClientException("Deactivation stopped. Found a nodes that do not support the " +
                    "correctness checking of this operation: " + notSupportedSafeDeactivation + ". Deactivation " +
                    "clears in-memory caches (without persistence) including the system caches. " +
                    "To deactivate cluster pass '--force' flag.");
            }

            if (F.isEmpty(supportedServerNodes))
                client.state().active(ClusterState.active(state));
            else
                if (notSupportedSafeDeactivation.isEmpty())
                    client.state().state(state, forceDeactivation);
                else
                    client.state().state(state);

            if (state == ACTIVE && !partialActivationCheckComplete)
                partialActivationCheckComplete = partialActivationCheck(client, clientCfg);

            log.info("Cluster state changed to " + state);

            return null;
        }
        catch (Throwable e) {
            log.info("Failed to change cluster state to " + state);

            throw e;
        }
    }

    /** {@inheritDoc} */
    @Override public void parseArguments(CommandArgIterator argIter) {
        String s = argIter.nextArg("New cluster state not found.");

        try {
            state = ClusterState.valueOf(s.toUpperCase());
        }
        catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Can't parse new cluster state. State: " + s, e);
        }

        forceDeactivation = false;

        if (argIter.hasNextArg()) {
            String arg = argIter.peekNextArg();

            if (FORCE_COMMAND.equalsIgnoreCase(arg)) {
                forceDeactivation = true;

                argIter.nextArg("");
            }
        }
    }

    private boolean partialActivationCheck(GridClient client, GridClientConfiguration clientCfg) throws GridClientException {
        BaselineArguments args = new BaselineArguments.Builder(BaselineSubcommands.COLLECT).build();

        VisorBaselineTaskResult res = executeTaskByNameOnNode(
                client,
                VisorBaselineTask.class.getName(),
                new VisorBaselineTaskArg(
                        args.getCmd().visorBaselineOperation(),
                        args.getTopVer(),
                        args.getConsistentIds()
                ),
                coordinatorId(client.compute()),
                clientCfg
        );

        for (VisorBaselineNode node : res.getBaseline().values()) {

            String constId = node.getConsistentId();
            VisorBaselineNode srvNode = res.getServers().get(constId);

            if (srvNode == null) offlineNodes.add(constId);

        }

        if (!offlineNodes.isEmpty()) {
            System.out.println("WARNING: PARTIAL ACTIVATION detected. Baseline has " +
                    offlineNodes.size() + " offline node(s).\nOffline node(s) = " +
                    offlineNodes + "\nThis may lead to partition loss. Please ensure that this is intended.");
        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public ClusterState arg() {
        return state;
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return SET_STATE.toCommandName();
    }
}
