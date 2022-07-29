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

package org.apache.ignite.internal.commandline.dr.subcommands;

import java.util.Collection;
import java.util.logging.Logger;
import org.apache.ignite.internal.client.GridClient;
import org.apache.ignite.internal.client.GridClientCompute;
import org.apache.ignite.internal.client.GridClientConfiguration;
import org.apache.ignite.internal.client.GridClientDisconnectedException;
import org.apache.ignite.internal.client.GridClientNode;
import org.apache.ignite.internal.commandline.CommandArgIterator;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.visor.VisorTaskArgument;
import org.apache.ignite.internal.visor.dr.VisorDrCacheLocalIncTaskResult;
import org.apache.ignite.internal.visor.dr.VisorDrIncrementalTransferCmdArgs;
import static org.apache.ignite.internal.commandline.CommandHandler.DELIM;
import static org.apache.ignite.internal.commandline.dr.DrSubCommandsList.INC_TRANSFER;

/** */
public class DrIncrementalTransferCommand
    extends DrAbstractRemoteSubCommand<VisorDrIncrementalTransferCmdArgs, VisorDrCacheLocalIncTaskResult, DrIncrementalTransferCommand.DrIncTransferArguments> {
    /**
     * Container for command arguments.
     */
    static class DrIncTransferArguments implements DrAbstractRemoteSubCommand.Arguments<VisorDrIncrementalTransferCmdArgs> {
        /** Snapshot id. */
        private long snapshotId;

        /** Data center id. */
        private byte dcId;

        /** Cache name. */
        private String cacheName;

        /** */
        public DrIncTransferArguments(String cacheName, long snapshotId, byte dcId) {
            this.cacheName = cacheName;
            this.snapshotId = snapshotId;
            this.dcId = dcId;
        }

        /**
         * @return Snapshot id.
         */
        public long snapshotId() {
            return snapshotId;
        }

        /**
         * @return Data center id.
         */
        public byte dcId() {
            return dcId;
        }

        /**
         * @return Cache name.
         */
        public String cacheName() {
            return cacheName;
        }

        /** {@inheritDoc} */
        @Override public VisorDrIncrementalTransferCmdArgs toVisorArgs() {
            return new VisorDrIncrementalTransferCmdArgs(cacheName, snapshotId, dcId);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(DrIncrementalTransferCommand.Arguments.class, this);
        }
    }

    /** {@inheritDoc} */
    @Override public DrIncTransferArguments parseArguments0(CommandArgIterator argIter) {
        String cacheName = argIter.nextArg("Cache name expected.");

        long snapshotId = Long.parseLong(argIter.nextArg("Snapshot identificator expected."));

        byte dcId = Byte.parseByte(argIter.nextArg("Remote data center identificator expected."));

        return new DrIncrementalTransferCommand.DrIncTransferArguments(cacheName, snapshotId, dcId);
    }

    /** {@inheritDoc} */
    @Override protected VisorDrCacheLocalIncTaskResult execute0(
        GridClientConfiguration clientCfg,
        GridClient client
    ) throws Exception {
        GridClientCompute compute = client.compute();

        Collection<GridClientNode> connectableNodes = compute.nodes(GridClientNode::connectable);

        if (F.isEmpty(connectableNodes))
            throw new GridClientDisconnectedException("Connectable nodes not found", null);

        GridClientNode node = compute.balancer().balancedNode(connectableNodes);

        return compute.projection(node).execute(
            visorTaskName(),
            new VisorTaskArgument<>(node.nodeId(), arg().toVisorArgs(), false)
        );
    }

    /** {@inheritDoc} */
    @Override protected void printResult(VisorDrCacheLocalIncTaskResult res, Logger log) {
        printUnrecognizedNodesMessage(log, false);

        log.info("Data Center ID: " + res.dataCenterId());

        log.info(DELIM);

        if (res.dataCenterId() == 0) {
            log.info("Data Replication state: is not configured.");

            return;
        }

        log.info(res.resultMessage());
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return INC_TRANSFER.text().toUpperCase();
    }

    /** {@inheritDoc} */
    @Override protected String visorTaskName() {
        return "org.gridgain.grid.internal.visor.dr.console.VisorDrStartIncrementalTransferTask";
    }
}
