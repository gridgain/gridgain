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

package org.apache.ignite.internal.commandline.cache;

import java.util.Objects;
import java.util.Set;
import java.util.logging.Logger;
import org.apache.ignite.internal.IgniteFeatures;
import org.apache.ignite.internal.client.GridClient;
import org.apache.ignite.internal.client.GridClientConfiguration;
import org.apache.ignite.internal.client.GridClientNode;
import org.apache.ignite.internal.commandline.Command;
import org.apache.ignite.internal.visor.cache.CheckIndexInlineSizesResult;
import org.apache.ignite.internal.visor.cache.CheckIndexInlineSizesTask;

import static java.util.stream.Collectors.toSet;
import static org.apache.ignite.internal.IgniteFeatures.nodeSupports;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_CLIENT_MODE;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_IGNITE_FEATURES;
import static org.apache.ignite.internal.commandline.cache.CacheCommands.usageCache;
import static org.apache.ignite.internal.commandline.cache.CacheSubcommands.CHECK_INDEX_INLINE_SIZES;

/**
 * Command for check secondary indexes inline size on the different nodes.
 */
public class CheckIndexInlineSizes implements Command<Void> {
    /** {@inheritDoc} */
    @Override public Object execute(GridClientConfiguration clientCfg, Logger log) throws Exception {
        try (GridClient client = Command.startClient(clientCfg)) {
            Set<GridClientNode> serverNodes = client.compute().nodes().stream()
                .filter(node -> Objects.equals(node.attribute(ATTR_CLIENT_MODE), false))
                .collect(toSet());

            Set<GridClientNode> supportedServerNodes = serverNodes.stream()
                .filter(CheckIndexInlineSizes::checkIndexInlineSizesSupported)
                .collect(toSet());

            CheckIndexInlineSizesResult res =
                client.compute().projection(supportedServerNodes).execute(CheckIndexInlineSizesTask.class.getName(), null);

            analizeResults(log, serverNodes, supportedServerNodes, res);
        }

        return null;
    }

    /** */
    private void analizeResults(
        Logger log,
        Set<GridClientNode> serverNodes,
        Set<GridClientNode> supportedServerNodes,
        CheckIndexInlineSizesResult res
    ) {

    }

    /** {@inheritDoc} */
    @Override public Void arg() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public void printUsage(Logger logger) {
        usageCache(
            logger,
            CHECK_INDEX_INLINE_SIZES,
            "Checks that secondary indexes inline size are same on the cluster nodes.",
            null
        );
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return CHECK_INDEX_INLINE_SIZES.text().toUpperCase();
    }

    /** */
    private static boolean checkIndexInlineSizesSupported(GridClientNode node) {
        return nodeSupports(node.attribute(ATTR_IGNITE_FEATURES), IgniteFeatures.CHECK_INDEX_INLINE_SIZES);
    }
}
