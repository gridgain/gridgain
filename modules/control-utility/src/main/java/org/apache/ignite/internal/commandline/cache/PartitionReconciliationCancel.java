/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.commandline.cache;

import java.util.HashMap;
import java.util.logging.Logger;
import org.apache.ignite.internal.client.GridClient;
import org.apache.ignite.internal.client.GridClientConfiguration;
import org.apache.ignite.internal.commandline.AbstractCommand;
import org.apache.ignite.internal.commandline.Command;
import org.apache.ignite.internal.visor.checker.VisorPartitionReconciliationCancelTask;

import static org.apache.ignite.internal.commandline.TaskExecutor.executeTask;
import static org.apache.ignite.internal.commandline.cache.CacheCommands.usageCache;
import static org.apache.ignite.internal.commandline.cache.CacheSubcommands.PARTITION_RECONCILIATION_CANCEL;

/**
 * Partition reconciliation cancel command.
 */
public class PartitionReconciliationCancel extends AbstractCommand<Void> {
    /** {@inheritDoc} */
    @Override public void printUsage(Logger log) {
        String desc = "Cancels partition reconciliation command.";

        usageCache(log, PARTITION_RECONCILIATION_CANCEL, desc, new HashMap<>());
    }

    /** {@inheritDoc} */
    @Override public Void arg() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return PARTITION_RECONCILIATION_CANCEL.text().toUpperCase();
    }

    /** {@inheritDoc} */
    @Override public Void execute(GridClientConfiguration clientCfg, Logger log) throws Exception {
        try (GridClient client = Command.startClient(clientCfg)) {
            executeTask(client, VisorPartitionReconciliationCancelTask.class, null, clientCfg);
        }

        return null;
    }
}
