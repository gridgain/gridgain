/*
 * Copyright 2020 GridGain Systems, Inc. and Contributors.
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

import java.util.logging.Logger;
import org.apache.ignite.internal.client.GridClientConfiguration;
import org.apache.ignite.internal.commandline.rebalance.RebalanceStatusArguments;

import static org.apache.ignite.internal.commandline.CommandList.REBALANCE_STATUS;

public class RebalanceStatusCommand implements Command<RebalanceStatusArguments> {
    /** Arguments. */
    private RebalanceStatusArguments rebalanceStatusArguments;

    @Override public Object execute(GridClientConfiguration clientCfg, Logger logger) throws Exception {

        System.out.println("org.apache.ignite.internal.commandline.RebalanceStatusCommand.execute");


        return null;
    }

    @Override public RebalanceStatusArguments arg() {
        return rebalanceStatusArguments;
    }

    @Override public void printUsage(Logger logger) {
        Command.usage(logger, "Print rebalance status:", REBALANCE_STATUS);
    }

    @Override public String name() {
        return REBALANCE_STATUS.toCommandName();
    }

    /** {@inheritDoc} */
    @Override public void parseArguments(CommandArgIterator argIter) {
        rebalanceStatusArguments = new RebalanceStatusArguments.Builder().build();
    }
}
