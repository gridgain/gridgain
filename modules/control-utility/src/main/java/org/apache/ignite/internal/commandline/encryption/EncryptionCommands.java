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

package org.apache.ignite.internal.commandline.encryption;

import java.util.logging.Logger;
import org.apache.ignite.internal.client.GridClientConfiguration;
import org.apache.ignite.internal.commandline.AbstractCommand;
import org.apache.ignite.internal.commandline.CommandArgIterator;
import org.apache.ignite.internal.commandline.CommandList;

/**
 * Commands related to encryption functions.
 *
 * @see EncryptionSubcommands
 */
public class EncryptionCommands extends AbstractCommand<EncryptionSubcommands> {
    /** Subcommand. */
    private EncryptionSubcommands cmd;

    /** {@inheritDoc} */
    @Override public Object execute(GridClientConfiguration clientCfg, Logger logger) throws Exception {
        return cmd.subcommand().execute(clientCfg, logger);
    }

    /** {@inheritDoc} */
    @Override public void parseArguments(CommandArgIterator argIter) {
        EncryptionSubcommands cmd = EncryptionSubcommands.of(argIter.nextArg("Expected encryption action."));

        if (cmd == null)
            throw new IllegalArgumentException("Expected correct encryption action.");

        cmd.subcommand().parseArguments(argIter);

        if (argIter.hasNextSubArg())
            throw new IllegalArgumentException("Unexpected argument of --encryption subcommand: " + argIter.peekNextArg());

        this.cmd = cmd;
    }

    /** {@inheritDoc} */
    @Override public EncryptionSubcommands arg() {
        return cmd;
    }

    /** {@inheritDoc} */
    @Override public void printUsage(Logger logger) {
        for (EncryptionSubcommands cmd : EncryptionSubcommands.values())
            cmd.subcommand().printUsage(logger);
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return CommandList.ENCRYPTION.toCommandName();
    }
}
