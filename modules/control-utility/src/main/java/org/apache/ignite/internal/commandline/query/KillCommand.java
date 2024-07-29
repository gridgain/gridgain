/*
 * Copyright 2024 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.internal.commandline.query;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.logging.Logger;
import org.apache.ignite.internal.client.GridClient;
import org.apache.ignite.internal.client.GridClientConfiguration;
import org.apache.ignite.internal.commandline.Command;
import org.apache.ignite.internal.commandline.CommandArgIterator;
import org.apache.ignite.internal.commandline.CommandLogger;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.visor.query.VisorContinuousQueryCancelTask;
import org.apache.ignite.internal.visor.query.VisorContinuousQueryCancelTaskArg;
import org.apache.ignite.internal.visor.query.VisorQueryCancelOnInitiatorTask;
import org.apache.ignite.internal.visor.query.VisorQueryCancelOnInitiatorTaskArg;

import static java.util.Collections.singletonMap;
import static org.apache.ignite.internal.commandline.CommandList.KILL;
import static org.apache.ignite.internal.commandline.TaskExecutor.executeTaskByNameOnNode;
import static org.apache.ignite.internal.commandline.query.KillSubcommand.CONTINUOUS;
import static org.apache.ignite.internal.commandline.query.KillSubcommand.SQL;
import static org.apache.ignite.internal.sql.command.SqlKillQueryCommand.parseGlobalQueryId;
import static org.apache.ignite.internal.QueryMXBeanImpl.EXPECTED_GLOBAL_QRY_ID_FORMAT;

/**
 * control.sh kill command.
 *
 * @see KillSubcommand
 */
public class KillCommand implements Command<Object> {
    /** Command argument. */
    private Object taskArgs;

    /** Task name. */
    private String taskName;

    /** {@inheritDoc} */
    @Override public Object execute(GridClientConfiguration clientCfg, Logger log) throws Exception {
        try (GridClient client = Command.startClient(clientCfg)) {
            return executeTaskByNameOnNode(
                client,
                taskName,
                taskArgs,
                null,
                clientCfg
            );
        }
        catch (Throwable e) {
            log.severe("Failed to perform operation.");
            log.severe(CommandLogger.errorMessage(e));

            throw e;
        }
    }

    /** {@inheritDoc} */
    @Override public Object arg() {
        return taskArgs;
    }

    /** {@inheritDoc} */
    @Override public void parseArguments(CommandArgIterator argIter) {
        KillSubcommand cmd;

        try {
            cmd = KillSubcommand.valueOf(argIter.nextArg("Expected type of resource to kill.").toUpperCase());
        }
        catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Expected type of resource to kill.");
        }

        switch (cmd) {
            case SQL:
                T2<UUID, Long> ids = parseGlobalQueryId(argIter.nextArg("Expected SQL query id."));
                if (ids == null)
                    throw new IllegalArgumentException("Expected global query id. " + EXPECTED_GLOBAL_QRY_ID_FORMAT);
                taskArgs = new VisorQueryCancelOnInitiatorTaskArg(ids.get1(), ids.get2());
                taskName = VisorQueryCancelOnInitiatorTask.class.getName();
                break;

            case CONTINUOUS:
                taskArgs = new VisorContinuousQueryCancelTaskArg(
                    UUID.fromString(argIter.nextArg("Expected query originating node id.")),
                    UUID.fromString(argIter.nextArg("Expected continuous query id.")));

                taskName = VisorContinuousQueryCancelTask.class.getName();

                break;

            default:
                throw new IllegalArgumentException("Unknown kill subcommand: " + cmd);
        }
    }

    /** {@inheritDoc} */
    @Override public void printUsage(Logger log) {
        Command.usage(log, "Kill sql query by query id:", KILL, singletonMap("query_id", "Query identifier."),
            SQL.toString(), "query_id");
        Map<String, String> params = new HashMap<>();
        params.put("origin_node_id", "Originating node id.");
        params.put("routine_id", "Routine identifier.");

        Command.usage(log, "Kill continuous query by routine id:", KILL, params, CONTINUOUS.toString(),
            "origin_node_id", "routine_id");
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return KILL.toCommandName();
    }
}
