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
package org.apache.ignite.internal.commandline.ru;

import java.util.logging.Logger;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.client.GridClient;
import org.apache.ignite.internal.client.GridClientConfiguration;
import org.apache.ignite.internal.commandline.Command;
import org.apache.ignite.internal.commandline.CommandArgIterator;
import org.apache.ignite.internal.commandline.CommandLogger;
import org.apache.ignite.internal.commandline.argument.CommandArgUtils;
import org.apache.ignite.internal.processors.ru.RollingUpgradeModeChangeResult;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.visor.ru.VisorRollingUpgradeChangeModeResult;
import org.apache.ignite.internal.visor.ru.VisorRollingUpgradeChangeModeTask;
import org.apache.ignite.internal.visor.ru.VisorRollingUpgradeChangeModeTaskArg;
import org.apache.ignite.internal.visor.ru.VisorRollingUpgradeOperation;
import org.apache.ignite.internal.visor.ru.VisorRollingUpgradeStatus;
import org.apache.ignite.internal.visor.ru.VisorRollingUpgradeStatusResult;
import org.apache.ignite.internal.visor.ru.VisorRollingUpgradeStatusTask;

import static org.apache.ignite.IgniteSystemProperties.getBoolean;
import static org.apache.ignite.internal.IgniteFeatures.DISTRIBUTED_ROLLING_UPGRADE_MODE;
import static org.apache.ignite.internal.commandline.CommandArgIterator.isCommandOrOption;
import static org.apache.ignite.internal.commandline.CommandList.ROLLING_UPGRADE;
import static org.apache.ignite.internal.commandline.TaskExecutor.executeTask;
import static org.apache.ignite.internal.commandline.ru.RollingUpgradeSubCommands.of;

/**
 * Represents a command associated with rolling upgrade functionality.
 */
public class RollingUpgradeCommand implements Command<RollingUpgradeArguments> {
    /** */
    private RollingUpgradeArguments rollingUpgradeArgs;

    /** {@inheritDoc} */
    @Override public Object execute(GridClientConfiguration clientCfg, Logger log) throws Exception {
        if (!getBoolean(DISTRIBUTED_ROLLING_UPGRADE_MODE.name(), false)) {
            log.severe("Failed to execute rolling upgrade command='" + rollingUpgradeArgs.command().text() + "\' "
                + "Rolling upgrade isn't supported.");

            throw new IgniteCheckedException("Rolling upgrade isn't supported.");
        }

        try (GridClient client = Command.startClient(clientCfg)) {
            if (RollingUpgradeSubCommands.STATUS == rollingUpgradeArgs.command()) {
                VisorRollingUpgradeStatusResult status = executeTask(
                    client,
                    VisorRollingUpgradeStatusTask.class,
                    null,
                    clientCfg);

                printRollingUpgradeStatus(log, status);

                return status;
            }

            VisorRollingUpgradeChangeModeResult res = executeTask(
                client,
                VisorRollingUpgradeChangeModeTask.class,
                toVisorArguments(rollingUpgradeArgs),
                clientCfg);

            printRollingUpgradeChangeModeResult(log, res);

            return res;
        }
        catch (Throwable e) {
            log.severe("Failed to execute rolling upgrade command='" + rollingUpgradeArgs.command().text() + "\' "
                + CommandLogger.errorMessage(e));

            throw e;
        }
    }

    /** {@inheritDoc} */
    @Override public RollingUpgradeArguments arg() {
        return rollingUpgradeArgs;
    }

    /** {@inheritDoc} */
    @Override public void printUsage(Logger logger) {
        Command.usage(logger, "Enable rolling upgrade:", ROLLING_UPGRADE, RollingUpgradeSubCommands.ENABLE.text());
        Command.usage(logger, "Disable rolling upgrade:", ROLLING_UPGRADE, RollingUpgradeSubCommands.DISABLE.text());
        Command.usage(logger, "Get rolling upgrade status:", ROLLING_UPGRADE, RollingUpgradeSubCommands.STATUS.text());
    }

    /** {@inheritDoc} */
    @Override public void parseArguments(CommandArgIterator argIter) {
        RollingUpgradeSubCommands cmd = of(argIter.nextArg("Expected rolling upgrade action"));

        if (cmd == null)
            throw new IllegalArgumentException("Expected correct action");

        RollingUpgradeArguments.Builder rollingUpgradeArgs = new RollingUpgradeArguments.Builder(cmd);

        if (RollingUpgradeSubCommands.ENABLE == cmd) {
            if (argIter.peekNextArg() != null && !isCommandOrOption(argIter.peekNextArg())) {
                RollingUpgradeCommandArg cmdArg = CommandArgUtils.of(
                    argIter.nextArg("Unknown parameter for enabling rolling upgrade"), RollingUpgradeCommandArg.class);

                if (RollingUpgradeCommandArg.FORCE != cmdArg)
                    throw new IllegalArgumentException("Unknown parameter for enabling rolling upgrade");

                rollingUpgradeArgs.withForcedMode(true);
            }
        }

        this.rollingUpgradeArgs = rollingUpgradeArgs.build();
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return ROLLING_UPGRADE.toCommandName();
    }

    /**
     * Prepare task argument.
     *
     * @param args Argument from command line.
     * @return Task argument.
     */
    private VisorRollingUpgradeChangeModeTaskArg toVisorArguments(RollingUpgradeArguments args) {
        assert RollingUpgradeSubCommands.ENABLE == args.command() || RollingUpgradeSubCommands.DISABLE == args.command();

        return RollingUpgradeSubCommands.ENABLE == args.command() ?
            new VisorRollingUpgradeChangeModeTaskArg(VisorRollingUpgradeOperation.ENABLE, args.isForcedMode())
            : new VisorRollingUpgradeChangeModeTaskArg(VisorRollingUpgradeOperation.DISABLE, false);
    }

    /**
     * Prints the given rolling upgrade status.
     *
     * @param res Rolling upgrade status.
     */
    private void printRollingUpgradeStatus(Logger log, VisorRollingUpgradeStatusResult res) {
        VisorRollingUpgradeStatus status = res.getStatus();

        log.info("Rolling upgrade is " + (status.isEnabled() ? "enabled" : "disabled"));
        log.info("Initial version: " + status.getInitialVersion());
        log.info("Target version: " + ((F.isEmpty(status.getTargetVersion())) ? "N/A" : status.getTargetVersion()));

        if (status.isForcedModeEnabled())
            log.info("Forced mode is enabled.");

        if (status.isEnabled()) {
            log.info("List of alive nodes in the cluster that are not updated yet:");

            res.getInitialNodes().forEach(id -> log.info(CommandLogger.INDENT + id));

            log.info("List of alive nodes in the cluster that are updated:");

            res.getUpdatedNodes().forEach(id -> log.info(CommandLogger.INDENT + id));
        }
    }

    /**
     * Prints the given mode change result.
     *
     * @param res Mode change result.
     */
    private void printRollingUpgradeChangeModeResult(Logger log, VisorRollingUpgradeChangeModeResult res) {
        if (RollingUpgradeModeChangeResult.Result.SUCCESS == res.getResult())
            log.info("Rolling upgrade mode successfully " +
                (RollingUpgradeSubCommands.ENABLE == rollingUpgradeArgs.command() ? "enabled." : "disabled."));
        else
            log.info("Rolling upgrade operation failed. " + res.getCause().getMessage());
    }
}
