/*
 * Copyright 2020 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.commandline;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.logging.Logger;
import org.apache.ignite.internal.client.GridClient;
import org.apache.ignite.internal.client.GridClientConfiguration;
import org.apache.ignite.internal.commandline.argument.CommandArgUtils;
import org.apache.ignite.internal.commandline.cache.argument.TracingConfigurationCommandArg;
import org.apache.ignite.internal.commandline.tracing.configuration.TracingConfigurationArguments;
import org.apache.ignite.internal.commandline.tracing.configuration.TracingConfigurationSubcommand;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.tracing.configuration.VisorTracingConfigurationTask;
import org.apache.ignite.internal.visor.tracing.configuration.VisorTracingConfigurationTaskArg;
import org.apache.ignite.internal.visor.tracing.configuration.VisorTracingConfigurationTaskResult;
import org.apache.ignite.spi.tracing.Scope;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_ENABLE_EXPERIMENTAL_COMMAND;
import static org.apache.ignite.internal.commandline.CommandHandler.UTILITY_NAME;
import static org.apache.ignite.internal.commandline.CommandList.TRACING_CONFIGURATION;
import static org.apache.ignite.internal.commandline.CommandLogger.DOUBLE_INDENT;
import static org.apache.ignite.internal.commandline.CommandLogger.INDENT;
import static org.apache.ignite.internal.commandline.CommandLogger.join;
import static org.apache.ignite.internal.commandline.CommandLogger.optional;
import static org.apache.ignite.internal.commandline.CommonArgParser.getCommonOptions;
import static org.apache.ignite.internal.commandline.TaskExecutor.executeTaskByNameOnNode;
import static org.apache.ignite.internal.commandline.cache.argument.TracingConfigurationCommandArg.INCLUDED_SCOPES;
import static org.apache.ignite.internal.commandline.cache.argument.TracingConfigurationCommandArg.LABEL;
import static org.apache.ignite.internal.commandline.cache.argument.TracingConfigurationCommandArg.SAMPLING_RATE;
import static org.apache.ignite.internal.commandline.cache.argument.TracingConfigurationCommandArg.SCOPE;
import static org.apache.ignite.internal.commandline.tracing.configuration.TracingConfigurationSubcommand.GET_ALL;
import static org.apache.ignite.internal.commandline.tracing.configuration.TracingConfigurationSubcommand.HELP;
import static org.apache.ignite.internal.commandline.tracing.configuration.TracingConfigurationSubcommand.RESET_ALL;
import static org.apache.ignite.internal.commandline.tracing.configuration.TracingConfigurationSubcommand.of;
import static org.apache.ignite.internal.commandline.util.TopologyUtils.coordinatorId;
import static org.apache.ignite.spi.tracing.TracingConfigurationParameters.SAMPLING_RATE_ALWAYS;
import static org.apache.ignite.spi.tracing.TracingConfigurationParameters.SAMPLING_RATE_NEVER;

/**
 * Commands associated with tracing configuration functionality.
 */
public class TracingConfigurationCommand extends AbstractCommand<TracingConfigurationArguments> {
    /** Arguments. */
    private TracingConfigurationArguments args;

    /** {@inheritDoc} */
    @Override public void printUsage(Logger log) {
        if (!experimentalEnabled())
            return;

        log.info("");
        log.info(INDENT + "View or update tracing configuration in a cluster. For more details type:");
        log.info(DOUBLE_INDENT + CommandLogger.join(" ", UTILITY_NAME, TRACING_CONFIGURATION, HELP));
    }

    /** Prints all possible subcommands with parameter descriptions */
    public void printVerboseUsage(Logger log) {
        if (!experimentalEnabled()) {
            return;
        }

        log.info(INDENT + "The '" + TRACING_CONFIGURATION + "' is used to get or update tracing configuration. " +
            "The command has the following syntax:");
        log.info("");
        log.info(INDENT + CommandLogger.join(" ", UTILITY_NAME, CommandLogger.join(" ", getCommonOptions())) + " " +
            TRACING_CONFIGURATION + " [subcommand <subcommand_parameters>]");
        log.info("");
        log.info(INDENT + "<scope> value of '" + SCOPE.argName() + "' or '" + INCLUDED_SCOPES.argName() + "' options in all subcommands can be: " +
            join("|", Scope.values()));
        log.info("");
        log.info("");
        log.info(INDENT + "Subcommands:");

        usageTracing(
            log,
            "Print tracing configuration. Works as '" + TRACING_CONFIGURATION + " get_all' without params.",
            null);

        usageTracing(
            log,
            "Print tracing configuration for given " + SCOPE.argName() + ", or all scopes if nothing was specified.",
            TracingConfigurationSubcommand.GET_ALL,
            optional(SCOPE.signature()));

        usageTracing(
            log,
            "Print specific tracing configuration based on specified " +
                SCOPE.argName() + " and " + LABEL.argName() + '.',
            TracingConfigurationSubcommand.GET,
            SCOPE.signature(),
            optional(LABEL.signature()));

        usageTracing(
            log,
            "Reset all specific tracing configuration the to default. " +
                "If " + SCOPE.argName() + " is specified, " +
                "then remove all label specific configuration for the given scope " +
                "and reset given scope specific configuration to the default, " +
                "if " + SCOPE.argName() + " is skipped then reset all tracing configurations to the default. " +
                "Print resulting configuration.",
            RESET_ALL,
            optional(SCOPE.signature()));

        usageTracing(
            log,
            "Reset specific tracing configuration to the default. If both " + SCOPE.argName() + " and " +
                LABEL.argName() + " are specified then remove given configuration, " +
                "if only " + SCOPE.argName() + " is specified then reset given configuration to the default. " +
                "Print resulting configuration.",
            TracingConfigurationSubcommand.RESET,
            SCOPE.signature(),
            optional(LABEL.signature()));

        Map<String, String> setParams = U.newLinkedHashMap(4);
        setParams.put(SCOPE.signature(),
            "Scope of a trace's root span to which provided configuration will be applied.");
        setParams.put(LABEL.signature(), "Label of a traced operations");
        setParams.put(SAMPLING_RATE.signature(),
            "Decimal value between 0 and 1.0, where 0 means never and 1.0 means always. " +
            "More or less reflects the probability of sampling specific trace. " +
            "Default value is 0");
        setParams.put(INCLUDED_SCOPES.signature(),
            "Set of scopes that defines which sub-traces will be included in given trace. In other words, if child's " +
            "span scope is equals to parent's scope or it belongs to the parent's span included scopes, " +
            "then given child span will be attached to the current trace, otherwise it'll be skipped.");

        usageTracing(log, TracingConfigurationSubcommand.SET,
            "Comma separated set new tracing configuration. If both " + SCOPE.argName() + " and " +
                LABEL.argName() + " are specified then add or override label specific configuration, " +
                "if only " + SCOPE.argName() + " is specified, then override scope specific configuration. " +
                "Print applied configuration.",
            setParams,
            optional(SCOPE.signature()),
            optional(LABEL.signature()),
            optional(SAMPLING_RATE.signature()),
            optional(INCLUDED_SCOPES.signature())
        );

        log.info("");
    }

    protected static void usageTracing(
        Logger logger,
        TracingConfigurationSubcommand cmd,
        String description,
        Map<String, String> paramsDesc,
        String... args
    ) {
        logger.info("");
        logger.info(INDENT + CommandLogger.join(" ", TRACING_CONFIGURATION,
            cmd != null ? cmd.text() : "",
            CommandLogger.join(" ", args)));
        logger.info(DOUBLE_INDENT + description);

        if (!F.isEmpty(paramsDesc)) {
            logger.info("");
            logger.info(DOUBLE_INDENT + "Parameters:");

            Command.usageParams(paramsDesc, DOUBLE_INDENT + INDENT, logger);
        }
    }

    protected static void usageTracing(
        Logger logger,
        String description,
        TracingConfigurationSubcommand cmd,
        String... args
    ) {
        usageTracing(logger, cmd, description, null, args);
    }

    /**
     * Execute tracing-configuration command.
     *
     * @param clientCfg Client configuration.
     * @throws Exception If failed to execute tracing-configuration action.
     */
    @Override public Object execute(GridClientConfiguration clientCfg, Logger log) throws Exception {
        if (experimentalEnabled()) {

            if (HELP.equals(args.command())) {
                printVerboseUsage(log);
                return null;
            }

            try (GridClient client = Command.startClient(clientCfg)) {
                UUID crdId = coordinatorId(client.compute());

                VisorTracingConfigurationTaskResult res = executeTaskByNameOnNode(
                    client,
                    VisorTracingConfigurationTask.class.getName(),
                    toVisorArguments(args),
                    crdId,
                    clientCfg
                );

                printResult(res, log::info);

                return res;
            }
            catch (Throwable e) {
                log.severe("Failed to execute tracing-configuration command='" + args.command().text() + "'");

                throw e;
            }
        }
        else {
            log.warning(String.format("For use experimental command add %s=true to JVM_OPTS in %s",
                IGNITE_ENABLE_EXPERIMENTAL_COMMAND, UTILITY_NAME));

            return null;
        }
    }

    /** {@inheritDoc} */
    @Override public void parseArguments(CommandArgIterator argIter) {
        // If there is no arguments, use list command.
        if (!argIter.hasNextSubArg()) {
            args = new TracingConfigurationArguments.Builder(TracingConfigurationSubcommand.GET_ALL).build();

            return;
        }

        String cmdString = argIter.nextArg("Expected tracing configuration action.");
        if ("help".equalsIgnoreCase(cmdString) || "--help".equalsIgnoreCase(cmdString)) {
            args = new TracingConfigurationArguments.Builder(HELP).build();

            return;
        }

        TracingConfigurationSubcommand cmd = of(cmdString);

        if (cmd == null)
            throw new IllegalArgumentException("Expected correct tracing configuration action.");

        TracingConfigurationArguments.Builder tracingConfigurationArgs = new TracingConfigurationArguments.Builder(cmd);

        Scope scope = null;

        String lb = null;

        double samplingRate = SAMPLING_RATE_NEVER;

        Set<Scope> includedScopes = new HashSet<>();

        while (argIter.hasNextSubArg()) {
            TracingConfigurationCommandArg arg =
                CommandArgUtils.ofArg(TracingConfigurationCommandArg.class, argIter.nextArg(""));

            String strVal;

            assert arg != null;

            switch (arg) {
                case SCOPE: {
                    String peekedNextArg = argIter.peekNextArg();

                    if (!TracingConfigurationCommandArg.args().contains(peekedNextArg)) {
                        strVal = argIter.nextArg(
                            "The scope should be specified. The following " +
                                "values can be used: " + Arrays.toString(Scope.values()) + '.');

                        try {
                            scope = Scope.valueOf(strVal.toUpperCase());
                        }
                        catch (IllegalArgumentException e) {
                            throw new IllegalArgumentException(
                                "Invalid scope '" + strVal + "'. The following " +
                                    "values can be used: " + Arrays.toString(Scope.values()) + '.');
                        }
                    }

                    break;
                }
                case LABEL: {
                    lb = argIter.nextArg(
                        "The label should be specified.");

                    break;
                }
                case SAMPLING_RATE: {
                    strVal = argIter.nextArg(
                        "The sampling rate should be specified. Decimal value between 0 and 1 should be used.");

                    try {
                        samplingRate = Double.parseDouble(strVal);
                    }
                    catch (IllegalArgumentException e) {
                        throw new IllegalArgumentException(
                            "Invalid sampling-rate '" + strVal + "'. Decimal value between 0 and 1 should be used.");
                    }

                    if (samplingRate < SAMPLING_RATE_NEVER || samplingRate > SAMPLING_RATE_ALWAYS)
                        throw new IllegalArgumentException(
                            "Invalid sampling-rate '" + strVal + "'. Decimal value between 0 and 1 should be used.");

                    break;
                }
                case INCLUDED_SCOPES: {
                    Set<String> setStrVals = argIter.nextStringSet(
                        "At least one supported scope should be specified.");

                    for (String scopeStrVal : setStrVals) {
                        try {
                            includedScopes.add(Scope.valueOf(scopeStrVal));
                        }
                        catch (IllegalArgumentException e) {
                            throw new IllegalArgumentException(
                                "Invalid supported scope '" + scopeStrVal + "'. The following " +
                                    "values can be used: " + Arrays.toString(Scope.values()) + '.');
                        }
                    }

                    break;
                }
            }
        }

        // Scope is a mandatory attribute for all sub-commands except get_all and reset_all.
        if ((cmd != GET_ALL && cmd != RESET_ALL) && scope == null) {
            throw new IllegalArgumentException(
                "Scope attribute is missing. Following values can be used: " + Arrays.toString(Scope.values()) + '.');
        }

        switch (cmd) {
            case GET_ALL:
            case RESET_ALL: {
                tracingConfigurationArgs.withScope(scope);

                break;
            }

            case RESET:
            case GET: {
                tracingConfigurationArgs.withScope(scope).withLabel(lb);

                break;
            }

            case SET: {
                tracingConfigurationArgs.withScope(scope).withLabel(lb).withSamplingRate(samplingRate).
                    withIncludedScopes(includedScopes);

                break;
            }

            default: {
                // We should never get here.
                assert false : "Unexpected tracing configuration argument [arg= " + cmd + ']';
            }
        }

        args = tracingConfigurationArgs.build();
    }

    /** {@inheritDoc} */
    @Override public TracingConfigurationArguments arg() {
        return args;
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return TRACING_CONFIGURATION.toCommandName();
    }

    /** {@inheritDoc} */
    @Override public boolean experimental() {
        return true;
    }

    /**
     * Print result.
     *
     * @param res Visor tracing configuration result.
     * @param printer Printer.
     */
    private void printResult(VisorTracingConfigurationTaskResult res, Consumer<String> printer) {
        res.print(printer);
    }

    /**
     * Prepare task argument.
     *
     * @param args Argument from command line.
     * @return Task argument.
     */
    private VisorTracingConfigurationTaskArg toVisorArguments(TracingConfigurationArguments args) {
        return new VisorTracingConfigurationTaskArg(
            args.command().visorBaselineOperation(),
            args.scope(),
            args.label(),
            args.samplingRate(),
            args.includedScopes()
        );
    }
}
