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

import java.util.Map;
import java.util.UUID;
import java.util.logging.Logger;
import java.util.stream.LongStream;
import org.apache.ignite.internal.client.GridClient;
import org.apache.ignite.internal.client.GridClientConfiguration;
import org.apache.ignite.internal.commandline.argument.CommandParameter;
import org.apache.ignite.internal.commandline.argument.CommandParameterConfig;
import org.apache.ignite.internal.util.GridStringBuilder;
import org.apache.ignite.internal.visor.statistics.MessageStatsTask;
import org.apache.ignite.internal.visor.statistics.MessageStatsTaskArg;
import org.apache.ignite.internal.visor.statistics.MessageStatsTaskResult;

import static java.lang.String.format;
import static org.apache.ignite.internal.commandline.CommandList.STATISTICS;
import static org.apache.ignite.internal.commandline.StatisticsCommandArg.NODE;
import static org.apache.ignite.internal.commandline.StatisticsCommandArg.STATS;
import static org.apache.ignite.internal.commandline.TaskExecutor.executeTask;
import static org.apache.ignite.internal.commandline.argument.CommandArgUtils.parseArgs;

/**
 *
 */
public class Statistics implements Command<MessageStatsTaskArg> {
    /** */
    private static final CommandParameterConfig<StatisticsCommandArg> STATS_PARAMS = new CommandParameterConfig<>(
        new CommandParameter(NODE, UUID.class, true),
        new CommandParameter(STATS, MessageStatsTaskArg.StatisticsType.class)
    );

    /** */
    private static final String[] REPORT_LEADING_COLUMNS = new String[] {
        "Message",
        "Total",
        "Total time (ms)",
        "Avg (ms)"
    };


    /** */
    private MessageStatsTaskArg arg;

    /** {@inheritDoc} */
    @Override public Object execute(GridClientConfiguration clientCfg, Logger logger) throws Exception {
        try (GridClient client = Command.startClient(clientCfg)) {
            MessageStatsTaskResult res = executeTask(client, MessageStatsTask.class, arg, clientCfg);

            printReport(arg.statisticsType().toString(), res, logger);
        }

        return null;
    }

    private void printReport(String taskName, MessageStatsTaskResult taskResult, Logger log) {
        if (taskResult.histograms().isEmpty()) {
            log.info("No data for given metrics was found.");

            return;
        }

        String fmt = formatTable(taskResult.bounds().length + 1, false);

        Object[] captionFmtObjects = new Object[taskResult.bounds().length + REPORT_LEADING_COLUMNS.length + 1];

        for (int i = 0; i < REPORT_LEADING_COLUMNS.length; i++)
            captionFmtObjects[i] = REPORT_LEADING_COLUMNS[i];

        for (int i = 0; i < taskResult.bounds().length; i++)
            captionFmtObjects[i + REPORT_LEADING_COLUMNS.length] = "<= " + taskResult.bounds()[i];

        captionFmtObjects[captionFmtObjects.length - 1] = "> " + taskResult.bounds()[taskResult.bounds().length - 1];

        GridStringBuilder report = new GridStringBuilder("Statistics report [" + taskName + "]:\n").
            a(format(formatTable(taskResult.bounds().length + 1, true), captionFmtObjects));

        taskResult.histograms().forEach((metric, values) -> {
            Object[] objects = new Object[values.length + REPORT_LEADING_COLUMNS.length];

            Long totalCount = LongStream.of(values).sum();

            long totalTime = taskResult.monotonicMetric().getOrDefault(metric, 0L);

            objects[0] = metric;
            objects[1] = totalCount;
            objects[2] = totalTime;
            objects[3] = totalTime == 0 ? 0 : totalCount.doubleValue() / totalTime;

            for (int i = 0; i < values.length; i++)
                objects[i + REPORT_LEADING_COLUMNS.length] = values[i];

            report.a(format(fmt, objects));
        });

        log.info(report.toString());
    }

    /** */
    private String formatTable(int histogramSize, boolean caption) {
        GridStringBuilder sb = new GridStringBuilder("%40s");

        for (int i = 1; i < REPORT_LEADING_COLUMNS.length - 1; i++)
            sb.a("%15s");

        if (caption)
            sb.a("%18s");
        else
            sb.a("%15.3f");

        for (int i = 0; i < histogramSize; i++)
            sb.a("%10s");

        sb.a("\n");

        return sb.toString();
    }

    /** {@inheritDoc} */
    @Override public MessageStatsTaskArg arg() {
        return arg;
    }

    /** {@inheritDoc} */
    @Override public void printUsage(Logger logger) {
        Command.usage(logger, "Prints requested node or cluster metrics or statistics.", STATISTICS, STATS_PARAMS.optionsUsage());
    }

    /** {@inheritDoc} */
    @Override public void parseArguments(CommandArgIterator argIterator) {
        Map<StatisticsCommandArg, Object> parsedArgs = parseArgs(
            argIterator,
            StatisticsCommandArg.class,
            STATS_PARAMS
        );

        arg = new MessageStatsTaskArg(
            (UUID) parsedArgs.get(NODE),
            (MessageStatsTaskArg.StatisticsType)parsedArgs.get(STATS)
        );
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return STATISTICS.toCommandName();
    }
}
