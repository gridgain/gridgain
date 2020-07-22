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
package org.apache.ignite.util;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.commandline.CommandArgIterator;
import org.apache.ignite.internal.commandline.StatisticsCommandArg;
import org.apache.ignite.internal.commandline.argument.CommandArg;
import org.apache.ignite.internal.commandline.argument.CommandParameter;
import org.apache.ignite.internal.commandline.argument.CommandParameterConfig;
import org.apache.ignite.internal.commandline.cache.argument.IdleVerifyCommandArg;
import org.apache.ignite.internal.visor.statistics.MessageStatsTaskArg;
import org.junit.Test;

import static java.util.Arrays.asList;
import static org.apache.ignite.internal.commandline.StatisticsCommandArg.NODE;
import static org.apache.ignite.internal.commandline.StatisticsCommandArg.STATS;
import static org.apache.ignite.internal.commandline.argument.CommandArgUtils.parseArgs;
import static org.apache.ignite.internal.commandline.cache.argument.IdleVerifyCommandArg.CACHE_FILTER;
import static org.apache.ignite.internal.commandline.cache.argument.IdleVerifyCommandArg.CHECK_CRC;
import static org.apache.ignite.internal.commandline.cache.argument.IdleVerifyCommandArg.DUMP;
import static org.apache.ignite.internal.commandline.cache.argument.IdleVerifyCommandArg.EXCLUDE_CACHES;
import static org.apache.ignite.internal.commandline.cache.argument.IdleVerifyCommandArg.SKIP_ZEROS;
import static org.apache.ignite.internal.visor.statistics.MessageStatsTaskArg.StatisticsType.PROCESSING;
import static org.apache.ignite.testframework.GridTestUtils.assertThrows;
import static org.junit.Assert.assertEquals;

/**
 *
 */
public class CommandHandlerParsedArgsTest {
    /** */
    private static final Map<StatisticsCommandArg, Object> statsExpectedMap;

    /** */
    private static final Map<IdleVerifyCommandArg, Object> idleVerifyExpectedMap;

    /** */
    private static final CommandParameterConfig<StatisticsCommandArg> statsCommandParamCfg;

    /** */
    private static final CommandParameterConfig<IdleVerifyCommandArg> idleVerifyCommandParamCfg;

    static {
        statsExpectedMap = new HashMap<>();

        statsExpectedMap.put(NODE, UUID.fromString("b63321ab-c8ec-4826-a586-af286166450d"));
        statsExpectedMap.put(STATS, PROCESSING);

        idleVerifyExpectedMap = new HashMap<>();

        Set<String> excludeCaches = new HashSet<>();

        excludeCaches.add("asd");
        excludeCaches.add("zxc");

        idleVerifyExpectedMap.put(DUMP, null);
        idleVerifyExpectedMap.put(SKIP_ZEROS, null);
        idleVerifyExpectedMap.put(EXCLUDE_CACHES, excludeCaches);
        idleVerifyExpectedMap.put(CHECK_CRC, null);
        idleVerifyExpectedMap.put(CACHE_FILTER, "rtybvc");

        statsCommandParamCfg = new CommandParameterConfig<StatisticsCommandArg>(
            new CommandParameter(NODE, UUID.class, true),
            new CommandParameter(STATS, MessageStatsTaskArg.StatisticsType.class)
        );

        idleVerifyCommandParamCfg = new CommandParameterConfig<IdleVerifyCommandArg>(
            new CommandParameter(DUMP, true),
            new CommandParameter(SKIP_ZEROS, true),
            new CommandParameter(EXCLUDE_CACHES, Set.class, true),
            new CommandParameter(CHECK_CRC, true),
            new CommandParameter(CACHE_FILTER, String.class, true)
        );
    }

    /** */
    @Test
    public void testParseStats() {
        test(
            statsCommandParamCfg,
            StatisticsCommandArg.class,
            statsExpectedMap,
            NODE.toString(), "b63321ab-c8ec-4826-a586-af286166450d", STATS.toString(), PROCESSING.toString()
        );
    }

    /** */
    @Test
    public void testParseStatsMissingArg() {
        assertThrows(
            null,
            () -> test(
                statsCommandParamCfg,
                StatisticsCommandArg.class,
                statsExpectedMap,
                NODE.toString(), "b63321ab-c8ec-4826-a586-af286166450d"
            ),
            IgniteException.class,
            null
        );
    }

    /** */
    @Test
    public void testParseStatsUnknownArg() {
        assertThrows(
            null,
            () -> test(
                statsCommandParamCfg,
                StatisticsCommandArg.class,
                statsExpectedMap,
                NODE.toString(), "b63321ab-c8ec-4826-a586-af286166450d", STATS.toString(), PROCESSING.toString(), "--unknown"
            ),
            IgniteException.class,
            null
        );
    }

    /** */
    @Test
    public void testParseIdleVerify() {
        test(
            idleVerifyCommandParamCfg,
            IdleVerifyCommandArg.class,
            idleVerifyExpectedMap,
            CACHE_FILTER.toString(), "rtybvc", DUMP.toString(), EXCLUDE_CACHES.toString(), "asd,zxc", CHECK_CRC.toString(), SKIP_ZEROS.toString()
        );
    }

    /** */
    @Test
    public void testParseIdleVerifyWithoutOptionalParams() {
        test(
            idleVerifyCommandParamCfg,
            IdleVerifyCommandArg.class,
            new HashMap<>()
        );
    }

    /**
     * @param params Parameters configuration.
     * @param argCls Argument class.
     * @param expectedMap Expected parse result.
     * @param args Arguments array.
     * @param <E> Argument enum.
     */
    private <E extends Enum<E> & CommandArg> void test(
        CommandParameterConfig<E> params,
        Class<E> argCls,
        Map<E, Object> expectedMap,
        String... args
    ) {
        CommandArgIterator iter = new CommandArgIterator(asList(args).iterator(), Collections.emptySet());

        Map<E, Object> parseResult = parseArgs(iter, argCls, params);

        assertEquals(expectedMap.size(), parseResult.size());

        expectedMap.forEach((k, v) -> assertEquals(v, parseResult.get(k)));
    }
}
