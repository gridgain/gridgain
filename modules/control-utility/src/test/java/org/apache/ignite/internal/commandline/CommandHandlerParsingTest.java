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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.UUID;
import java.util.function.Predicate;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.ignite.ShutdownPolicy;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.internal.commandline.baseline.BaselineArguments;
import org.apache.ignite.internal.commandline.cache.CacheCommands;
import org.apache.ignite.internal.commandline.cache.CacheSubcommands;
import org.apache.ignite.internal.commandline.cache.CacheValidateIndexes;
import org.apache.ignite.internal.commandline.cache.FindAndDeleteGarbage;
import org.apache.ignite.internal.commandline.cache.argument.FindAndDeleteGarbageArg;
import org.apache.ignite.internal.processors.cache.verify.RepairAlgorithm;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.visor.tx.VisorTxOperation;
import org.apache.ignite.internal.visor.tx.VisorTxProjection;
import org.apache.ignite.internal.visor.tx.VisorTxSortOrder;
import org.apache.ignite.internal.visor.tx.VisorTxTaskArg;
import org.apache.ignite.spi.tracing.Scope;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.SystemPropertiesRule;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.jetbrains.annotations.Nullable;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_ENABLE_EXPERIMENTAL_COMMAND;
import static org.apache.ignite.internal.QueryMXBeanImpl.EXPECTED_GLOBAL_QRY_ID_FORMAT;
import static org.apache.ignite.internal.commandline.CommandList.CACHE;
import static org.apache.ignite.internal.commandline.CommandList.CLUSTER_CHANGE_ID;
import static org.apache.ignite.internal.commandline.CommandList.CLUSTER_CHANGE_TAG;
import static org.apache.ignite.internal.commandline.CommandList.ROLLING_UPGRADE;
import static org.apache.ignite.internal.commandline.CommandList.SET_STATE;
import static org.apache.ignite.internal.commandline.CommandList.SHUTDOWN_POLICY;
import static org.apache.ignite.internal.commandline.CommandList.WAL;
import static org.apache.ignite.internal.commandline.CommandList.WARM_UP;
import static org.apache.ignite.internal.commandline.CommonArgParser.CMD_VERBOSE;
import static org.apache.ignite.internal.commandline.TaskExecutor.DFLT_HOST;
import static org.apache.ignite.internal.commandline.TaskExecutor.DFLT_PORT;
import static org.apache.ignite.internal.commandline.WalCommands.WAL_DELETE;
import static org.apache.ignite.internal.commandline.WalCommands.WAL_PRINT;
import static org.apache.ignite.internal.commandline.cache.CacheSubcommands.FIND_AND_DELETE_GARBAGE;
import static org.apache.ignite.internal.commandline.cache.CacheSubcommands.VALIDATE_INDEXES;
import static org.apache.ignite.internal.commandline.cache.PartitionReconciliation.PARALLELISM_FORMAT_MESSAGE;
import static org.apache.ignite.internal.commandline.cache.argument.ValidateIndexesCommandArg.CHECK_FIRST;
import static org.apache.ignite.internal.commandline.cache.argument.ValidateIndexesCommandArg.CHECK_THROUGH;
import static org.apache.ignite.testframework.GridTestUtils.assertThrows;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests Command Handler parsing arguments.
 */
@WithSystemProperty(key = IGNITE_ENABLE_EXPERIMENTAL_COMMAND, value = "true")
public class CommandHandlerParsingTest {
    /** */
    @ClassRule public static final TestRule classRule = new SystemPropertiesRule();

    /** */
    private static final String INVALID_REGEX = "[]";

    /** */
    @Rule public final TestRule methodRule = new SystemPropertiesRule();

    /**
     * validate_indexes command arguments parsing and validation
     */
    @Test
    public void testValidateIndexArguments() {
        //happy case for all parameters
        try {
            int expectedCheckFirst = 10;
            int expectedCheckThrough = 11;
            UUID nodeId = UUID.randomUUID();

            ConnectionAndSslParameters args = parseArgs(asList(
                CACHE.text(),
                VALIDATE_INDEXES.text(),
                "cache1, cache2",
                nodeId.toString(),
                CHECK_FIRST.toString(),
                Integer.toString(expectedCheckFirst),
                CHECK_THROUGH.toString(),
                Integer.toString(expectedCheckThrough)
            ));

            assertTrue(args.command() instanceof CacheCommands);

            CacheSubcommands subcommand = ((CacheCommands)args.command()).arg();

            CacheValidateIndexes.Arguments arg = (CacheValidateIndexes.Arguments)subcommand.subcommand().arg();

            assertEquals("nodeId parameter unexpected value", nodeId, arg.nodeId());
            assertEquals("checkFirst parameter unexpected value", expectedCheckFirst, arg.checkFirst());
            assertEquals("checkThrough parameter unexpected value", expectedCheckThrough, arg.checkThrough());
        }
        catch (IllegalArgumentException e) {
            fail("Unexpected exception: " + e);
        }

        try {
            int expectedParam = 11;
            UUID nodeId = UUID.randomUUID();

            ConnectionAndSslParameters args = parseArgs(asList(
                    CACHE.text(),
                    VALIDATE_INDEXES.text(),
                    nodeId.toString(),
                    CHECK_THROUGH.toString(),
                    Integer.toString(expectedParam)
                ));

            assertTrue(args.command() instanceof CacheCommands);

            CacheSubcommands subcommand = ((CacheCommands)args.command()).arg();

            CacheValidateIndexes.Arguments arg = (CacheValidateIndexes.Arguments)subcommand.subcommand().arg();

            assertNull("caches weren't specified, null value expected", arg.caches());
            assertEquals("nodeId parameter unexpected value", nodeId, arg.nodeId());
            assertEquals("checkFirst parameter unexpected value", -1, arg.checkFirst());
            assertEquals("checkThrough parameter unexpected value", expectedParam, arg.checkThrough());
        }
        catch (IllegalArgumentException e) {
            e.printStackTrace();
        }

        assertParseArgsThrows("Value for '--check-first' property should be positive.", CACHE.text(), VALIDATE_INDEXES.text(), CHECK_FIRST.toString(), "0");
        assertParseArgsThrows("Numeric value for '--check-through' parameter expected.", CACHE.text(), VALIDATE_INDEXES.text(), CHECK_THROUGH.toString());
    }

    /** */
    @Test
    public void testFindAndDeleteGarbage() {
        String nodeId = UUID.randomUUID().toString();
        String delete = FindAndDeleteGarbageArg.DELETE.toString();
        String groups = "group1,grpoup2,group3";

        List<List<String>> lists = generateArgumentList(
            FIND_AND_DELETE_GARBAGE.text(),
            new T2<>(nodeId, false),
            new T2<>(delete, false),
            new T2<>(groups, false)
        );

        for (List<String> list : lists) {
            ConnectionAndSslParameters args = parseArgs(list);

            assertTrue(args.command() instanceof CacheCommands);

            CacheSubcommands subcommand = ((CacheCommands)args.command()).arg();

            FindAndDeleteGarbage.Arguments arg = (FindAndDeleteGarbage.Arguments)subcommand.subcommand().arg();

            if (list.contains(nodeId))
                assertEquals("nodeId parameter unexpected value", nodeId, arg.nodeId().toString());
            else
                assertNull(arg.nodeId());

            assertEquals(list.contains(delete), arg.delete());

            if (list.contains(groups))
                assertEquals(3, arg.groups().size());
            else
                assertNull(arg.groups());
        }
    }

    /** */
    private List<List<String>> generateArgumentList(String subcommand, T2<String, Boolean>...optional) {
        List<List<T2<String, Boolean>>> lists = generateAllCombinations(asList(optional), (x) -> x.get2());

        ArrayList<List<String>> res = new ArrayList<>();

        ArrayList<String> empty = new ArrayList<>();

        empty.add(CACHE.text());
        empty.add(subcommand);

        res.add(empty);

        for (List<T2<String, Boolean>> list : lists) {
            ArrayList<String> arg = new ArrayList<>(empty);

            list.forEach(x -> arg.add(x.get1()));

            res.add(arg);
        }

        return res;
    }

    /** */
    private <T> List<List<T>> generateAllCombinations(List<T> source, Predicate<T> stopFunc) {
        List<List<T>> res = new ArrayList<>();

        for (int i = 0; i < source.size(); i++) {
            List<T> sourceCopy = new ArrayList<>(source);

            T removed = sourceCopy.remove(i);

            generateAllCombinations(singletonList(removed), sourceCopy, stopFunc, res);
        }

        return res;
    }

    /** */
    private <T> void generateAllCombinations(List<T> res, List<T> source, Predicate<T> stopFunc, List<List<T>> acc) {
        acc.add(res);

        if (stopFunc != null && stopFunc.test(res.get(res.size() - 1)))
            return;

        if (source.size() == 1) {
            ArrayList<T> list = new ArrayList<>(res);

            list.add(source.get(0));

            acc.add(list);

            return;
        }

        for (int i = 0; i < source.size(); i++) {
            ArrayList<T> res0 = new ArrayList<>(res);

            List<T> sourceCopy = new ArrayList<>(source);

            T removed = sourceCopy.remove(i);

            res0.add(removed);

            generateAllCombinations(res0, sourceCopy, stopFunc, acc);
        }
    }

    /**
     * Tests parsing and validation for the SSL arguments.
     */
    @Test
    public void testParseAndValidateSSLArguments() {
        for (CommandList cmd : CommandList.values()) {
            if (requireArgs(cmd))
                continue;

            assertParseArgsThrows("Expected SSL trust store path", "--truststore");

            ConnectionAndSslParameters args = parseArgs(asList("--keystore", "testKeystore", "--keystore-password", "testKeystorePassword", "--keystore-type", "testKeystoreType",
                "--truststore", "testTruststore", "--truststore-password", "testTruststorePassword", "--truststore-type", "testTruststoreType",
                "--ssl-key-algorithm", "testSSLKeyAlgorithm", "--ssl-protocol", "testSSLProtocol", cmd.text()));

            assertEquals("testSSLProtocol", args.sslProtocol());
            assertEquals("testSSLKeyAlgorithm", args.sslKeyAlgorithm());
            assertEquals("testKeystore", args.sslKeyStorePath());
            assertArrayEquals("testKeystorePassword".toCharArray(), args.sslKeyStorePassword());
            assertEquals("testKeystoreType", args.sslKeyStoreType());
            assertEquals("testTruststore", args.sslTrustStorePath());
            assertArrayEquals("testTruststorePassword".toCharArray(), args.sslTrustStorePassword());
            assertEquals("testTruststoreType", args.sslTrustStoreType());

            assertEquals(cmd.command(), args.command());
        }
    }

    /**
     * Tests parsing and validation for user and password arguments.
     */
    @Test
    public void testParseAndValidateUserAndPassword() {
        for (CommandList cmd : CommandList.values()) {
            if (requireArgs(cmd))
                continue;

            assertParseArgsThrows("Expected user name", "--user");
            assertParseArgsThrows("Expected password", "--password");

            ConnectionAndSslParameters args = parseArgs(asList("--user", "testUser", "--password", "testPass", cmd.text()));

            assertEquals("testUser", args.userName());
            assertEquals("testPass", args.password());
            assertEquals(cmd.command(), args.command());
        }
    }

    /**
     * Tests parsing and validation  of WAL commands.
     */
    @Test
    public void testParseAndValidateWalActions() {
        ConnectionAndSslParameters args = parseArgs(asList(WAL.text(), WAL_PRINT));

        assertEquals(WAL.command(), args.command());

        T2<String, String> arg = ((WalCommands)args.command()).arg();

        assertEquals(WAL_PRINT, arg.get1());

        String nodes = UUID.randomUUID().toString() + "," + UUID.randomUUID().toString();

        args = parseArgs(asList(WAL.text(), WAL_DELETE, nodes));

        arg = ((WalCommands)args.command()).arg();

        assertEquals(WAL_DELETE, arg.get1());

        assertEquals(nodes, arg.get2());

        assertParseArgsThrows("Expected arguments for " + WAL.text(), WAL.text());

        String rnd = UUID.randomUUID().toString();

        assertParseArgsThrows("Unexpected action " + rnd + " for " + WAL.text(), WAL.text(), rnd);
    }

    /**
     * Tets checks a parser of shutdown policy command.
     */
    @Test
    public void testParseShutdownPolicyParameters() {
        ConnectionAndSslParameters args = parseArgs(asList(SHUTDOWN_POLICY.text()));

        assertEquals(SHUTDOWN_POLICY.command(), args.command());

        assertNull(((ShutdownPolicyCommand)args.command()).arg().getShutdown());

        for (ShutdownPolicy policy : ShutdownPolicy.values()) {
            args = parseArgs(asList(SHUTDOWN_POLICY.text(), String.valueOf(policy)));

            assertEquals(SHUTDOWN_POLICY.command(), args.command());

            assertSame(policy, ((ShutdownPolicyCommand)args.command()).arg().getShutdown());
        }
    }

    /**
     * Tests that the auto confirmation flag was correctly parsed.
     */
    @Test
    public void testParseAutoConfirmationFlag() {
        for (CommandList cmdL : CommandList.values()) {
            // SET_STATE command have mandatory argument, which used in confirmation message.
            Command cmd;

            if (cmdL == SET_STATE)
                cmd = parseArgs(asList(cmdL.text(), "ACTIVE")).command();
            else if (cmdL == ROLLING_UPGRADE)
                cmd = parseArgs(asList(cmdL.text(), "start")).command();
            else
                cmd = cmdL.command();

            if (cmd.confirmationPrompt() == null)
                continue;

            ConnectionAndSslParameters args;

            UUID uuid = UUID.fromString("11111111-1111-1111-1111-111111111111");
            if (cmdL == CLUSTER_CHANGE_TAG)
                args = parseArgs(asList(cmdL.text(), "test_tag"));
            else if (cmdL == CLUSTER_CHANGE_ID)
                args = parseArgs(asList(cmdL.text(), uuid.toString()));
            else if (cmdL == SET_STATE)
                args = parseArgs(asList(cmdL.text(), "ACTIVE"));
            else if (cmdL == ROLLING_UPGRADE)
                args = parseArgs(asList(cmdL.text(), "start"));
            else if (cmdL == WARM_UP)
                args = parseArgs(asList(cmdL.text(), "--stop"));
            else
                args = parseArgs(asList(cmdL.text()));

            checkCommonParametersCorrectlyParsed(cmdL, args, false);

            switch (cmdL) {
                case ROLLING_UPGRADE: {
                    args = parseArgs(asList(cmdL.text(), "start", "--yes"));

                    checkCommonParametersCorrectlyParsed(cmdL, args, true);

                    break;
                }

                case DEACTIVATE: {
                    args = parseArgs(asList(cmdL.text(), "--yes"));

                    checkCommonParametersCorrectlyParsed(cmdL, args, true);

                    break;
                }
                case SET_STATE: {
                    for (String newState : asList("ACTIVE_READ_ONLY", "ACTIVE", "INACTIVE")) {
                        args = parseArgs(asList(cmdL.text(), newState, "--yes"));

                        checkCommonParametersCorrectlyParsed(cmdL, args, true);

                        ClusterState argState = ((ClusterStateChangeCommand)args.command()).arg();

                        assertEquals(newState, argState.toString());
                    }

                    break;
                }
                case BASELINE: {
                    for (String baselineAct : asList("add", "remove", "set")) {
                        args = parseArgs(asList(cmdL.text(), baselineAct, "c_id1,c_id2", "--yes"));

                        checkCommonParametersCorrectlyParsed(cmdL, args, true);

                        BaselineArguments arg = ((BaselineCommand)args.command()).arg();

                        assertEquals(baselineAct, arg.getCmd().text());
                        assertEquals(new HashSet<>(asList("c_id1","c_id2")), new HashSet<>(arg.getConsistentIds()));
                    }

                    break;
                }

                case TX: {
                    args = parseArgs(asList(cmdL.text(), "--xid", "xid1", "--min-duration", "10", "--kill", "--yes"));

                    checkCommonParametersCorrectlyParsed(cmdL, args, true);

                    VisorTxTaskArg txTaskArg = ((TxCommands)args.command()).arg();

                    assertEquals("xid1", txTaskArg.getXid());
                    assertEquals(10_000, txTaskArg.getMinDuration().longValue());
                    assertEquals(VisorTxOperation.KILL, txTaskArg.getOperation());

                    break;
                }

                case CLUSTER_CHANGE_ID: {
                    args = parseArgs(asList(cmdL.text(), uuid.toString(), "--yes"));

                    checkCommonParametersCorrectlyParsed(cmdL, args, true);

                    assertEquals(uuid, ((ClusterChangeIdCommand)args.command()).arg());

                    break;
                }

                case CLUSTER_CHANGE_TAG: {
                    args = parseArgs(asList(cmdL.text(), "test_tag", "--yes"));

                    checkCommonParametersCorrectlyParsed(cmdL, args, true);

                    assertEquals("test_tag", ((ClusterChangeTagCommand)args.command()).arg());

                    break;
                }

                case WARM_UP: {
                    args = parseArgs(asList(cmdL.text(), "--stop", "--yes"));

                    checkCommonParametersCorrectlyParsed(cmdL, args, true);

                    break;
                }

                default:
                    fail("Unknown command: " + cmdL);
            }
        }
    }

    /** */
    private void checkCommonParametersCorrectlyParsed(
        CommandList cmd,
        ConnectionAndSslParameters args,
        boolean autoConfirm
    ) {
        assertEquals(cmd.command(), args.command());
        assertEquals(DFLT_HOST, args.host());
        assertEquals(DFLT_PORT, args.port());
        assertEquals(autoConfirm, args.autoConfirmation());
    }

    /**
     * Tests host and port arguments.
     * Tests connection settings arguments.
     */
    @Test
    public void testConnectionSettings() {
        for (CommandList cmd : CommandList.values()) {
            if (requireArgs(cmd))
                continue;

            ConnectionAndSslParameters args = parseArgs(asList(cmd.text()));

            assertEquals(cmd.command(), args.command());
            assertEquals(DFLT_HOST, args.host());
            assertEquals(DFLT_PORT, args.port());

            args = parseArgs(asList("--port", "12345", "--host", "test-host", "--ping-interval", "5000",
                "--ping-timeout", "40000", "--connection-timeout", "1000", cmd.text()));

            assertEquals(cmd.command(), args.command());
            assertEquals("test-host", args.host());
            assertEquals("12345", args.port());
            assertEquals(5000, args.pingInterval());
            assertEquals(40000, args.pingTimeout());
            assertEquals(1_000, args.connectionTimeout());

            assertParseArgsThrows("Invalid value for port: wrong-port", "--port", "wrong-port", cmd.text());
            assertParseArgsThrows("Invalid value for ping interval: -10", "--ping-interval", "-10", cmd.text());
            assertParseArgsThrows("Invalid value for ping timeout: -20", "--ping-timeout", "-20", cmd.text());
            assertParseArgsThrows("Invalid value for connection timeout: -30", "--connection-timeout",
                "-30", cmd.text());
        }
    }

    /**
     * Test parsing dump transaction arguments
     */
    @Test
    public void testTransactionArguments() {
        ConnectionAndSslParameters args;

        parseArgs(asList("--tx"));

        assertParseArgsThrows("Expecting --min-duration", "--tx", "--min-duration");
        assertParseArgsThrows("Invalid value for --min-duration: -1", "--tx", "--min-duration", "-1");
        assertParseArgsThrows("Expecting --min-size", "--tx", "--min-size");
        assertParseArgsThrows("Invalid value for --min-size: -1", "--tx", "--min-size", "-1");
        assertParseArgsThrows("--label", "--tx", "--label");
        assertParseArgsThrows("Illegal regex syntax", "--tx", "--label", "tx123[");
        assertParseArgsThrows("Projection can't be used together with list of consistent ids.", "--tx", "--servers", "--nodes", "1,2,3");

        args = parseArgs(asList("--tx", "--min-duration", "120", "--min-size", "10", "--limit", "100", "--order", "SIZE", "--servers"));

        VisorTxTaskArg arg = ((TxCommands)args.command()).arg();

        assertEquals(Long.valueOf(120 * 1000L), arg.getMinDuration());
        assertEquals(Integer.valueOf(10), arg.getMinSize());
        assertEquals(Integer.valueOf(100), arg.getLimit());
        assertEquals(VisorTxSortOrder.SIZE, arg.getSortOrder());
        assertEquals(VisorTxProjection.SERVER, arg.getProjection());

        args = parseArgs(asList("--tx", "--min-duration", "130", "--min-size", "1", "--limit", "60", "--order", "DURATION",
            "--clients"));

        arg = ((TxCommands)args.command()).arg();

        assertEquals(Long.valueOf(130 * 1000L), arg.getMinDuration());
        assertEquals(Integer.valueOf(1), arg.getMinSize());
        assertEquals(Integer.valueOf(60), arg.getLimit());
        assertEquals(VisorTxSortOrder.DURATION, arg.getSortOrder());
        assertEquals(VisorTxProjection.CLIENT, arg.getProjection());

        args = parseArgs(asList("--tx", "--nodes", "1,2,3"));

        arg = ((TxCommands)args.command()).arg();

        assertNull(arg.getProjection());
        assertEquals(asList("1", "2", "3"), arg.getConsistentIds());
    }

    /**
     * Test parsing kill arguments.
     */
    @Test
    public void testKillArguments() {
        assertParseArgsThrows("Expected type of resource to kill.", "--kill");

        String uuid = UUID.randomUUID().toString();

        // SQL command format errors.
        assertParseArgsThrows("Expected SQL query id.", "--kill", "sql");

        assertParseArgsThrows("Expected global query id. " + EXPECTED_GLOBAL_QRY_ID_FORMAT,
            "--kill", "sql", "not_sql_id");

        // Continuous command format errors.
        assertParseArgsThrows("Expected query originating node id.", "--kill", "continuous");

        assertParseArgsThrows("Expected continuous query id.", "--kill", "continuous", UUID.randomUUID().toString());

        assertParseArgsThrows("Invalid UUID string: not_a_uuid", IllegalArgumentException.class,
            "--kill", "continuous", "not_a_uuid");

        assertParseArgsThrows("Invalid UUID string: not_a_uuid", IllegalArgumentException.class,
            "--kill", "continuous", UUID.randomUUID().toString(), "not_a_uuid");

        // Scan command format errors.
        assertParseArgsThrows("Expected query originating node id.", "--kill", "scan");
        assertParseArgsThrows("Expected cache name.", "--kill", "scan", uuid);
        assertParseArgsThrows("Expected query identifier.", "--kill", "scan", uuid, "cache");

        assertParseArgsThrows("Invalid UUID string: not_a_uuid", IllegalArgumentException.class,
            "--kill", "scan", "not_a_uuid");

        assertParseArgsThrows("For input string: \"not_a_number\"", NumberFormatException.class,
            "--kill", "scan", uuid, "my-cache", "not_a_number");

        // Compute command format errors.
        assertParseArgsThrows("Expected compute task id.", "--kill", "compute");

        assertParseArgsThrows("Invalid UUID string: not_a_uuid", IllegalArgumentException.class,
            "--kill", "compute", "not_a_uuid");

        // Service command format errors.
        assertParseArgsThrows("Expected service name.", "--kill", "service");

        // Transaction command format errors.
        assertParseArgsThrows("Expected transaction id.", "--kill", "transaction");
    }

    /**  */
    @Test
    public void testValidateIndexesNotAllowedForSystemCache() {
        GridTestUtils.assertThrows(
            null,
            () -> parseArgs(asList("--cache", "validate_indexes", "cache1,ignite-sys-cache")),
            IllegalArgumentException.class,
            "validate_indexes not allowed for 'ignite-sys-cache' cache."
        );
    }

    /** */
    @Test
    public void testIdleVerifyWithCheckCrcNotAllowedForSystemCache() {
        GridTestUtils.assertThrows(
            null,
            () -> parseArgs(asList("--cache", "idle_verify", "--check-crc", "--cache-filter", "ALL")),
            IllegalArgumentException.class,
            "idle_verify with --check-crc and --cache-filter ALL or SYSTEM not allowed. You should remove --check-crc or change --cache-filter value."
        );

        GridTestUtils.assertThrows(
            null,
            () -> parseArgs(asList("--cache", "idle_verify", "--check-crc", "--cache-filter", "SYSTEM")),
            IllegalArgumentException.class,
            "idle_verify with --check-crc and --cache-filter ALL or SYSTEM not allowed. You should remove --check-crc or change --cache-filter value."
        );

        GridTestUtils.assertThrows(
            null,
            () -> parseArgs(asList("--cache", "idle_verify", "--check-crc", "ignite-sys-cache")),
            IllegalArgumentException.class,
            "idle_verify with --check-crc not allowed for `ignite-sys-cache` cache."
        );
    }

    /** */
    @Test
    public void testIndexForceRebuildWrongArgs() {
        String nodeId = UUID.randomUUID().toString();

        GridTestUtils.assertThrows(
            null,
            () -> parseArgs(asList("--cache", "indexes_force_rebuild", "--node-id")),
            IllegalArgumentException.class,
            "Failed to read node id."
        );

        GridTestUtils.assertThrows(
            null,
            () -> parseArgs(asList("--cache", "indexes_force_rebuild", "--node-id", nodeId, "--cache-names")),
            IllegalArgumentException.class,
            "Expected comma-separated list of cache names."
        );

        GridTestUtils.assertThrows(
            null,
            () -> parseArgs(asList("--cache", "indexes_force_rebuild", "--node-id", nodeId, "--group-names")),
            IllegalArgumentException.class,
            "Expected comma-separated list of cache group names."
        );

        GridTestUtils.assertThrows(
            null,
            () -> parseArgs(asList("--cache", "indexes_force_rebuild", "--node-id", nodeId, "--group-names", "someNames", "--cache-names", "someNames")),
            IllegalArgumentException.class,
            "Either --group-names or --cache-names must be specified."
        );

        GridTestUtils.assertThrows(
            null,
            () -> parseArgs(asList("--cache", "indexes_force_rebuild", "--node-id", nodeId, "--cache-names", "someNames", "--cache-names", "someMoreNames")),
            IllegalArgumentException.class,
            "--cache-names arg specified twice."
        );

        GridTestUtils.assertThrows(
            null,
            () -> parseArgs(asList("--cache", "indexes_force_rebuild", "--node-id", nodeId, "--group-names", "someNames", "--group-names", "someMoreNames")),
            IllegalArgumentException.class,
            "--group-names arg specified twice."
        );

        GridTestUtils.assertThrows(
            null,
            () -> parseArgs(asList("--cache", "indexes_force_rebuild", "--node-id", nodeId, "--group-names", "--some-other-arg")),
            IllegalArgumentException.class,
            "--group-names not specified."
        );

        GridTestUtils.assertThrows(
            null,
            () -> parseArgs(asList("--cache", "indexes_force_rebuild", "--node-id", nodeId, "--cache-names", "--some-other-arg")),
            IllegalArgumentException.class,
            "--cache-names not specified."
        );
    }

    /** */
    @Test
    public void testIndexListWrongArgs() {
        String nodeId = UUID.randomUUID().toString();

        GridTestUtils.assertThrows(
            null,
            () -> parseArgs(asList("--cache", "indexes_list", "--node-id")),
            IllegalArgumentException.class,
            "Failed to read node id."
        );

        GridTestUtils.assertThrows(
            null,
            () -> parseArgs(asList("--cache", "indexes_list", "--node-id", nodeId, "--group-name")),
            IllegalArgumentException.class,
            "Failed to read group name regex."
        );

        GridTestUtils.assertThrows(
            null,
            () -> parseArgs(asList("--cache", "indexes_list", "--node-id", nodeId, "--group-name", INVALID_REGEX)),
            IllegalArgumentException.class,
            "Invalid group name regex: " + INVALID_REGEX
        );

        GridTestUtils.assertThrows(
            null,
            () -> parseArgs(asList("--cache", "indexes_list", "--node-id", nodeId, "--cache-name")),
            IllegalArgumentException.class,
            "Failed to read cache name regex."
        );

        GridTestUtils.assertThrows(
            null,
            () -> parseArgs(asList("--cache", "indexes_list", "--node-id", nodeId, "--cache-name", INVALID_REGEX)),
            IllegalArgumentException.class,
            "Invalid cache name regex: " + INVALID_REGEX
        );

        GridTestUtils.assertThrows(
            null,
            () -> parseArgs(asList("--cache", "indexes_list", "--node-id", nodeId, "--index-name")),
            IllegalArgumentException.class,
            "Failed to read index name regex."
        );

        GridTestUtils.assertThrows(
            null,
            () -> parseArgs(asList("--cache", "indexes_list", "--node-id", nodeId, "--index-name", INVALID_REGEX)),
            IllegalArgumentException.class,
            "Invalid index name regex: " + INVALID_REGEX
        );
    }

    /** */
    @Test
    public void testIndexRebuildStatusWrongArgs() {
        GridTestUtils.assertThrows(
            null,
            () -> parseArgs(asList("--cache", "indexes_list", "--node-id")),
            IllegalArgumentException.class,
            "Failed to read node id."
        );
    }

    /**
     * Argument validation test.
     *
     * validate that following partition_reconciliation arguments validated as expected:
     *
     * --repair
     *      if value is missing - IllegalArgumentException (The repair algorithm should be specified.
     *      The following values can be used: [LATEST, PRIMARY, MAJORITY, REMOVE, PRINT_ONLY].) is expected.
     *      if unsupported value is used - IllegalArgumentException (Invalid repair algorithm: <invalid-repair-alg>.
     *      The following values can be used: [LATEST, PRIMARY, MAJORITY, REMOVE, PRINT_ONLY].) is expected.
     *
     * --parallelism
     *      Int value from [0, 128] is expected.
     *      If value is missing of differs from mentioned integer -
     *      IllegalArgumentException (Invalid parallelism) is expected.
     *
     * --batch-size
     *      if value is missing - IllegalArgumentException (The batch size should be specified.) is expected.
     *      if unsupported value is used - IllegalArgumentException (Invalid batch size: <invalid-batch-size>.
     *      Int value greater than zero should be used.) is expected.
     *
     * --recheck-attempts
     *      if value is missing - IllegalArgumentException (The recheck attempts should be specified.) is expected.
     *      if unsupported value is used - IllegalArgumentException (Invalid recheck attempts:
     *      <invalid-recheck-attempts>. Int value between 1 and 5 should be used.) is expected.
     *
     * As invalid values use values that produce NumberFormatException and out-of-range values.
     * Also ensure that in case of appropriate parameters parseArgs() doesn't throw any exceptions.
     */
    @Test
    public void testPartitionReconciliationArgumentsValidation() {
        assertParseArgsThrows("The repair algorithm should be specified. The following values can be used: "
            + Arrays.toString(RepairAlgorithm.values()) + '.', "--cache", "partition_reconciliation", "--repair");

        assertParseArgsThrows("Invalid repair algorithm: invalid-repair-alg. The following values can be used: "
            + Arrays.toString(RepairAlgorithm.values()) + '.', "--cache", "partition_reconciliation", "--repair",
            "invalid-repair-alg");

        parseArgs(asList("--cache", "partition_reconciliation", "--fix-alg", "PRIMARY"));

        // --load-factor
        assertParseArgsThrows("The parallelism level should be specified.",
            "--cache", "partition_reconciliation", "--parallelism");

        assertParseArgsThrows(String.format(PARALLELISM_FORMAT_MESSAGE, "abc"),
            "--cache", "partition_reconciliation", "--parallelism", "abc");

        assertParseArgsThrows(String.format(PARALLELISM_FORMAT_MESSAGE, "0.5"),
            "--cache", "partition_reconciliation", "--parallelism", "0.5");

        assertParseArgsThrows(String.format(PARALLELISM_FORMAT_MESSAGE, "-1"),
            "--cache", "partition_reconciliation", "--parallelism", "-1");

        parseArgs(asList("--cache", "partition_reconciliation", "--parallelism", "8"));

        parseArgs(asList("--cache", "partition_reconciliation", "--parallelism", "1"));

        parseArgs(asList("--cache", "partition_reconciliation", "--parallelism", "0"));

        // --batch-size
        assertParseArgsThrows("The batch size should be specified.",
            "--cache", "partition_reconciliation", "--batch-size");

        assertParseArgsThrows("Invalid batch size: abc. Integer value greater than zero should be used.",
            "--cache", "partition_reconciliation", "--batch-size", "abc");

        assertParseArgsThrows("Invalid batch size: 0. Integer value greater than zero should be used.",
            "--cache", "partition_reconciliation", "--batch-size", "0");

        parseArgs(asList("--cache", "partition_reconciliation", "--batch-size", "10"));

        // --recheck-attempts
        assertParseArgsThrows("The recheck attempts should be specified.",
            "--cache", "partition_reconciliation", "--recheck-attempts");

        assertParseArgsThrows("Invalid recheck attempts: abc. Integer value between 1 (inclusive) and 5 (exclusive) should be used.",
            "--cache", "partition_reconciliation", "--recheck-attempts", "abc");

        assertParseArgsThrows("Invalid recheck attempts: 6. Integer value between 1 (inclusive) and 5 (exclusive) should be used.",
            "--cache", "partition_reconciliation", "--recheck-attempts", "6");

        parseArgs(asList("--cache", "partition_reconciliation", "--recheck-attempts", "1"));

        parseArgs(asList("--cache", "partition_reconciliation", "--recheck-attempts", "5"));

        // --recheck-delay
        assertParseArgsThrows("The recheck delay should be specified.",
            "--cache", "partition_reconciliation", "--recheck-delay");

        assertParseArgsThrows("Invalid recheck delay: abc. Integer value between 0 (inclusive) and 100 (exclusive) should be used.",
            "--cache", "partition_reconciliation", "--recheck-delay", "abc");

        assertParseArgsThrows("Invalid recheck delay: 101. Integer value between 0 (inclusive) and 100 (exclusive) should be used.",
            "--cache", "partition_reconciliation", "--recheck-delay", "101");

        parseArgs(asList("--cache", "partition_reconciliation", "--recheck-delay", "0"));

        parseArgs(asList("--cache", "partition_reconciliation", "--recheck-delay", "50"));
    }

    /**
     * Negative argument validation test for tracing-configuration command.
     *
     * validate that following tracing-configuration arguments validated as expected:
     * <ul>
     *     <li>
     *         reset_all, get_all
     *         <ul>
     *             <li>
     *                 --scope
     *                 <ul>
     *                     <li>
     *                         if value is missing:
     *                          IllegalArgumentException (The scope should be specified. The following values can be used: [DISCOVERY, EXCHANGE, COMMUNICATION, TX].")
     *                     </li>
     *                     <li>
     *                         if unsupported value is used:
     *                          IllegalArgumentException (Invalid scope 'aaa'. The following values can be used: [DISCOVERY, EXCHANGE, COMMUNICATION, TX])
     *                     </li>
     *                 </ul>
     *             </li>
     *         </ul>
     *     </li>
     *     <li>
     *         reset, get:
     *         <ul>
     *             <li>
     *                 --scope
     *                 <ul>
     *                     <li>
     *                         if value is missing:
     *                          IllegalArgumentException (The scope should be specified. The following values can be used: [DISCOVERY, EXCHANGE, COMMUNICATION, TX].")
     *                     </li>
     *                     <li>
     *                         if unsupported value is used:
     *                          IllegalArgumentException (Invalid scope 'aaa'. The following values can be used: [DISCOVERY, EXCHANGE, COMMUNICATION, TX])
     *                     </li>
     *                 </ul>
     *             </li>
     *             <li>
     *                 --label
     *                 <ul>
     *                     <li>
     *                         if value is missing:
     *                          IllegalArgumentException (The label should be specified.)
     *                     </li>
     *                 </ul>
     *             </li>
     *         </ul>
     *     </li>
     *     <li>
     *         set:
     *         <ul>
     *             <li>
     *                 --scope
     *                 <ul>
     *                     <li>
     *                         if value is missing:
     *                          IllegalArgumentException (The scope should be specified. The following values can be used: [DISCOVERY, EXCHANGE, COMMUNICATION, TX].")
     *                     </li>
     *                     <li>
     *                         if unsupported value is used:
     *                          IllegalArgumentException (Invalid scope 'aaa'. The following values can be used: [DISCOVERY, EXCHANGE, COMMUNICATION, TX])
     *                     </li>
     *                 </ul>
     *             </li>
     *             <li>
     *                 --label
     *                 <ul>
     *                     <li>
     *                          if value is missing:
     *                              IllegalArgumentException (The label should be specified.)
     *                     </li>
     *                 </ul>
     *             </li>
     *             <li>
     *                 --sampling-rate
     *                 <ul>
     *                     <li>
     *                          if value is missing:
     *                              IllegalArgumentException (The sampling-rate should be specified. Decimal value between 0 and 1 should be used.)
     *                     </li>
     *                     <li>
     *                          if unsupported value is used:
     *                              IllegalArgumentException (Invalid samling-rate 'aaa'. Decimal value between 0 and 1 should be used.)
     *                     </li>
     *                 </ul>
     *             </li>
     *             <li>
     *                 --included-scopes
     *                 <ul>
     *                     <li>
     *                          if value is missing:
     *                              IllegalArgumentException (At least one supported scope should be specified.)
     *                     </li>
     *                     <li>
     *                          if unsupported value is used:
     *                              IllegalArgumentException (Invalid supported scope: aaa. The following values can be used: [DISCOVERY, EXCHANGE, COMMUNICATION, TX].)
     *                     </li>
     *                 </ul>
     *             </li>
     *         </ul>
     *     </li>
     * </ul>
     */
    @Test
    public void testTracingConfigurationArgumentsValidation() {
        // reset
        assertParseArgsThrows("The scope should be specified. The following values can be used: "
            + Arrays.toString(Scope.values()) + '.', "--tracing-configuration", "reset", "--scope");

        assertParseArgsThrows("Invalid scope 'aaa'. The following values can be used: "
            + Arrays.toString(Scope.values()) + '.', "--tracing-configuration", "reset", "--scope", "aaa");

        assertParseArgsThrows("The label should be specified.",
            "--tracing-configuration", "reset", "--label");

        // reset all
        assertParseArgsThrows("The scope should be specified. The following values can be used: "
            + Arrays.toString(Scope.values()) + '.', "--tracing-configuration", "reset_all", "--scope");

        assertParseArgsThrows("Invalid scope 'aaa'. The following values can be used: "
            + Arrays.toString(Scope.values()) + '.', "--tracing-configuration", "reset_all", "--scope", "aaa");

        // get
        assertParseArgsThrows("The scope should be specified. The following values can be used: "
            + Arrays.toString(Scope.values()) + '.', "--tracing-configuration", "get", "--scope");

        assertParseArgsThrows("Invalid scope 'aaa'. The following values can be used: "
            + Arrays.toString(Scope.values()) + '.', "--tracing-configuration", "get", "--scope", "aaa");

        assertParseArgsThrows("The label should be specified.",
            "--tracing-configuration", "get", "--label");

        // get all
        assertParseArgsThrows("The scope should be specified. The following values can be used: "
            + Arrays.toString(Scope.values()) + '.', "--tracing-configuration", "get_all", "--scope");

        assertParseArgsThrows("Invalid scope 'aaa'. The following values can be used: "
            + Arrays.toString(Scope.values()) + '.', "--tracing-configuration", "get_all", "--scope", "aaa");

        // set
        assertParseArgsThrows("The scope should be specified. The following values can be used: "
            + Arrays.toString(Scope.values()) + '.', "--tracing-configuration", "set", "--scope");

        assertParseArgsThrows("Invalid scope 'aaa'. The following values can be used: "
            + Arrays.toString(Scope.values()) + '.', "--tracing-configuration", "set", "--scope", "aaa");

        assertParseArgsThrows("The label should be specified.",
            "--tracing-configuration", "set", "--label");

        assertParseArgsThrows("The sampling rate should be specified. Decimal value between 0 and 1 should be used.",
            "--tracing-configuration", "set", "--sampling-rate");

        assertParseArgsThrows("Invalid sampling-rate 'aaa'. Decimal value between 0 and 1 should be used.",
            "--tracing-configuration", "set", "--sampling-rate", "aaa");

        assertParseArgsThrows("Invalid sampling-rate '-1'. Decimal value between 0 and 1 should be used.",
            "--tracing-configuration", "set", "--sampling-rate", "-1");

        assertParseArgsThrows("Invalid sampling-rate '2'. Decimal value between 0 and 1 should be used.",
            "--tracing-configuration", "set", "--sampling-rate", "2");

        assertParseArgsThrows("At least one supported scope should be specified.",
            "--tracing-configuration", "set", "--included-scopes");

        assertParseArgsThrows("Invalid supported scope 'aaa'. The following values can be used: "
                + Arrays.toString(Scope.values()) + '.', "--tracing-configuration", "set", "--included-scopes", "TX,aaa");
    }

    /**
     * Positive argument validation test for tracing-configuration command.
     */
    @Test
    public void testTracingConfigurationArgumentsValidationMandatoryArgumentSet() {
        parseArgs(asList("--tracing-configuration"));

        parseArgs(asList("--tracing-configuration", "get_all"));

        assertParseArgsThrows("Scope attribute is missing. Following values can be used: "
            + Arrays.toString(Scope.values()) + '.', "--tracing-configuration", "reset");

        assertParseArgsThrows("Scope attribute is missing. Following values can be used: "
            + Arrays.toString(Scope.values()) + '.', "--tracing-configuration", "get");

        assertParseArgsThrows("Scope attribute is missing. Following values can be used: "
            + Arrays.toString(Scope.values()) + '.', "--tracing-configuration", "set");

        parseArgs(asList("--tracing-configuration", "set", "--scope", "DISCOVERY"));

        parseArgs(asList("--tracing-configuration", "set", "--scope", "discovery"));

        parseArgs(asList("--tracing-configuration", "set", "--scope", "Discovery"));

        parseArgs(asList("--tracing-configuration", "get", "--scope", "TX"));

        parseArgs(asList("--tracing-configuration", "get", "--scope", "tx"));

        parseArgs(asList("--tracing-configuration", "get", "--scope", "Tx"));
    }

    /**
     * Test checks that option {@link CommonArgParser#CMD_VERBOSE} is parsed
     * correctly and if it is not present, it takes the default value
     * {@code false}.
     */
    @Test
    public void testParseVerboseOption() {
        for (CommandList cmd : CommandList.values()) {
            if (requireArgs(cmd))
                continue;

            assertFalse(cmd.toString(), parseArgs(singletonList(cmd.text())).verbose());
            assertTrue(cmd.toString(), parseArgs(asList(cmd.text(), CMD_VERBOSE)).verbose());
        }
    }

    /**
     * Test verifies correctness of parsing of arguments --warm-up command.
     */
    @Test
    public void testWarmUpArgs() {
        String[][] args = {
            {"--warm-up"},
            {"--warm-up", "1"},
            {"--warm-up", "stop"}
        };

        for (String[] arg : args) {
            GridTestUtils.assertThrows(
                null,
                () -> parseArgs(asList(arg)),
                IllegalArgumentException.class,
                "--stop argument is missing."
            );
        }

        assertNotNull(parseArgs(asList("--warm-up", "--stop")));
    }

    /**
     * @param args Raw arg list.
     * @return Common parameters container object.
     */
    private ConnectionAndSslParameters parseArgs(List<String> args) {
        return new CommonArgParser(setupTestLogger()).
            parseAndValidate(args.iterator());
    }

    /**
     * @return logger for tests.
     */
    private Logger setupTestLogger() {
        Logger result;

        result = Logger.getLogger(getClass().getName());
        result.setLevel(Level.INFO);
        result.setUseParentHandlers(false);

        result.addHandler(CommandHandler.setupStreamHandler());

        return result;
    }

    /**
     * Checks that parse arguments fails with {@link IllegalArgumentException} and {@code failMsg} message.
     *
     * @param failMsg Exception message (optional).
     * @param args Incoming arguments.
     */
    private void assertParseArgsThrows(@Nullable String failMsg, String... args) {
        assertParseArgsThrows(failMsg, IllegalArgumentException.class, args);
    }

    /**
     * Checks that parse arguments fails with {@code exception} and {@code failMsg} message.
     *
     * @param failMsg Exception message (optional).
     * @param cls Exception class.
     * @param args Incoming arguments.
     */
    private void assertParseArgsThrows(@Nullable String failMsg, Class<? extends Exception> cls, String... args) {
        assertThrows(null, () -> parseArgs(asList(args)), cls, failMsg);
    }

    /**
     * Return {@code True} if cmd there are required arguments.
     *
     * @return {@code True} if cmd there are required arguments.
     */
    private boolean requireArgs(@Nullable CommandList cmd) {
        return cmd == CommandList.CACHE ||
            cmd == CommandList.WAL ||
            cmd == CommandList.ROLLING_UPGRADE ||
            cmd == CommandList.CLUSTER_CHANGE_TAG ||
            cmd == CommandList.CLUSTER_CHANGE_ID ||
            cmd == CommandList.DATA_CENTER_REPLICATION ||
            cmd == CommandList.SET_STATE ||
            cmd == CommandList.ENCRYPTION ||
            cmd == CommandList.METADATA ||
            cmd == CommandList.WARM_UP ||
            cmd == CommandList.PROPERTY ||
            cmd == CommandList.METRIC ||
            cmd == CommandList.DEFRAGMENTATION ||
            cmd == CommandList.KILL;
    }
}
