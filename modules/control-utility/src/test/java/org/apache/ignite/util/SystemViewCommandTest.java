/*
 * Copyright 2026 GridGain Systems, Inc. and Contributors.
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.commandline.CommandList;
import org.apache.ignite.internal.commandline.systemview.SystemViewCommandArg;
import org.apache.ignite.internal.processors.service.DummyService;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.services.ServiceConfiguration;
import org.apache.ignite.spi.systemview.view.SystemView;
import org.apache.ignite.spi.systemview.view.SystemViewRowAttributeWalker.AttributeVisitor;
import org.junit.Test;

import static java.util.Arrays.asList;
import static java.util.regex.Pattern.quote;
import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_INVALID_ARGUMENTS;
import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_OK;
import static org.apache.ignite.internal.commandline.CommandList.SYSTEM_VIEW;
import static org.apache.ignite.internal.commandline.systemview.SystemViewCommand.COLUMN_SEPARATOR;
import static org.apache.ignite.internal.commandline.systemview.SystemViewCommandArg.ALL_NODES;
import static org.apache.ignite.internal.commandline.systemview.SystemViewCommandArg.NODE_ID;
import static org.apache.ignite.internal.commandline.systemview.SystemViewCommandArg.NODE_IDS;
import static org.apache.ignite.internal.managers.discovery.GridDiscoveryManager.NODES_SYS_VIEW;
import static org.apache.ignite.internal.processors.cache.ClusterCachesInfo.CACHES_VIEW;
import static org.apache.ignite.internal.processors.cache.ClusterCachesInfo.CACHE_GRPS_VIEW;
import static org.apache.ignite.internal.processors.cache.GridCacheProcessor.CACHE_GRP_PAGE_LIST_VIEW;
import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.toSqlName;
import static org.apache.ignite.internal.processors.service.IgniteServiceProcessor.SVCS_VIEW;
import static org.apache.ignite.testframework.GridTestUtils.assertContains;

/** Tests output of {@link CommandList#SYSTEM_VIEW} command. */
public class SystemViewCommandTest extends GridCommandHandlerClusterByClassAbstractTest {
    /** Command line argument for printing content of a system view. */
    private static final String CMD_SYS_VIEW = SYSTEM_VIEW.text();

    /** Test node with 0 index. */
    private IgniteEx ignite0;

    /** Test node with 1 index. */
    private IgniteEx ignite1;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        injectTestSystemOut();

        autoConfirmation = false;

        ignite0 = ignite(0);
        ignite1 = ignite(1);
    }

    /** Tests command error output in case of mandatory system view name is omitted. */
    @Test
    public void testSystemViewNameMissedFailure() {
        assertContains(log, executeCommand(EXIT_CODE_INVALID_ARGUMENTS, CMD_SYS_VIEW),
            "The name of the system view for which its content should be printed is expected.");
    }

    /** Tests command error output in case value of {@link SystemViewCommandArg#NODE_ID} argument is omitted. */
    @Test
    public void testNodeIdMissedFailure() {
        assertContains(log, executeCommand(EXIT_CODE_INVALID_ARGUMENTS, CMD_SYS_VIEW, SVCS_VIEW, NODE_ID.argName()),
            "ID of the node from which system view content should be obtained is expected.");
    }

    /** Tests command error output in case value of {@link SystemViewCommandArg#NODE_ID} argument is invalid.*/
    @Test
    public void testInvalidNodeIdFailure() {
        assertContains(log,
            executeCommand(EXIT_CODE_INVALID_ARGUMENTS, CMD_SYS_VIEW, SVCS_VIEW, NODE_ID.argName(), "invalid_node_id"),
            "Failed to parse " + NODE_ID.argName() +
                " command argument. String representation of \"java.util.UUID\" is exepected." +
                " For example: 123e4567-e89b-42d3-a456-556642440000"
        );
    }

    /** Tests command error output in case multiple system view names are specified. */
    @Test
    public void testMultipleSystemViewNamesFailure() {
        assertContains(log,
            executeCommand(EXIT_CODE_INVALID_ARGUMENTS, CMD_SYS_VIEW, SVCS_VIEW, CACHE_GRP_PAGE_LIST_VIEW),
            "Multiple system view names are not supported.");
    }

    /**
     * Tests command error output in case {@link SystemViewCommandArg#ALL_NODES} and
     * {@link SystemViewCommandArg#NODE_IDS} are both specified.
     */
    @Test
    public void testAllNodesAndNodeIds() {
        assertContains(log, executeCommand(EXIT_CODE_INVALID_ARGUMENTS,
                CMD_SYS_VIEW, SVCS_VIEW, ALL_NODES.argName(), NODE_IDS.argName(), ignite0.localNode().id().toString()),
            "The " + ALL_NODES.argName() + " parameter cannot be used with specified node IDs.");

        assertContains(log, executeCommand(EXIT_CODE_INVALID_ARGUMENTS,
                CMD_SYS_VIEW, SVCS_VIEW, ALL_NODES.argName(), NODE_ID.argName(), ignite0.localNode().id().toString()),
            "The " + ALL_NODES.argName() + " parameter cannot be used with specified node IDs.");
    }

    /**
     * Tests command error output in case {@link SystemViewCommandArg#NODE_ID} argument value refers to nonexistent
     * node.
     */
    @Test
    public void testNonExistentNodeIdFailure() {
        String incorrectNodeId = UUID.randomUUID().toString();

        assertContains(log,
            executeCommand(EXIT_CODE_INVALID_ARGUMENTS, CMD_SYS_VIEW, "--node-id", incorrectNodeId, CACHES_VIEW),
            "Failed to perform operation.\nNode with id=" + incorrectNodeId + " not found");
    }

    /** Tests command output in case nonexistent system view names is specified. */
    @Test
    public void testNonExistentSystemView() {
        assertContains(log, executeCommand(EXIT_CODE_OK, CMD_SYS_VIEW, "non_existent_system_view"),
            "No system view with specified name was found [name=non_existent_system_view]");
    }

    /** */
    @Test
    public void testCachesView() {
        Set<String> cacheNames = new HashSet<>(asList("cache-1", "cache-2"));

        cacheNames.forEach(cacheName -> ignite0.createCache(cacheName));

        List<List<String>> cachesView = systemView(ignite0, CACHES_VIEW);

        assertEquals(ignite0.context().cache().cacheDescriptors().size(), cachesView.size());

        cachesView.forEach(row -> cacheNames.remove(row.get(0)));

        assertTrue(cacheNames.isEmpty());
    }

    /** */
    @Test
    public void testCacheGroupsView() {
        Set<String> grpNames = new HashSet<>(asList("grp-1", "grp-2"));

        for (String grpName : grpNames)
            ignite0.createCache(new CacheConfiguration<>("cache-" + grpName).setGroupName(grpName));

        List<List<String>> cacheGrpsView = systemView(ignite0, CACHE_GRPS_VIEW);

        assertEquals(ignite0.context().cache().cacheGroupDescriptors().size(), cacheGrpsView.size());

        cacheGrpsView.forEach(row -> grpNames.remove(row.get(0)));

        assertTrue(grpNames.toString(), grpNames.isEmpty());
    }

    /** */
    @Test
    public void testServices() {
        ServiceConfiguration srvcCfg = new ServiceConfiguration();

        srvcCfg.setName("service");
        srvcCfg.setMaxPerNodeCount(1);
        srvcCfg.setService(new DummyService());

        ignite0.services().deploy(srvcCfg);

        List<List<String>> srvsView = systemView(ignite0, SVCS_VIEW);

        assertEquals(1, srvsView.size());

        List<String> sysView = srvsView.get(0);

        assertEquals(srvcCfg.getName(), sysView.get(1)); // name
        assertEquals(DummyService.class.getName(), sysView.get(2)); // serviceClass
        assertEquals(Integer.toString(srvcCfg.getMaxPerNodeCount()), sysView.get(6)); // maxPerNodeCount
    }

    /** */
    @Test
    public void testMultipleNodes() {
        checkNodesResult(Collections.singleton(ignite0), NODE_IDS.argName());
        checkNodesResult(Collections.singleton(client), NODE_IDS.argName());

        checkNodesResult(F.asList(ignite0, ignite1), NODE_IDS.argName());
        checkNodesResult(F.asList(ignite0, ignite1, client), NODE_IDS.argName());

        checkNodesResult(F.viewReadOnly(G.allGrids(), node -> (IgniteEx)node), ALL_NODES.argName());
    }

    /** */
    private void checkNodesResult(Collection<IgniteEx> nodes, String nodesArg) {
        Map<UUID, List<List<String>>> map = systemView(nodes, NODES_SYS_VIEW, nodesArg);

        assertEquals(nodes.size(), map.size());

        map.forEach((nodeId, rows) -> {
            assertEquals(ignite0.cluster().nodes().size(), rows.size());

            for (List<String> row : rows) {
                UUID rowNodeId = UUID.fromString(row.get(0));
                boolean isLocal = Boolean.parseBoolean(row.get(7));

                assertEquals(nodeId.equals(rowNodeId), isLocal);
            }
        });
    }

    /**
     * Gets system view content via control utility from specified node. Here we also check if attributes names
     * returned by the command match the real ones. And that both "SQL" and "Java" command names styles are supported.
     *
     * @param node The node to obtain system view from.
     * @param sysViewName Name of the system view which content is required.
     * @return Content of the requested system view.
     */
    private List<List<String>> systemView(IgniteEx node, String sysViewName) {
        Map<UUID, List<List<String>>> map = systemView(Collections.singleton(node), sysViewName, NODE_ID.argName());

        assertEquals(1, map.size());

        return map.get(node.localNode().id());
    }

    /**
     * Gets system view content via control utility from specified nodes. Here we also check if attributes names
     * returned by the command match the real ones. And that both "SQL" and "Java" command names styles are supported.
     *
     * @param nodes Nodes to obtain system view from.
     * @param sysViewName Name of the system view which content is required.
     * @param nodesArg Argument to specify nodes.
     * @return Content of the requested system view.
     */
    private Map<UUID, List<List<String>>> systemView(Collection<IgniteEx> nodes, String sysViewName, String nodesArg) {
        List<String> attrNames = new ArrayList<>();

        SystemView<?> sysView = nodes.iterator().next().context().systemView().view(sysViewName);

        sysView.walker().visitAll(new AttributeVisitor() {
            @Override public <T> void accept(int idx, String name, Class<T> clazz) {
                attrNames.add(name);
            }
        });

        int attrsCnt = sysView.walker().count();

        Map<UUID, List<List<String>>> map = null;

        for (String nameArg : F.asList(toSqlName(sysViewName), toSqlName(sysViewName).toLowerCase(), sysViewName)) {
            String[] args;

            if (ALL_NODES.argName().equals(nodesArg))
                args = new String[] {CMD_SYS_VIEW, nameArg, ALL_NODES.argName()};
            else {
                String nodeIds = String.join(",", F.viewReadOnly(nodes, n -> n.localNode().id().toString()));

                args = new String[] {CMD_SYS_VIEW, nameArg, nodesArg, nodeIds};
            }

            map = parseSystemViewCommandOutput(executeCommand(EXIT_CODE_OK, args));

            map.values().forEach(rows -> rows.forEach(row -> assertEquals(attrsCnt, row.size())));

            map.values().forEach(rows -> assertEquals(attrNames, rows.remove(0)));
        }

        return map;
    }

    /**
     * Obtains system view values for each row from command output.
     *
     * @param out Command output to parse.
     * @return System view values.
     */
    private Map<UUID, List<List<String>>> parseSystemViewCommandOutput(String out) {
        String outStart = "--------------------------------------------------------------------------------";

        String outEnd = "Command [" + SYSTEM_VIEW.toCommandName() + "] finished with code: " + EXIT_CODE_OK;

        String[] rows = out.substring(
            out.indexOf(outStart) + outStart.length() + 1,
            out.indexOf(outEnd) - 1
        ).split(U.nl());

        Pattern nodePtrn = Pattern.compile("Results from node with ID: (.*)");
        String tableDelim = "---";

        Map<UUID, List<List<String>>> res = new HashMap<>();

        UUID currNodeId = null;

        for (String rowStr : rows) {
            Matcher nodeMatcher = nodePtrn.matcher(rowStr);

            if (nodeMatcher.matches()) {
                currNodeId = UUID.fromString(nodeMatcher.group(1));

                continue;
            }

            if (tableDelim.equals(rowStr) || rowStr.isEmpty())
                continue;

            assertNotNull("Expected node ID: " + out, currNodeId);

            List<String> row = Arrays.stream(rowStr.split(quote(COLUMN_SEPARATOR)))
                .map(String::trim)
                .filter(str -> !str.isEmpty())
                .collect(Collectors.toList());

            res.computeIfAbsent(currNodeId, id -> new ArrayList<>()).add(row);
        }

        return res;
    }

    /**
     * Executes command and checks its exit code.
     *
     * @param expExitCode Expected exit code.
     * @param args Command lines arguments.
     * @return Result of command execution.
     */
    private String executeCommand(int expExitCode, String... args) {
        int res = execute(args);

        assertEquals(expExitCode, res);

        return testOut.toString();
    }
}
