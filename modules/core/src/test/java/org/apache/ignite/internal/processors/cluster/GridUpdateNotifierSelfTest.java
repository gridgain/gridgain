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

package org.apache.ignite.internal.processors.cluster;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.GridKernalGateway;
import org.apache.ignite.internal.IgniteProperties;
import org.apache.ignite.internal.IgniteVersionUtils;
import org.apache.ignite.internal.managers.discovery.GridDiscoveryManager;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteProductVersion;
import org.apache.ignite.plugin.PluginProvider;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonTest;
import org.junit.Test;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;

/**
 * Update notifier test.
 */
@GridCommonTest(group = "Kernal Self")
public class GridUpdateNotifierSelfTest extends GridCommonAbstractTest {
    /** Server nodes count. */
    public static final int SERVER_NODES = 3;

    /** */
    private String updateStatusParams;

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 120 * 1000;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        System.setProperty(IgniteSystemProperties.IGNITE_UPDATE_NOTIFIER, "true");

        Properties props = U.field(IgniteProperties.class, "PROPS");

        updateStatusParams = props.getProperty("ignite.update.status.params");

        props.setProperty("ignite.update.status.params", "ver=" + IgniteProperties.get("ignite.version"));
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        System.setProperty(IgniteSystemProperties.IGNITE_UPDATE_NOTIFIER, "false");

        Properties props = U.field(IgniteProperties.class, "PROPS");

        props.setProperty("ignite.update.status.params", updateStatusParams);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testNotifier() throws Exception {
        String nodeVer = IgniteProperties.get("ignite.version");

        HttpIgniteUpdatesChecker updatesCheckerMock = Mockito.mock(HttpIgniteUpdatesChecker.class);

        // Return current node version and some other info
        Map<String, String> updates = new HashMap<>();
        updates.put("meta", "meta");
        updates.put("latest_version", "99.88.77");
        updates.put("download_url", "http://example.com/gg");
        updates.put("eol_message", "EOL MESSAGE");

        Mockito.when(updatesCheckerMock.getUpdates(anyString(), any())).thenReturn(updates);

        GridKernalContext ctx = Mockito.mock(GridKernalContext.class);
        GridDiscoveryManager discovery = Mockito.mock(GridDiscoveryManager.class);
        List<ClusterNode> srvNodes = Mockito.mock(List.class);

        Mockito.when(srvNodes.size()).thenReturn(SERVER_NODES);
        Mockito.when(discovery.serverNodes(Mockito.any(AffinityTopologyVersion.class))).thenReturn(srvNodes);
        Mockito.when(ctx.discovery()).thenReturn(discovery);

        GridUpdateNotifier ntf = new GridUpdateNotifier(null, nodeVer, null, ctx.discovery(), Collections.emptyList(), false, updatesCheckerMock);

        ntf.checkForNewVersion(log);

        String ver = ntf.latestVersion();

        // Wait 60 sec for response.
        for (int i = 0; ver == null && i < 600; i++) {
            Thread.sleep(100);

            ver = ntf.latestVersion();
        }

        info("Notifier version [ver=" + ver + ", nodeVer=" + nodeVer + ']');

        assertNotNull("Ignite latest version has not been detected.", ver);

        byte nodeMaintenance = IgniteProductVersion.fromString(nodeVer).maintenance();

        byte lastMaintenance = IgniteProductVersion.fromString(ver).maintenance();

        assertTrue("Wrong latest version [nodeVer=" + nodeMaintenance + ", lastVer=" + lastMaintenance + ']',
            (nodeMaintenance == 0 && lastMaintenance == 0) || (nodeMaintenance > 0 && lastMaintenance > 0));

        ntf.reportStatus(log);

        assertEquals("EOL MESSAGE", ntf.endOfLifeMessage());
    }

    /**
     * @throws IgniteCheckedException if failed.
     * @throws NoSuchFieldException if failed.
     * @throws IllegalAccessException if failed.
     */
    @Test
    public void testInitializationWithNullPluginProviderVersion() throws IgniteCheckedException, NoSuchFieldException, IllegalAccessException {
        PluginProvider pp = Mockito.mock(PluginProvider.class);
        Mockito.when(pp.version()).thenReturn(null);
        Mockito.when(pp.name()).thenReturn("my-cool-name");

        GridUpdateNotifier notifier = new GridUpdateNotifier(
            "", "",
            Mockito.mock(GridKernalGateway.class),
            Mockito.mock(GridDiscoveryManager.class),
            Arrays.asList(pp),
            true, Mockito.mock(HttpIgniteUpdatesChecker.class)
        );

        Field vers = notifier.getClass().getDeclaredField("pluginsVers");
        vers.setAccessible(true);
        String versionsString = (String)vers.get(notifier);
        assertTrue(versionsString.contains("my-cool-name-plugin-version=UNKNOWN"));
    }

    @Test
    public void testGetUpdatesCurrentVersion() throws Exception {
        Map<String, String> updates = getUpdates(IgniteVersionUtils.VER_STR);

        assertEquals("", updates.get("eol_message"));
    }

    @Test
    public void testGetUpdatesOldVersionEndOfLife() throws Exception {
        Map<String, String> updates = getUpdates("8.7.2");

        assertEquals(
                "The GridGain 8.7.x is End Of Life since 2021-12-31. " +
                        "Upgrade now to the latest version of GridGain 8.9. " +
                        "Learn more on the Versioning and Support Lifecycle, and upgrade options: " +
                        "https://www.gridgain.com/docs/latest/installation-guide/versioning-and-support-lifecycle",
                updates.get("eol_message"));
    }

    private static Map<String, String> getUpdates(String ver) throws Exception {
        HttpIgniteUpdatesChecker checker = new HttpIgniteUpdatesChecker(GridUpdateNotifier.DEFAULT_GRIDGAIN_UPDATES_URL, "UTF-8");

        GridUpdateNotifier notifier = new GridUpdateNotifier(
            "test-instance",
            ver,
            Mockito.mock(GridKernalGateway.class),
            Mockito.mock(GridDiscoveryManager.class),
            Collections.emptyList(),
            true,
            checker
        );

        Map<String, String> updates = notifier.getUpdates();

        assertTrue(updates.containsKey("latest_version"));
        assertTrue(updates.containsKey("eol_message"));

        return updates;
    }
}
