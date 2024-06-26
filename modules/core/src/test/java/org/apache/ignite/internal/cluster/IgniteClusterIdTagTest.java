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
package org.apache.ignite.internal.cluster;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.Ignite;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.ClusterIdUpdatedEvent;
import org.apache.ignite.events.ClusterTagUpdatedEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.IgnitionEx;
import org.apache.ignite.internal.processors.cluster.IgniteClusterMXBeanImpl;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.mxbean.IgniteClusterMXBean;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.internal.SupportFeaturesUtils.IGNITE_CLUSTER_ID_AND_TAG_FEATURE;

/**
 * Tests for ID and tag features of IgniteCluster.
 */
public class IgniteClusterIdTagTest extends GridCommonAbstractTest {
    /** */
    private static final UUID CUSTOM_ID_0 = UUID.fromString("11111111-1111-1111-1111-111111111111");

    /** */
    private static final UUID CUSTOM_ID_1 = UUID.fromString("22222222-2222-2222-2222-222222222222");

    /** */
    private static final UUID CLIENT_CUSTOM_ID_0 = UUID.fromString("33333333-3333-3333-3333-333333333333");

    /** */
    private static final UUID CLIENT_CUSTOM_ID_1 = UUID.fromString("44444444-4444-4444-4444-444444444444");

    /** */
    private static final String CUSTOM_TAG_0 = "my_super_cluster";

    /** */
    private static final String CUSTOM_TAG_1 = "not_so_super_but_OK";

    /** */
    private static final String CLIENT_CUSTOM_TAG_0 = "client_custom_tag_0";

    /** */
    private static final String CLIENT_CUSTOM_TAG_1 = "client_custom_tag_1";

    /** */
    private boolean isPersistenceEnabled;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        if (igniteInstanceName.contains("client"))
            cfg.setClientMode(true);
        else {
            DataStorageConfiguration dsCfg = new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(
                    new DataRegionConfiguration()
                        .setInitialSize(128 * 1024 * 1024)
                        .setMaxSize(128 * 1024 * 1024)
                        .setPersistenceEnabled(isPersistenceEnabled)
                );

            cfg.setDataStorageConfiguration(dsCfg);
        }

        cfg.setIncludeEventTypes(EventType.EVTS_ALL);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /**
     * Verifies that Cluster ID and tag are not available on client that hasn't connected to any server yet.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testClusterIdAndTagOnClient() throws Exception {
        String clientName = "client0";

        IgniteInternalFuture<IgniteEx> clFut = GridTestUtils.runAsync(() -> startGrid(clientName));

        GridTestUtils.waitForCondition(() -> {
            try {
                IgniteKernal clientGrid = IgnitionEx.gridx(clientName);

                return clientGrid != null && clientGrid.cluster() != null;
            }
            catch (Exception ignored) {
                return false;
            }
        }, 20_000);

        IgniteKernal cl0 = IgnitionEx.gridx("client0");

        assertNull(cl0.cluster().id());
        assertNull(cl0.cluster().tag());

        IgniteEx srv0 = startGrid(0);

        clFut.get();

        awaitPartitionMapExchange();

        assertNotNull(cl0.cluster().id());
        assertNotNull(cl0.cluster().tag());

        assertEquals(cl0.cluster().id(), srv0.cluster().id());
        assertEquals(cl0.cluster().tag(), srv0.cluster().tag());
    }

    /**
     * Verifies that Cluster ID and tag are available through JMX interface both for read and write.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testChangeClusterTagWithJMX() throws Exception {
        String srvName = "srv0";
        String newTag = "newTag0";

        IgniteEx srv0 = startGrid(srvName);

        IgniteClusterMXBean clustMxBean = getMxBean(
            srvName,
            "IgniteCluster",
            IgniteClusterMXBean.class,
            IgniteClusterMXBeanImpl.class
        );

        assertNotNull(clustMxBean.getId());
        assertNotNull(clustMxBean.getTag());

        assertEquals(srv0.cluster().id(), clustMxBean.getId());
        assertEquals(srv0.cluster().tag(), clustMxBean.getTag());

        clustMxBean.tag(newTag);

        assertEquals(newTag, srv0.cluster().tag());

        clustMxBean.id(CUSTOM_ID_0);

        assertEquals(CUSTOM_ID_0, srv0.cluster().id());
    }

    /**
     * Test verifies that cluster ID is generated upon cluster start
     * and correctly spread across all nodes joining later.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testInMemoryClusterId() throws Exception {
        Ignite ig0 = startGrid(0);

        UUID id0 = ig0.cluster().id();

        assertNotNull(id0);

        ig0.cluster().id(CUSTOM_ID_0);

        Ignite ig1 = startGrid(1);

        UUID id1 = ig1.cluster().id();

        assertNotNull(id1);

        assertEquals(CUSTOM_ID_0, id1);

        IgniteEx ig2 = startGrid(2);

        assertEquals(CUSTOM_ID_0, ig2.cluster().id());

        ig2.cluster().id(CUSTOM_ID_1);

        // ID set from one server node is applied on all other nodes.
        assertEquals(CUSTOM_ID_1, ig0.cluster().id());

        assertEquals(CUSTOM_ID_1, ig1.cluster().id());

        Ignite cl0 = startGrid("client0");

        assertEquals(CUSTOM_ID_1, cl0.cluster().id());

        cl0.cluster().id(CLIENT_CUSTOM_ID_0);

        //tag set from client is applied on server nodes
        assertEquals(CLIENT_CUSTOM_ID_0, ig0.cluster().id());

        IgniteEx cl1 = startGrid("client1");

        cl1.cluster().id(CLIENT_CUSTOM_ID_1);

        // ID set from client is applied on other client nodes.
        assertTrue(GridTestUtils.waitForCondition(() -> CLIENT_CUSTOM_ID_1.equals(cl0.cluster().id()), 5_000));
    }

    /**
     * Test verifies that even if user reaches internal APIs and tries to update cluster tag, it will be no-op
     * if the feature is turned off.
     *
     * @throws Exception If failed.
     */
    @Test
    @WithSystemProperty(key = IGNITE_CLUSTER_ID_AND_TAG_FEATURE, value = "false")
    public void testClusterIdAndTagAreNullWhenFeatureIsDisabled() throws Exception {
        IgniteEx ex = startGrid(0);

        IgniteClusterEx cl = ex.cluster();

        assertNull(cl.id());
        assertNull(cl.tag());

        cl.tag("new_tag");

        assertNull(cl.tag());

        cl.id(CUSTOM_ID_0);

        assertNull(cl.id());
    }

    /**
     * Test verifies that reconnected client applies newly generated ID and tag
     * and throws away values from old cluster.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testInMemoryClusterIdWithClientReconnect() throws Exception {
        IgniteClusterEx cluster0 = startGrid(0).cluster();

        UUID oldId = cluster0.id();
        String oldTag = cluster0.tag();

        IgniteEx client0 = startGrid("client0");

        AtomicBoolean reconnectEvent = new AtomicBoolean(false);

        client0.events().localListen((e) -> {
            reconnectEvent.set(true);

            return true;
        }, EventType.EVT_CLIENT_NODE_RECONNECTED);

        assertEquals(oldId, client0.cluster().id());
        assertEquals(oldTag, client0.cluster().tag());

        stopGrid(0);

        cluster0 = startGrid(0).cluster();

        assertNotSame(oldId, cluster0.id());
        assertNotSame(oldTag, cluster0.tag());

        assertTrue(GridTestUtils.waitForCondition(reconnectEvent::get, 10_000));

        assertEquals("OldID " + oldId, cluster0.id(), client0.cluster().id());
        assertEquals(cluster0.tag(), client0.cluster().tag());
    }

    /**
     * Verifies that in persistent-enabled cluster ID is not lost upon cluster restart.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testPersistentClusterId() throws Exception {
        isPersistenceEnabled = true;

        IgniteEx ig0 = startGrid(0);

        IgniteEx ig1 = startGrid(1);

        assertEquals(ig0.cluster().id(), ig1.cluster().id());

        UUID id1 = ig1.cluster().id();

        ig0.cluster().state(ClusterState.ACTIVE);

        stopAllGrids();

        ig0 = startGrid(0);

        ig1 = startGrid(1);

        assertEquals(id1, ig0.cluster().id());

        ig1.cluster().state(ClusterState.ACTIVE);

        IgniteEx cl0 = startGrid("client0");

        cl0.cluster().id(CUSTOM_ID_0);

        stopAllGrids();

        startGrid(0);

        ig1 = startGrid(1);

        assertEquals(CUSTOM_ID_0, ig1.cluster().id());
    }

    /**
     * Test verifies consistency of tag changes in cluster:
     * <ul>
     *     <li>Consistency across all server nodes when changed from a specific server node.</li>
     *     <li>Consistency across joining nodes including clients.</li>
     *     <li>Consistency across clients and servers when changed from client.</li>
     * </ul>
     *
     * @throws Exception If failed.
     */
    @Test
    public void testInMemoryClusterTag() throws Exception {
        IgniteEx ig0 = startGrid(0);

        String tag0 = ig0.cluster().tag();

        assertNotNull(tag0);

        ig0.cluster().tag(CUSTOM_TAG_0);

        IgniteEx ig1 = startGrid(1);

        String tag1 = ig1.cluster().tag();

        assertNotNull(tag1);

        assertEquals(CUSTOM_TAG_0, tag1);

        IgniteEx ig2 = startGrid(2);

        assertEquals(CUSTOM_TAG_0, ig2.cluster().tag());

        ig2.cluster().tag(CUSTOM_TAG_1);

        //tag set from one server node is applied on all other nodes
        assertEquals(CUSTOM_TAG_1, ig0.cluster().tag());

        assertEquals(CUSTOM_TAG_1, ig1.cluster().tag());

        IgniteEx cl0 = startGrid("client0");

        assertEquals(CUSTOM_TAG_1, cl0.cluster().tag());

        cl0.cluster().tag(CLIENT_CUSTOM_TAG_0);

        //tag set from client is applied on server nodes
        assertEquals(CLIENT_CUSTOM_TAG_0, ig0.cluster().tag());

        IgniteEx cl1 = startGrid("client1");

        cl1.cluster().tag(CLIENT_CUSTOM_TAG_1);

        //tag set from client is applied on other client nodes
        assertTrue(GridTestUtils.waitForCondition(() -> CLIENT_CUSTOM_TAG_1.equals(cl0.cluster().tag()), 5_000));
    }

    /**
     * Verifies restrictions for new tag provided for {@link IgniteClusterEx#tag(String)} method:
     * <ol>
     *     <li>Not null.</li>
     *     <li>Non-empty.</li>
     *     <li>Below 280 symbols (max tag length).</li>
     * </ol>
     *
     * @throws Exception If failed.
     */
    @Test
    public void testChangeTagExceptions() throws Exception {
        IgniteEx ig0 = startGrid(0);

        try {
            ig0.cluster().tag(null);

            fail("Expected exception has not been thrown.");
        }
        catch (NullPointerException e) {
            assertTrue(e.getMessage().contains("cannot be null"));
        }

        try {
            ig0.cluster().tag("");

            fail("Expected exception has not been thrown.");
        }
        catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("should not be empty"));
        }

        String longString = new String(new char[281]);

        try {
            ig0.cluster().tag(longString);

            fail("Expected exception has not been thrown.");
        }
        catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("Maximum tag length is exceeded"));
        }
    }


    /**
     * Verifies restrictions for new ID provided for {@link IgniteClusterEx#id(UUID)} method:
     * <ol>
     *     <li>Not null.</li>
     * </ol>
     *
     * @throws Exception If failed.
     */
    @Test
    public void testChangeIdExceptions() throws Exception {
        IgniteEx ig0 = startGrid(0);

        try {
            ig0.cluster().id(null);

            fail("Expected exception has not been thrown.");
        }
        catch (NullPointerException e) {
            assertTrue(e.getMessage().contains("cannot be null"));
        }
    }

    /**
     *  Verifies consistency of tag when set up in inactive and active clusters and on client nodes.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testPersistentClusterTag() throws Exception {
        isPersistenceEnabled = true;

        IgniteEx ig0 = startGrid(0);

        IgniteEx ig1 = startGrid(1);

        assertEquals(ig0.cluster().tag(), ig1.cluster().tag());

        String tag1 = ig1.cluster().tag();

        ig0.cluster().state(ClusterState.ACTIVE);

        stopAllGrids();

        ig0 = startGrid(0);

        ig1 = startGrid(1);

        assertEquals(tag1, ig0.cluster().tag());

        ig1.cluster().state(ClusterState.ACTIVE);

        IgniteEx cl0 = startGrid("client0");

        cl0.cluster().tag(CUSTOM_TAG_0);

        stopAllGrids();

        startGrid(0);

        ig1 = startGrid(1);

        assertEquals(CUSTOM_TAG_0, ig1.cluster().tag());
    }

    /**
     * Verifies that event is fired when tag change request sent by user is completed.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testTagChangedEvent() throws Exception {
        IgniteEx ig = startGrid(0);

        UUID clusterId = ig.cluster().id();
        String generatedTag = ig.cluster().tag();

        AtomicReference<UUID> clusterIdFromEvent = new AtomicReference<>(null);
        AtomicReference<String> oldTagFromEvent = new AtomicReference<>(null);
        AtomicReference<String> newTagFromEvent = new AtomicReference<>(null);

        AtomicBoolean evtFired = new AtomicBoolean(false);

        ig.events().localListen((evt) ->
            {
                evtFired.set(true);

                ClusterTagUpdatedEvent tagUpdatedEvt = (ClusterTagUpdatedEvent)evt;

                clusterIdFromEvent.set(tagUpdatedEvt.clusterId());
                oldTagFromEvent.set(tagUpdatedEvt.previousTag());
                newTagFromEvent.set(tagUpdatedEvt.newTag());

                return true;
            },
            EventType.EVT_CLUSTER_TAG_UPDATED);

        ig.cluster().tag(CUSTOM_TAG_0);

        assertTrue(GridTestUtils.waitForCondition(evtFired::get, 10_000));

        assertEquals(clusterId, clusterIdFromEvent.get());
        assertEquals(generatedTag, oldTagFromEvent.get());
        assertEquals(CUSTOM_TAG_0, newTagFromEvent.get());
    }

    /**
     * Verifies that event is fired when ID change request sent by user is completed.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testIdChangedEvent() throws Exception {
        IgniteEx ig = startGrid(0);

        UUID generatedId = ig.cluster().id();

        AtomicReference<UUID> previousIdFromEvent = new AtomicReference<>(null);
        AtomicReference<UUID> newIdFromEvent = new AtomicReference<>(null);

        AtomicBoolean evtFired = new AtomicBoolean(false);

        ig.events().localListen((evt) ->
            {
                evtFired.set(true);

                ClusterIdUpdatedEvent tagUpdatedEvt = (ClusterIdUpdatedEvent)evt;

                previousIdFromEvent.set(tagUpdatedEvt.previousId());
                newIdFromEvent.set(tagUpdatedEvt.newId());

                return true;
            },
            EventType.EVT_CLUSTER_ID_UPDATED);

        ig.cluster().id(CUSTOM_ID_0);

        assertTrue(GridTestUtils.waitForCondition(evtFired::get, 10_000));

        assertEquals(generatedId, previousIdFromEvent.get());
        assertEquals(CUSTOM_ID_0, newIdFromEvent.get());
    }

    /**
     * Verifies that event about cluster tag update is fired on remote nodes as well.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testTagChangedEventMultinodeWithRemoteFilter() throws Exception {
        IgniteEx ig0 = startGrid(0);

        IgniteEx ig1 = startGrid(1);

        UUID clusterId = ig0.cluster().id();
        String generatedTag = ig0.cluster().tag();

        AtomicReference<UUID> eventNodeId = new AtomicReference<>(null);

        AtomicReference<UUID> clusterIdFromEvent = new AtomicReference<>(null);
        AtomicReference<String> oldTagFromEvent = new AtomicReference<>(null);
        AtomicReference<String> newTagFromEvent = new AtomicReference<>(null);

        AtomicBoolean evtFired = new AtomicBoolean(false);

        ig0.events(ig0.cluster().forRemotes()).remoteListen(
            (IgniteBiPredicate<UUID, Event>)(uuid, event) -> {
                eventNodeId.set(uuid);

                evtFired.set(true);

                ClusterTagUpdatedEvent tagUpdatedEvt = (ClusterTagUpdatedEvent)event;

                clusterIdFromEvent.set(tagUpdatedEvt.clusterId());
                oldTagFromEvent.set(tagUpdatedEvt.previousTag());
                newTagFromEvent.set(tagUpdatedEvt.newTag());

                return true;
            },
            (IgnitePredicate<Event>)event -> event.type() == EventType.EVT_CLUSTER_TAG_UPDATED);

        ig0.cluster().tag(CUSTOM_TAG_0);

        assertTrue(GridTestUtils.waitForCondition(evtFired::get, 10_000));

        assertEquals(ig1.localNode().id(), eventNodeId.get());

        assertEquals(clusterId, clusterIdFromEvent.get());
        assertEquals(generatedTag, oldTagFromEvent.get());
        assertEquals(CUSTOM_TAG_0, newTagFromEvent.get());
    }

    /**
     * Verifies that event about cluster ID update is fired on remote nodes as well.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testIdChangedEventMultinodeWithRemoteFilter() throws Exception {
        IgniteEx ig0 = startGrid(0);

        IgniteEx ig1 = startGrid(1);

        UUID generatedId = ig0.cluster().id();

        AtomicReference<UUID> eventNodeId = new AtomicReference<>(null);

        AtomicReference<UUID> previousIdFromEvent = new AtomicReference<>(null);
        AtomicReference<UUID> newIdFromEvent = new AtomicReference<>(null);

        AtomicBoolean evtFired = new AtomicBoolean(false);

        ig0.events(ig0.cluster().forRemotes()).remoteListen(
            (IgniteBiPredicate<UUID, Event>)(uuid, event) -> {
                eventNodeId.set(uuid);

                evtFired.set(true);

                ClusterIdUpdatedEvent tagUpdatedEvt = (ClusterIdUpdatedEvent)event;

                previousIdFromEvent.set(tagUpdatedEvt.previousId());
                newIdFromEvent.set(tagUpdatedEvt.newId());

                return true;
            },
            (IgnitePredicate<Event>)event -> event.type() == EventType.EVT_CLUSTER_ID_UPDATED);

        ig0.cluster().id(CUSTOM_ID_0);

        assertTrue(GridTestUtils.waitForCondition(evtFired::get, 10_000));

        assertEquals(ig1.localNode().id(), eventNodeId.get());

        assertEquals(generatedId, previousIdFromEvent.get());
        assertEquals(CUSTOM_ID_0, newIdFromEvent.get());
    }
}
