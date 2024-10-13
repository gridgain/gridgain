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

package org.apache.ignite.internal.processors.rest.protocols.http.jetty;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteEvents;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.configuration.ConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.EventType;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Integration test for Grid REST functionality; Jetty is under the hood.
 */
public class RestSetupSimpleTest extends GridCommonAbstractTest {
    /** Jetty port. */
    private static final int JETTY_PORT = 8080;

    /** Mapper */
    private static final ObjectMapper MAPPER = new ObjectMapper();

    /** Auxiliar type reference for reading HTTP responses */
    private static final TypeReference<Map<String, Object>> RESPONSE_TYPE = new TypeReference<Map<String, Object>>() {
        //
    };

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration configuration = super.getConfiguration(igniteInstanceName);

        configuration.setConnectorConfiguration(new ConnectorConfiguration());

        configuration.setIncludeEventTypes(
            EventType.EVT_CACHE_ENTRY_DESTROYED
            , EventType.EVT_CACHE_ENTRY_DESTROYED
            , EventType.EVT_CACHE_ENTRY_EVICTED
            , EventType.EVT_CACHE_OBJECT_REMOVED
        );

        return configuration;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrid(0);
    }

    /**
     * Runs version command using GridJettyRestProtocol.
     */
    @Test
    public void testVersionCommand() throws Exception {
        URLConnection conn = new URL("http://localhost:" + JETTY_PORT + "/ignite?cmd=version").openConnection();

        conn.connect();

        try (InputStreamReader streamReader = new InputStreamReader(conn.getInputStream())) {
            Map<String, Object> myMap = readResponse(streamReader);

            log.info("Version command response is: " + myMap);

            assertTrue(myMap.containsKey("response"));
            assertEquals(0, myMap.get("successStatus"));
        }
    }

    /**
     * Runs version command using GridJettyRestProtocol.
     */
    @Test
    public void destroyClear() throws Exception {
        final String CACHE_NAME = "foo";

        Ignite grid = grid(0);

        IgniteCache<Integer, Integer> cache = grid.getOrCreateCache(CACHE_NAME);

        for (int i = 0; i < 10; i++) {
            cache.put(i, i);
        }

        AtomicBoolean removalHappened = new AtomicBoolean(false);
        grid.events().localListen(evt -> {
                removalHappened.set(true);
                return true;
            }, EventType.EVT_CACHE_ENTRY_DESTROYED
            , EventType.EVT_CACHE_ENTRY_DESTROYED
            , EventType.EVT_CACHE_ENTRY_EVICTED
            , EventType.EVT_CACHE_OBJECT_REMOVED);

        URLConnection conn = new URL("http://localhost:" + JETTY_PORT + "/ignite?cmd=destcache&cacheName=" + CACHE_NAME).openConnection();

        conn.connect();

        try (InputStreamReader streamReader = new InputStreamReader(conn.getInputStream())) {
            Map<String, Object> myMap = readResponse(streamReader);

            log.info("Version command response is: " + myMap);

            assertEquals(0, myMap.get("successStatus"));
        }

        assertNull(grid.cache(CACHE_NAME));
        assertFalse(removalHappened.get());
    }

    @Test
    public void cacheClear() throws Exception {
        final String CACHE_NAME = "foo";

        Ignite grid = grid(0);

        IgniteCache<Integer, Integer> cache = grid.getOrCreateCache(CACHE_NAME);

        for (int i = 0; i < 10; i++) {
            cache.put(i, i);
        }

        AtomicBoolean removalHappened = new AtomicBoolean(false);
        grid.events().localListen(evt -> {
                removalHappened.set(true);
                return true;
            }, EventType.EVT_CACHE_ENTRY_DESTROYED
            , EventType.EVT_CACHE_ENTRY_DESTROYED
            , EventType.EVT_CACHE_ENTRY_EVICTED
            , EventType.EVT_CACHE_OBJECT_REMOVED);

        URLConnection conn = new URL("http://localhost:" + JETTY_PORT + "/ignite?cmd=clear&cacheName=" + CACHE_NAME).openConnection();

        conn.connect();

        try (InputStreamReader streamReader = new InputStreamReader(conn.getInputStream())) {
            Map<String, Object> myMap = readResponse(streamReader);

            log.info("Version command response is: " + myMap);

            assertEquals(0, myMap.get("successStatus"));
        }

        assertNotNull(grid.cache(CACHE_NAME));
        assertEquals(0, grid.cache(CACHE_NAME).size(CachePeekMode.PRIMARY));
        assertFalse(removalHappened.get());
    }

    private static Map<String, Object> readResponse(InputStreamReader streamReader) throws IOException {
        return MAPPER.readValue(streamReader, RESPONSE_TYPE);
    }
}
