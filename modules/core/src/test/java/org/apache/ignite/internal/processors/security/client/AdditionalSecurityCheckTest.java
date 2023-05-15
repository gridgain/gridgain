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

package org.apache.ignite.internal.processors.security.client;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteAuthenticationException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.client.ClientAuthenticationException;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.client.GridClient;
import org.apache.ignite.internal.client.GridClientAuthenticationException;
import org.apache.ignite.internal.client.GridClientFactory;
import org.apache.ignite.internal.processors.metastorage.DistributedMetastorageLifecycleListener;
import org.apache.ignite.internal.processors.metastorage.ReadableDistributedMetaStorage;
import org.apache.ignite.lifecycle.LifecycleBean;
import org.apache.ignite.lifecycle.LifecycleEventType;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.testframework.GridTestUtils.runAsync;

/**
 * Security tests for thin client.
 */
@RunWith(JUnit4.class)
public class AdditionalSecurityCheckTest extends CommonSecurityCheckTest {
    /** */
    private LifecycleBean lifecycleBean;

    /** {@inheritDoc} */
    @Override
    protected IgniteConfiguration getConfiguration(String instanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(instanceName);

        if (lifecycleBean != null)
            cfg.setLifecycleBeans(lifecycleBean);

        return cfg;
    }

    /**
     *
     */
    @Test
    public void testClientInfo() throws Exception {
        Ignite ignite = startGrids(2);

        assertEquals(2, ignite.cluster().topologyVersion());

        startGrid(2);

        assertEquals(3, ignite.cluster().topologyVersion());
        assertFalse(ignite.cluster().active());

        try (GridClient client = GridClientFactory.start(getGridClientConfiguration())) {
            assertTrue(client.connected());

            client.state().active(true);
        }

        try (IgniteClient client = Ignition.startClient(getClientConfiguration())) {
            client.createCache("test_cache");

            assertEquals(1, client.cacheNames().size());
        }
    }

    /**
     *
     */
    @Test
    public void testClientInfoGridClientFail() throws Exception {
        Ignite ignite = startGrids(2);

        assertEquals(2, ignite.cluster().topologyVersion());

        startGrid(2);

        assertEquals(3, ignite.cluster().topologyVersion());

        fail = true;

        try (GridClient client = GridClientFactory.start(getGridClientConfiguration())) {
            assertFalse(client.connected());
            GridTestUtils.assertThrowsAnyCause(log,
                () -> {
                    throw client.checkLastError();
                },
                GridClientAuthenticationException.class,
                "Client version is not found.");
        }
    }

    /**
     *
     */
    @Test
    public void testClientInfoIgniteClientFail() throws Exception {
        Ignite ignite = startGrids(2);

        assertEquals(2, ignite.cluster().topologyVersion());

        startGrid(2);

        assertEquals(3, ignite.cluster().topologyVersion());

        fail = true;

        try (IgniteClient client = Ignition.startClient(getClientConfiguration())) {
            fail();
        }
        catch (ClientAuthenticationException e) {
            assertTrue(e.getMessage().contains("Client version is not found"));
        }
    }

    /**
     *
     */
    @Test
    public void testClientInfoClientFail() throws Exception {
        Ignite ignite = startGrids(1);

        assertEquals(1, ignite.cluster().topologyVersion());

        fail = true;

        GridTestUtils.assertThrowsAnyCause(log,
            () -> {
                startGrid(2);
                return null;
            },
            IgniteSpiException.class,
            "Authentication failed");

        assertEquals(1, ignite.cluster().topologyVersion());
    }

    /**
     *
     */
    @Test
    public void testAdditionalPasswordServerFail() throws Exception {
        Ignite ignite = startGrid(0);

        fail = true;

        GridTestUtils.assertThrowsAnyCause(log,
            () -> {
                startGrid(1);
                return null;
            },
            IgniteAuthenticationException.class,
            "Authentication failed");

        assertEquals(1, ignite.cluster().topologyVersion());
    }

    /**
     * Tests that the authentication request can be handled before a node is considered as completely started.
     *
     * @throws Exception If fails.
     */
    @Test
    public void testAuthenticationBeforeNodeStart() throws Exception {
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch contLatch = new CountDownLatch(1);
        AtomicReference<Exception> err = new AtomicReference<>();

        // Lifecycle bean that is used to block node start.
        lifecycleBean = new LifecycleBean() {
            /** Ignite instance. */
            @IgniteInstanceResource
            IgniteEx ignite;

            /** {@inheritDoc} */
            @Override public void onLifecycleEvent(LifecycleEventType evt) throws IgniteException {
                if (evt == LifecycleEventType.BEFORE_NODE_START) {
                    ignite.context().internalSubscriptionProcessor()
                        .registerDistributedMetastorageListener(new DistributedMetastorageLifecycleListener() {
                            @Override public void onReadyForRead(ReadableDistributedMetaStorage metastorage) {
                                try {
                                    startLatch.countDown();

                                    if (contLatch.await(10, SECONDS))
                                        throw new RuntimeException("Failed to wait for a latch to continue node start.");
                                }
                                catch (Exception e) {
                                    err.set(e);
                                }
                            }
                        });
                }
            }
        };

        IgniteInternalFuture<?> startFut = runAsync(() -> {
            startGrid(0);
        });

        assertTrue("Failed to wait for starting node.", startLatch.await(10, SECONDS));

        try (GridClient client = GridClientFactory.start(getGridClientConfiguration())) {
            err.set(client.checkLastError());
        }

        contLatch.countDown();

        startFut.get();

        Exception unexpectedErr = err.get();

        assertNull("Unexpected error [err=" + unexpectedErr + ']', unexpectedErr);
    }
}
