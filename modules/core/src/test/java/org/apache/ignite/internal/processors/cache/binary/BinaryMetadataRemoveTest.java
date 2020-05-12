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

package org.apache.ignite.internal.processors.cache.binary;

import java.util.Arrays;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.Ignite;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.DiscoverySpiCustomMessage;
import org.apache.ignite.spi.discovery.DiscoverySpiListener;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

/**
 */
public class BinaryMetadataRemoveTest extends GridCommonAbstractTest {
    /** Max retry cont. */
    private static final int MAX_RETRY_CONT = 10;

    /** */
    private static final String CACHE_NAME = "cache";

    /** */
    private GridTestUtils.DiscoveryHook discoveryHook;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        TcpDiscoverySpi discoSpi;

        final GridTestUtils.DiscoveryHook discoveryHook0 = discoveryHook;

        if (discoveryHook0 != null) {
            discoSpi = new TcpDiscoverySpi() {
                @Override public void setListener(@Nullable DiscoverySpiListener lsnr) {
                    if (discoveryHook0 != null)
                        super.setListener(GridTestUtils.DiscoverySpiListenerWrapper.wrap(lsnr, discoveryHook0));
                }
            };
        }
        else
            discoSpi = new TcpDiscoverySpi();

        cfg.setDiscoverySpi(discoSpi);

        cfg.setCacheConfiguration(new CacheConfiguration().setName(CACHE_NAME));

        return cfg;
    }

    /**
     */
    protected void startCluster() throws Exception {
        startGrid("srv0");
        startGrid("srv1");
        startGrid("srv2");
        startClientGrid("cli0");
        startClientGrid("cli1");
        startClientGrid("cli2");
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        startCluster();

        discoveryHook = null;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /**
     * Tests remove type metadata at all nodes (coordinator, server, client).
     */
    @Test
    public void testRemoveTypeOnNodes() throws Exception {
        for (Ignite ignCreateType : G.allGrids()) {
            for (Ignite ignRemoveType : G.allGrids()) {
                for (Ignite ignRecreateType : G.allGrids()) {
                    log.info("+++ Check [createOn=" + ignCreateType.name() +
                        ", removeOn=" + ignRemoveType.name() + ", recreateOn=" + ignRecreateType.name());

                    BinaryObjectBuilder builder0 = ignCreateType.binary().builder("Type0");

                    builder0.setField("f", 1);
                    builder0.build();

                    delayIfClient(ignCreateType, ignRemoveType, ignRecreateType);

                    removeType((IgniteEx)ignRemoveType, "Type0");

                    delayIfClient(ignCreateType, ignRemoveType, ignRecreateType);

                    BinaryObjectBuilder builder1 = ignRecreateType.binary().builder("Type0");
                    builder1.setField("f", "string");
                    builder1.build();

                    delayIfClient(ignCreateType, ignRemoveType, ignRecreateType);

                    removeType((IgniteEx)ignRemoveType, "Type0");

                    delayIfClient(ignCreateType, ignRemoveType, ignRecreateType);
                }
            }
        }
    }

    /**
     * Tests reject metadata update on coordinator when remove type is processed.
     */
    @Test
    public void testChangeMetaWhenTypeRemoving() throws Exception {
        final CyclicBarrier barrier0 = new CyclicBarrier(2);
        final CyclicBarrier barrier1 = new CyclicBarrier(2);

        AtomicBoolean hookMsgs = new AtomicBoolean(true);

        discoveryHook = new GridTestUtils.DiscoveryHook() {
            @Override public void handleDiscoveryMessage(DiscoverySpiCustomMessage msg) {
                if (!hookMsgs.get())
                    return;

                DiscoveryCustomMessage customMsg = msg == null ? null
                    : (DiscoveryCustomMessage) IgniteUtils.field(msg, "delegate");

                if (customMsg instanceof MetadataRemoveProposedMessage) {
                    try {
                        barrier0.await();

                        barrier1.await();
                    }
                    catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        };

        // Install discovery hoot at the node 'srv1'
        stopGrid("srv1");
        IgniteEx ign = startGrid("srv1");

        discoveryHook = null;

        // Move srv2 node an the end of the discovery circle.
        stopGrid("srv2");
        startGrid("srv2");

        BinaryObjectBuilder builder0 = ign.binary().builder("Type0");

        builder0.setField("f", 1);
        builder0.build();

        GridTestUtils.runAsync(()-> {
            try {
                removeType(ign, "Type0");
            }
            catch (Exception e) {
                log.error("Unexpected exception", e);

                fail("Unexpected exception.");
            }
        });

        barrier0.await();

        GridTestUtils.assertThrows(log, () -> {
            BinaryObjectBuilder bld = grid("srv2").binary().builder("Type0");

            bld.setField("f1", 1);

            // Short delay guarantee that we go into update metadata before remove metadata continue processing.
            GridTestUtils.runAsync(()-> {
                try {
                    U.sleep(200);

                    hookMsgs.set(false);

                    barrier1.await();
                }
                catch (Exception e) {
                    // No-op.
                }
            });

            bld.build();
        }, BinaryObjectException.class, "The type is removing now");
    }

    /**
     * @param ign Node to remove type.
     * @param typeName Binary type name.
     */
    protected void removeType(IgniteEx ign, String typeName) throws Exception {
        Exception err = null;

        for (int i = 0; i < MAX_RETRY_CONT; ++i) {
            try {
                ign.context().cacheObjects().removeType(typeName);

                err = null;

                break;
            }
            catch(Exception e) {
                err = e;

                U.sleep(200);
            }
        }

        if (err != null)
            throw err;
    }

    /**
     * Delay operation if an operation is executed on a client node.
     *
     * @param igns Tests nodes.
     */
    protected void delayIfClient(Ignite ... igns) throws IgniteInterruptedCheckedException {
        boolean isThereCli = Arrays.stream(igns).anyMatch(ign -> ((IgniteEx)ign).context().clientNode());

        if (isThereCli)
            U.sleep(500);
    }
}
