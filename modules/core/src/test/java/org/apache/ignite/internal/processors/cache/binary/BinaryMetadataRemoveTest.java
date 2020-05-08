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
import java.util.Map;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.Ignite;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.PA;
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

    /**
     * Number of {@link MetadataUpdateProposedMessage} that have been sent since a test was start.
     */

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        TcpDiscoverySpi discoSpi = discoveryHook == null ? new TcpDiscoverySpi() : new TcpDiscoverySpi() {
            @Override public void setListener(@Nullable DiscoverySpiListener lsnr) {
                super.setListener(GridTestUtils.DiscoverySpiListenerWrapper.wrap(lsnr, discoveryHook));
            }
        };

        cfg.setDiscoverySpi(discoSpi);

        cfg.setCacheConfiguration(new CacheConfiguration().setName(CACHE_NAME));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startCluster();
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
        discoveryHook = null;
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
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
     * Tests remove type metadata at all nodes (coordinator, server, client).
     */
    @Test
    public void testChangeOnLocalNodeWhenTypeRemoving() throws Exception {
        final CyclicBarrier barrier0 = new CyclicBarrier(2);
        final CyclicBarrier barrier1 = new CyclicBarrier(2);

        discoveryHook = new GridTestUtils.DiscoveryHook() {
            private volatile IgniteEx ignite;

            @Override public void handleDiscoveryMessage(DiscoverySpiCustomMessage msg) {
                DiscoveryCustomMessage customMsg = msg == null ? null
                    : (DiscoveryCustomMessage) IgniteUtils.field(msg, "delegate");

                if (customMsg instanceof MetadataRemoveAcceptedMessage) {
                    MetadataRemoveAcceptedMessage propMsg = (MetadataRemoveAcceptedMessage)customMsg;

                    try {
                        log.info("+++ on barrier0");
                        barrier0.await();
                        log.info("+++ leave barrier0");
                        barrier1.await();
                        log.info("+++ leave barrier1");
                    }
                    catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }

            @Override public void ignite(IgniteEx ignite) {
                this.ignite = ignite;
            }
        };

        stopGrid("srv0");
        IgniteEx ign = startGrid("srv0");

        BinaryObjectBuilder builder0 = ign.binary().builder("Type0");

        builder0.setField("f", 1);
        builder0.build();

        GridTestUtils.runAsync(()-> {
            try {
                removeType((IgniteEx)ign, "Type0");
            }
            catch (Exception e) {
                log.error("Unexpected exception", e);

                fail("Unexpected exception.");
            }
        });

        barrier0.await();

        System.out.println("+++ create new type");

        BinaryObjectBuilder builder1 = ign.binary().builder("Type0");

        builder0.setField("f1", 1);
        builder0.build();

        System.out.println("+++ END");
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
            U.sleep(100);
    }
}
