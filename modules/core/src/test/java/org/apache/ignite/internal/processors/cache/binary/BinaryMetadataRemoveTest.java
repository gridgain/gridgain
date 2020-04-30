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

import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.binary.BinaryObjectBuilder;
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

    /**
     * Number of {@link MetadataUpdateProposedMessage} that have been sent since a test was start.
     */
    private static final AtomicInteger proposeMsgNum = new AtomicInteger();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        GridTestUtils.DiscoveryHook discoveryHook = new GridTestUtils.DiscoveryHook() {
            @Override public void handleDiscoveryMessage(DiscoverySpiCustomMessage msg) {
                DiscoveryCustomMessage customMsg = msg == null ? null
                    : (DiscoveryCustomMessage)IgniteUtils.field(msg, "delegate");

                if (customMsg instanceof MetadataRemoveProposedMessage)
                    proposeMsgNum.incrementAndGet();
            }
        };

        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi() {
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
        startGrid("srv0");
        startGrid("srv1");
        startGrid("srv2");
        startClientGrid("cli0");
        startClientGrid("cli1");
        startClientGrid("cli2");
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        proposeMsgNum.set(0);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /**
     * Tests registration of user classes.
     */
    @Test
    public void test() throws Exception {
        for (Ignite ignCreateType : G.allGrids()) {
            for (Ignite ignRemoveType : G.allGrids()) {
                for (Ignite ignRecreateType : G.allGrids()) {
                    log.info("+++ Check [createOn=" + ignCreateType.name() +
                        ", removeOn=" + ignRemoveType.name() + ", recreateOn=" + ignRecreateType.name());

                    BinaryObjectBuilder builder0 = ignCreateType.binary().builder("Type0");

                    builder0.setField("f", 1);
                    builder0.build();

                    removeType((IgniteEx)ignRemoveType, "Type0");

                    BinaryObjectBuilder builder1 = ignRecreateType.binary().builder("Type0");
                    builder1.setField("f", "string");
                    builder1.build();

                    removeType((IgniteEx)ignRemoveType, "Type0");
                }
            }
        }
    }

    /**
     * @param ign Node to remove type.
     * @param typeName Binary type name.
     */
    private void removeType(IgniteEx ign, String typeName) throws Exception {
        Exception err = null;

        for (int i = 0; i < MAX_RETRY_CONT; ++i) {
            try {
                ign.context().cacheObjects().removeType(typeName);

                err = null;

                break;
            }
            catch(Exception e) {
                err = e;

                U.sleep(100);
            }
        }

        if (err != null)
            throw err;
    }
}
