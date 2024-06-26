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

package org.apache.ignite.internal.processors.cache;

import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.managers.communication.GridMessageListener;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearGetRequest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearSingleGetRequest;
import org.apache.ignite.testframework.MvccFeatureChecker;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Assume;
import org.junit.Test;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheRebalanceMode.SYNC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.internal.GridTopic.TOPIC_CACHE;

/**
 *
 */
public class GridCachePartitionedGetSelfTest extends GridCommonAbstractTest {
    /** */
    private static final int GRID_CNT = 3;

    /** */
    private static final String KEY = "key";

    /** */
    private static final int VAL = 1;

    /** */
    private static final AtomicBoolean received = new AtomicBoolean();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setCacheConfiguration(cacheConfiguration());

        return cfg;
    }

    /**
     * @return Cache configuration.
     */
    private CacheConfiguration cacheConfiguration() {
        CacheConfiguration cc = defaultCacheConfiguration();

        cc.setCacheMode(PARTITIONED);
        cc.setBackups(1);
        cc.setRebalanceMode(SYNC);
        cc.setWriteSynchronizationMode(FULL_SYNC);
        cc.setNearConfiguration(null);
        cc.setReadFromBackup(true);

        return cc;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGridsMultiThreaded(GRID_CNT);

        prepare();
    }

    @Override protected void beforeTest() throws Exception {
        received.set(false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testGetFromPrimaryNode() throws Exception {
        for (int i = 0; i < GRID_CNT; i++) {
            IgniteCache<String, Integer> c = grid(i).cache(DEFAULT_CACHE_NAME);

            if (grid(i).affinity(DEFAULT_CACHE_NAME).isPrimary(grid(i).localNode(), KEY)) {
                info("Primary node: " + grid(i).localNode().id());

                c.get(KEY);

                break;
            }
        }

        assert !await();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testGetFromBackupNode() throws Exception {
        Assume.assumeFalse("https://issues.apache.org/jira/browse/IGNITE-10274", MvccFeatureChecker.forcedMvcc());

        MvccFeatureChecker.skipIfNotSupported(MvccFeatureChecker.Feature.EVICTION);

        for (int i = 0; i < GRID_CNT; i++) {
            IgniteCache<String, Integer> c = grid(i).cache(DEFAULT_CACHE_NAME);

            if (grid(i).affinity(DEFAULT_CACHE_NAME).isBackup(grid(i).localNode(), KEY)) {
                info("Backup node: " + grid(i).localNode().id());

                Integer val = c.get(KEY);

                assert val != null && val == 1;

                assert !await();

                c.localEvict(Collections.singleton(KEY));

                assert c.localPeek(KEY, CachePeekMode.ONHEAP) == null;

                val = c.get(KEY);

                assert val != null && val == 1;

                assert !await();

                break;
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testGetFromNearNode() throws Exception {
        for (int i = 0; i < GRID_CNT; i++) {
            IgniteCache<String, Integer> c = grid(i).cache(DEFAULT_CACHE_NAME);

            if (!grid(i).affinity(DEFAULT_CACHE_NAME).isPrimaryOrBackup(grid(i).localNode(), KEY)) {
                info("Near node: " + grid(i).localNode().id());

                Integer val = c.get(KEY);

                assert val != null && val == 1;

                break;
            }
        }

        assert await();
    }

    /**
     * @return {@code True} if awaited message.
     * @throws Exception If failed.
     */
    @SuppressWarnings({"BusyWait"})
    private boolean await() throws Exception {
        info("Checking flag: " + System.identityHashCode(received));

        for (int i = 0; i < 3; i++) {
            if (received.get())
                return true;

            info("Flag is false.");

            Thread.sleep(500);
        }

        return received.get();
    }

    /**
     * Puts value to primary node and registers listener
     * that sets {@link #received} flag to {@code true}
     * if {@link GridNearGetRequest} was received on primary node.
     *
     * @throws Exception If failed.
     */
    private void prepare() throws Exception {
        for (int i = 0; i < GRID_CNT; i++) {
            Ignite g = grid(i);

            if (grid(i).affinity(DEFAULT_CACHE_NAME).isPrimary(grid(i).localNode(), KEY)) {
                info("Primary node: " + g.cluster().localNode().id());

                // Put value.
                g.cache(DEFAULT_CACHE_NAME).put(KEY, VAL);

                // Register listener.
                ((IgniteKernal)g).context().io().addMessageListener(
                    TOPIC_CACHE,
                    new GridMessageListener() {
                        @Override public void onMessage(UUID nodeId, Object msg, byte plc) {
                            info("Received message from node [nodeId=" + nodeId + ", msg=" + msg + ']');

                            if (msg instanceof GridNearSingleGetRequest) {
                                info("Setting flag: " + System.identityHashCode(received));

                                received.set(true);
                            }
                        }
                    }
                );

                break;
            }
        }
    }
}
