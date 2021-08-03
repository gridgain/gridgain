/*
 * Copyright 2021 GridGain Systems, Inc. and Contributors.
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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.commandline.CommandHandler;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.junit.Test;

import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_OK;

/** Tests for partition size consistency reconciliation. */
public class GridCommandHandlerPartitionSizeReconciliationTest extends GridCommandHandlerClusterPerMethodAbstractTest {
    /** */
    static final long BROKEN_PART_SIZE = 10;

    /** */
    private static CommandHandler hnd;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setCacheConfiguration(cacheConfiguration(DEFAULT_CACHE_NAME));

        return cfg;
    }

    /**
     * @param name Name.
     */
    protected CacheConfiguration<Object, Object> cacheConfiguration(String name) {
        CacheConfiguration ccfg = new CacheConfiguration(name);

        ccfg.setBackups(1);
        ccfg.setOnheapCacheEnabled(false);
        ccfg.setAffinity(new RendezvousAffinityFunction(false, 16));

        return ccfg;
    }

    /** */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        hnd = new CommandHandler();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /** Partition size consistency reconciliation. */
    @Test
    public void testReconciliationSizeFixed() throws Exception {
        testReconciliationCommand(new String[]{"--cache", "partition_reconciliation", "--repair", "--local-output"}, true);
    }

    /** Partition size consistency reconciliation with explicite arg in control.sh command. */
    @Test
    public void testReconciliationSizeFixedWithExplicitArg() throws Exception {
        testReconciliationCommand(new String[]{"--cache", "partition_reconciliation", "--repair",
                "--cache-size-consistency-reconciliation", "true"},
            true);
    }

    /** Test that partition size not fixed if it's not need. */
    @Test
    public void testReconciliationSizeNotFixed1() throws Exception {
        testReconciliationCommand(new String[]{"--cache", "partition_reconciliation", "--repair",
                "--local-output", "--cache-size-consistency-reconciliation", "false"},
            false
        );
    }

    /** Test that partition size not fixed if it's not need. */
    @Test
    public void testReconciliationSizeNotFixed2() throws Exception {
        testReconciliationCommand(new String[]{"--cache", "partition_reconciliation",
                "--local-output", "--cache-size-consistency-reconciliation", "true"},
            false
        );
    }

    /**
     * <ul>
     *   <li>Start two nodes.</li>
     *   <li>Create cache.</li>
     *   <li>Break size.</li>
     *   <li>Invoke a reconciliation util for partition size consistency reconciliation.</li>
     *   <li>Check that size was fixed.</li>
     * </ul>
     */
    private void testReconciliationCommand(String[] cmdArgs, boolean expFixed) throws Exception {
        int nodesCnt = 2;

        IgniteEx ig = startGrids(nodesCnt);

        IgniteEx client = startClientGrid(nodesCnt);

        ig.cluster().active(true);

        IgniteCache<Object, Object> cache0 = client.cache(DEFAULT_CACHE_NAME);

        for (long i = 0; i < 1000; i++)
            cache0.put(i, i);

        int startSize = cache0.size();

        List<IgniteEx> grids = new ArrayList<>();

        for (int i = 0; i < nodesCnt; i++)
            grids.add(grid(i));

        breakCacheSizes(grids, new HashSet<>(Arrays.asList(DEFAULT_CACHE_NAME)));

        int breakSize = cache0.size();

        assertFalse(cache0.size() == startSize);

        assertEquals(EXIT_CODE_OK, execute(hnd, cmdArgs));

        if (expFixed)
            assertEquals(startSize, cache0.size());
        else
            assertEquals(breakSize, cache0.size());
    }

    /**
     *
     */
    private void breakCacheSizes(List<IgniteEx> nodes, Set<String> cacheNames) {
        nodes.forEach(node -> cacheNames.forEach(cacheName -> updatePartitionsSize(node, cacheName)));
    }

    /**
     *
     */
    private void updatePartitionsSize(IgniteEx grid, String cacheName) {
        GridCacheContext<Object, Object> cctx = grid
            .context()
            .cache()
            .cache(cacheName)
            .context();

        int cacheId = cctx.cacheId();

        cctx.group().topology().localPartitions().forEach(part -> part.dataStore().updateSize(cacheId, BROKEN_PART_SIZE));
    }
}
