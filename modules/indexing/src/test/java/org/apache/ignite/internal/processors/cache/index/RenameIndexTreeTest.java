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

package org.apache.ignite.internal.processors.cache.index;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.client.Person;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.pagemem.wal.record.IndexRenameRootPageRecord;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.persistence.RootPage;
import org.apache.ignite.internal.processors.query.h2.H2TableDescriptor;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.apache.ignite.internal.processors.query.h2.database.H2TreeIndex;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static java.util.stream.Collectors.toSet;
import static org.apache.ignite.internal.processors.cache.persistence.IndexStorageImpl.MAX_IDX_NAME_LEN;
import static org.apache.ignite.testframework.GridTestUtils.assertThrows;
import static org.apache.ignite.testframework.GridTestUtils.cacheContext;
import static org.apache.ignite.testframework.GridTestUtils.getFieldValue;

/**
 * Class for testing index tree renaming.
 */
public class RenameIndexTreeTest extends AbstractRebuildIndexTest {
    /**
     * Checking the correct renaming of the index root pages.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testRenamingIndexRootPage() throws Exception {
        IgniteEx n = startGrid(0);

        IgniteCache<Integer, Person> cache = n.cache(DEFAULT_CACHE_NAME);
        populate(cache, 100);

        String idxName = "IDX0";
        createIdx(cache, idxName);

        H2TreeIndex idx = index(n, cache, idxName);

        int segments = idx.segmentsCount();

        String oldTreeName = treeName(idx);
        assertExistIndexRoot(cache, oldTreeName, segments, true);

        String newTreeName = UUID.randomUUID().toString();

        // There will be no renaming.
        assertExistIndexRoot(cache, newTreeName, segments, false);
        assertTrue(renameIndexRoot(cache, newTreeName, newTreeName, segments).isEmpty());

        // Checking the validation of the new index name.
        String moreMaxLenName = Arrays.toString(new byte[MAX_IDX_NAME_LEN + 1]);
        assertThrows(log, () -> renameIndexRoot(cache, oldTreeName, moreMaxLenName, segments), Exception.class, null);

        assertEquals(segments, renameIndexRoot(cache, oldTreeName, newTreeName, segments).size());

        assertExistIndexRoot(cache, oldTreeName, segments, false);
        assertExistIndexRoot(cache, newTreeName, segments, true);
    }

    /**
     * Checking that the renamed index root pages after the checkpoint will be
     * correctly restored and found after the node is restarted.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testPersistRenamingIndexRootPage() throws Exception {
        IgniteEx n = startGrid(0);

        IgniteCache<Integer, Person> cache = n.cache(DEFAULT_CACHE_NAME);
        populate(cache, 100);

        String idxName = "IDX0";
        createIdx(cache, idxName);

        H2TreeIndex idx = index(n, cache, idxName);

        String oldTreeName = treeName(idx);
        String newTreeName = UUID.randomUUID().toString();

        int segments = idx.segmentsCount();
        assertEquals(segments, renameIndexRoot(cache, oldTreeName, newTreeName, segments).size());

        forceCheckpoint();

        stopGrid(0);

        n = startGrid(0);
        cache = n.cache(DEFAULT_CACHE_NAME);

        assertExistIndexRoot(cache, oldTreeName, segments, true);
        assertExistIndexRoot(cache, newTreeName, segments, true);
    }

    /**
     * Checking that if we rename the index root pages without a checkpoint,
     * then after restarting the node we will not find them.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testNotPersistRenamingIndexRootPage() throws Exception {
        IgniteEx n = startGrid(0);

        IgniteCache<Integer, Person> cache = n.cache(DEFAULT_CACHE_NAME);
        populate(cache, 100);

        String idxName = "IDX0";
        createIdx(cache, idxName);

        enableCheckpoints(n, getTestIgniteInstanceName(), false);

        H2TreeIndex idx = index(n, cache, idxName);

        String oldTreeName = treeName(idx);
        String newTreeName = UUID.randomUUID().toString();

        int segments = idx.segmentsCount();
        assertEquals(segments, renameIndexRoot(cache, oldTreeName, newTreeName, segments).size());

        stopGrid(0);

        n = startGrid(0);
        cache = n.cache(DEFAULT_CACHE_NAME);

        assertExistIndexRoot(cache, oldTreeName, segments, true);
        assertExistIndexRoot(cache, newTreeName, segments, false);
    }

    /**
     * Checking applying {@link IndexRenameRootPageRecord}.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testIndexRenameRootPageRecord() throws Exception {
        IgniteEx n = startGrid(0);

        IgniteCache<Integer, Person> cache = n.cache(DEFAULT_CACHE_NAME);

        populate(cache, 100);

        String idxName = "IDX0";
        createIdx(cache, idxName);

        enableCheckpoints(n, getTestIgniteInstanceName(), false);

        H2TreeIndex idx = index(n, cache, idxName);

        String oldTreeName = treeName(idx);
        String newTreeName = UUID.randomUUID().toString();

        int segments = idx.segmentsCount();
        int cacheId = cacheContext(cache).cacheId();

        IndexRenameRootPageRecord r = new IndexRenameRootPageRecord(cacheId, oldTreeName, newTreeName, segments);
        walMgr(n).log(r);

        Set<Integer> cacheIds = n.context().cache().cacheNames().stream().map(CU::cacheId).collect(toSet());
        int fakeCacheId = cacheIds.stream().mapToInt(Integer::intValue).sum();

        while (cacheIds.contains(fakeCacheId))
            fakeCacheId++;

        // Check that the node does not crash when trying to apply a logical record with a non-existing cacheId.
        walMgr(n).log(new IndexRenameRootPageRecord(fakeCacheId, oldTreeName, newTreeName, segments));

        stopGrid(0);

        n = startGrid(0);

        cache = n.cache(DEFAULT_CACHE_NAME);

        assertExistIndexRoot(cache, oldTreeName, segments, false);
        assertExistIndexRoot(cache, newTreeName, segments, true);
    }

    /**
     * Renaming index trees.
     *
     * @param cache Cache.
     * @param oldTreeName Old index tree name.
     * @param newTreeName Old index tree name.
     * @param segments Segment count.
     * @return Root pages of renamed trees.
     * @throws Exception If failed.
     */
    private Collection<RootPage> renameIndexRoot(
        IgniteCache<Integer, Person> cache,
        String oldTreeName,
        String newTreeName,
        int segments
    ) throws Exception {
        GridCacheContext<Integer, Person> cacheCtx = cacheContext(cache);

        List<RootPage> res = new ArrayList<>();

        cacheCtx.shared().database().checkpointReadLock();

        try {
            for (int i = 0; i < segments; i++) {
                RootPage rootPage = cacheCtx.offheap().renameRootPageForIndex(
                    cacheCtx.cacheId(),
                    oldTreeName,
                    newTreeName,
                    i
                );

                if (rootPage != null)
                    res.add(rootPage);
            }
        }
        finally {
            cacheCtx.shared().database().checkpointReadUnlock();
        }

        return res;
    }

    /**
     * Checking for the existence of root pages for an index.
     *
     * @param cache Cache.
     * @param treeName Index tree name.
     * @param segments Segment count.
     * @param expExist Expectation of existence.
     * @throws Exception If failed.
     */
    private void assertExistIndexRoot(
        IgniteCache<Integer, Person> cache,
        String treeName,
        int segments,
        boolean expExist
    ) throws Exception {
        GridCacheContext<Integer, Person> cacheCtx = cacheContext(cache);

        for (int i = 0; i < segments; i++) {
            RootPage rootPage = cacheCtx.offheap().findRootPageForIndex(cacheCtx.cacheId(), treeName, i);

            assertEquals(expExist, rootPage != null);
        }
    }

    /**
     * Getting the cache index.
     *
     * @param n Node.
     * @param cache Cache.
     * @param idxName Index name.
     * @return Index.
     */
    @Nullable private H2TreeIndex index(IgniteEx n, IgniteCache<Integer, Person> cache, String idxName) {
        IgniteH2Indexing indexing = (IgniteH2Indexing)n.context().query().getIndexing();

        return indexing.schemaManager().tablesForCache(cacheContext(cache).name()).stream()
            .map(H2TableDescriptor::table)
            .map(table -> table.getIndex(idxName))
            .findAny()
            .map(H2TreeIndex.class::cast)
            .orElse(null);
    }

    /**
     * Getting index tree name.
     *
     * @param idx Index.
     * @return Index tree name.
     */
    private String treeName(H2TreeIndex idx) {
        return getFieldValue(idx, "treeName");
    }
}
