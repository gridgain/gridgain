/*
 * Copyright 2022 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.internal.metric;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.StopNodeFailureHandler;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.pagemem.PageIdAllocator;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.cache.persistence.DataRegion;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStore;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.PageMetrics;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.processors.cache.persistence.wal.crc.IgniteDataIntegrityViolationException;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static java.util.stream.Collectors.joining;
import static org.apache.ignite.internal.util.IgniteUtils.KB;
import static org.apache.ignite.internal.util.IgniteUtils.MB;
import static org.apache.ignite.testframework.GridTestUtils.getFieldValue;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.assertThat;

/**
 * Test class for checking that index page metrics stay correct in case of page displacement.
 */
public class IndexPagesMetricsPageDisplacementTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setFailureHandler(new StopNodeFailureHandler())
            .setCacheConfiguration(
                new CacheConfiguration<>(DEFAULT_CACHE_NAME).setIndexedTypes(Integer.class, Person.class)
            )
            .setDataStorageConfiguration(
                new DataStorageConfiguration()
                    // Default values are not suitable for us because bigger concurrency level means bigger
                    // segment sizes which means no page replacement.
                    .setPageSize((int)(4 * KB))
                    .setConcurrencyLevel(4)
                    .setDefaultDataRegionConfiguration(
                        new DataRegionConfiguration()
                            .setMaxSize(12 * MB)
                            .setPersistenceEnabled(true)
                    )
            );
    }

    /**
     * Inserts data into the cache as long as it fills enough for some data pages to get dumped to the
     * storage. Since index page metrics should reflect the number of in-memory pages, the metric value is expected to
     * go down.
     */
    @Test
    public void testPageDisplacement() throws Exception {
        IgniteEx grid = startGrid(0);

        grid.cluster().state(ClusterState.ACTIVE);

        IndexPageCounter idxPageCounter = new IndexPageCounter(grid, true);

        IgniteInternalCache<Integer, Person> cache = grid.cachex(DEFAULT_CACHE_NAME);

        int grpId = cache.context().groupId();

        DataRegion dataRegion = grid.context().cache().context().database().dataRegion(null);
        PageMetrics pageMetrics = dataRegion.metrics().cacheGrpPageMetrics(grpId);

        // Test may start to flaky if the size of the PageMemoryImpl.Segment increases.
        assertEquals(
            dataRegion.config().getMaxSize() + IgniteUtils.checkpointBufferSize(dataRegion.config()),
            LongStream.of(getFieldValue(dataRegion.pageMemory(), "sizes")).sum()
        );

        int personId = 0;
        long idxPagesOnDisk;
        long idxPagesInMemory;

        // To make the index pages larger (and their number) and have a higher chance of replacing them.
        String namePrefix = IntStream.range(0, (int)KB).mapToObj(i -> "a").collect(joining());

        do {
            // insert data into the cache until some index pages get displaced to the storage
            for (int i = 0; i < 100; i++, personId++)
                cache.put(
                    personId,
                    new Person(personId, namePrefix + "_name_" + personId, namePrefix + "_surname_" + personId)
                );

            forceCheckpoint(grid);

            idxPagesOnDisk = getIdxPagesOnDisk(grid, grpId).size();
            idxPagesInMemory = idxPageCounter.countIdxPagesInMemory(grpId);

            assertThat(pageMetrics.indexPages().value(), is(idxPagesInMemory));
        } while (idxPagesOnDisk <= idxPagesInMemory);

        // load pages back into memory and check that the metric value has increased
        touchIdxPages(grid, grpId);

        long allIdxPagesInMemory = idxPageCounter.countIdxPagesInMemory(grpId);

        assertThat(allIdxPagesInMemory, greaterThan(idxPagesInMemory));
        assertThat(pageMetrics.indexPages().value(), is(allIdxPagesInMemory));
    }

    /**
     * Acquires all pages inside the index partition for the given cache group in order to load them back into
     * memory if some of them had been displaced to the storage earlier.
     */
    private void touchIdxPages(IgniteEx grid, int grpId) throws IgniteCheckedException {
        DataRegion dataRegion = grid.context().cache().context().database().dataRegion(null);
        PageMemory pageMemory = dataRegion.pageMemory();

        for (long pageId : getIdxPagesOnDisk(grid, grpId))
            // acquire, but not release all pages, so that they don't get displaced back to the storage
            pageMemory.acquirePage(grpId, pageId);
    }

    /**
     * Returns IDs of index pages currently residing in the storage.
     */
    private List<Long> getIdxPagesOnDisk(IgniteEx grid, int grpId) throws IgniteCheckedException {
        FilePageStoreManager pageStoreMgr = (FilePageStoreManager) grid.context().cache().context().pageStore();
        FilePageStore pageStore = (FilePageStore) pageStoreMgr.getStore(grpId, PageIdAllocator.INDEX_PARTITION);

        List<Long> result = new ArrayList<>();

        ByteBuffer buf = ByteBuffer
            .allocateDirect(pageStore.getPageSize())
            .order(ByteOrder.nativeOrder());

        // Page Store contains a one-page header
        long numPages = pageStore.size() / pageStore.getPageSize() - 1;

        for (int i = 0; i < numPages; i++) {
            long pageId = PageIdUtils.pageId(PageIdAllocator.INDEX_PARTITION, (byte)0, i);

            try {
                pageStore.read(pageId, buf, false);
            } catch (IgniteDataIntegrityViolationException ignored) {
                // sometimes we try to access an invalid page, in which case this exception will be thrown.
                // We simply ignore it and try to access other pages.
            }

            if (PageIO.isIndexPage(PageIO.getType(buf)))
                result.add(PageIO.getPageId(buf));

            U.clear(buf);
        }

        return result;
    }

    /**
     * A person entity used for the tests. More indexes are used so that there are more index pages.
     */
    private static class Person {
        /** Id. */
        @QuerySqlField(index = true)
        private Integer id;

        /** Name. */
        @QuerySqlField(index = true)
        private String name;

        /** Name. */
        @QuerySqlField(index = true)
        private String surname;

        /** */
        public Person(Integer id, String name, String surname) {
            this.id = id;
            this.name = name;
            this.surname = surname;
        }

        /** */
        public Integer getId() {
            return id;
        }

        /** */
        public String getName() {
            return name;
        }

        /** */
        public String getSurname() {
            return surname;
        }

        /** */
        public void setId(Integer id) {
            this.id = id;
        }

        /** */
        public void setName(String name) {
            this.name = name;
        }

        /** */
        public void setSurname(String surname) {
            this.surname = surname;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return Objects.hash(id, name, surname);
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object obj) {
            if (!(obj instanceof Person))
                return false;

            Person other = (Person)obj;

            return other.id.equals(id) && other.name.equals(name) && other.surname.equals(surname);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(Person.class, this);
        }
    }
}
