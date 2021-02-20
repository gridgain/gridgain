/*
 * Copyright 2020 GridGain Systems, Inc. and Contributors.
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
package org.apache.ignite.internal.processors.query.stat;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.cache.persistence.IgniteCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.MetastorageLifecycleListener;
import org.apache.ignite.internal.processors.metastorage.persistence.ReadWriteMetaStorageMock;
import org.apache.ignite.internal.processors.subscription.GridInternalSubscriptionProcessor;
import org.gridgain.internal.h2.value.ValueInt;
import org.junit.Test;
import org.mockito.Mockito;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;

/**
 * Test for statistics repository.
 */
public class IgniteStatisticsRepositoryTest extends StatisticsAbstractTest {
    /** First default key. */
    private static final StatisticsKey K1 = new StatisticsKey("PUBLIC", "tab1");

    /** Second default key. */
    private static final StatisticsKey K2 = new StatisticsKey("PUBLIC", "tab2");

    /** Column statistics with 100 nulls. */
    ColumnStatistics cs1 = new ColumnStatistics(null, null, 100, 0, 100,
        0, new byte[0]);

    /** Column statistics with 100 integers 0-100. */
    ColumnStatistics cs2 = new ColumnStatistics(ValueInt.get(0), ValueInt.get(100), 0, 100, 100,
        4, new byte[0]);

    /** Column statistics with 0 rows. */
    ColumnStatistics cs3 = new ColumnStatistics(null, null, 0, 0, 0, 0, new byte[0]);

    /** Column statistics with 100 integers 0-10. */
    ColumnStatistics cs4 = new ColumnStatistics(ValueInt.get(0), ValueInt.get(10), 0, 10, 100,
            4, new byte[0]);

    /**
     * Test ignite statistics repository on client node without persistence.
     */
    @Test
    public void testClientNode() {
        IgniteStatisticsDummyStoreImpl dummyStore = new IgniteStatisticsDummyStoreImpl(cls -> log);
        IgniteStatisticsRepositoryImpl statsRepos = new IgniteStatisticsRepositoryImpl(dummyStore, null, cls -> log);

        testRepositoryGlobal(statsRepos);
    }

    /**
     * Test ignite statistics repository on server node without persistence.
     */
    @Test
    public void testServerWithoutPersistence() {
        IgniteStatisticsStore store = new IgniteStatisticsInMemoryStoreImpl(cls -> log);
        IgniteStatisticsRepositoryImpl statsRepos = new IgniteStatisticsRepositoryImpl(store, null, cls -> log);

        testRepositoryGlobal(statsRepos);
        testRepositoryLocal(statsRepos);
        testRepositoryPartitions(statsRepos);
    }

    /**
     * Test ignite statistics repository on server node with persistence.
     */
    @Test
    public void testServerWithPersistence() throws IgniteCheckedException {
        MetastorageLifecycleListener lsnr[] = new MetastorageLifecycleListener[1];

        GridInternalSubscriptionProcessor subscriptionProcessor = Mockito.mock(GridInternalSubscriptionProcessor.class);
        Mockito.doAnswer(invocation -> lsnr[0] = invocation.getArgument(0))
            .when(subscriptionProcessor).registerMetastorageListener(Mockito.any(MetastorageLifecycleListener.class));
        IgniteCacheDatabaseSharedManager db = Mockito.mock(IgniteCacheDatabaseSharedManager.class);

        IgniteStatisticsRepositoryImpl statsRepos[] = new IgniteStatisticsRepositoryImpl[1];
        IgniteStatisticsStore store = new IgniteStatisticsPersistenceStoreImpl(subscriptionProcessor, db,
            (k, s) -> statsRepos[0].cacheLocalStatistics(k, s), cls -> log);
        IgniteStatisticsHelper helper = Mockito.mock(IgniteStatisticsHelper.class);
        statsRepos[0] = new IgniteStatisticsRepositoryImpl(store, helper, cls -> log);

        ReadWriteMetaStorageMock metastorage = new ReadWriteMetaStorageMock();
        lsnr[0].onReadyForReadWrite(metastorage);

        testRepositoryGlobal(statsRepos[0]);
        testRepositoryLocal(statsRepos[0]);
        testRepositoryPartitions(statsRepos[0]);
    }

    /**
     * Test specified statistics repository with partitions statistics.
     *
     * 1) Generate two object key and few partition statistics.
     * 2) Check that there are no statistics before tests.
     * 3) Put local partition statistics.
     * 4) Read and check partition statistics one by one.
     * 5) Read all partition statistics by object and check its size.
     * 6) Save few partition statistics at once.
     * 7) Real all partition statistics by object and check its size.
     *
     * @param repo Ignite statistics repository to test.
     */
    public void testRepositoryPartitions(IgniteStatisticsRepositoryImpl repo) {
        ObjectPartitionStatisticsImpl stat1 = getPartitionStatistics(1);
        ObjectPartitionStatisticsImpl stat10 = getPartitionStatistics(10);
        ObjectPartitionStatisticsImpl stat100 = getPartitionStatistics(100);

        ObjectPartitionStatisticsImpl stat1_2 = getPartitionStatistics(1);

        assertTrue(repo.getLocalPartitionsStatistics(K1).isEmpty());
        assertTrue(repo.getLocalPartitionsStatistics(K2).isEmpty());

        repo.saveLocalPartitionStatistics(K1, stat1);
        repo.saveLocalPartitionStatistics(K1, stat10);
        repo.saveLocalPartitionStatistics(K2, stat1_2);

        ObjectPartitionStatisticsImpl stat1Readed = repo.getLocalPartitionStatistics(K1, 1);
        assertNotNull(stat1Readed);
        assertEquals(1, stat1Readed.partId());

        ObjectPartitionStatisticsImpl stat10Readed = repo.getLocalPartitionStatistics(K1, 10);
        assertNotNull(stat10Readed);
        assertEquals(10, stat10Readed.partId());

        assertNull(repo.getLocalPartitionStatistics(K1, 2));

        assertEquals(2, repo.getLocalPartitionsStatistics(K1).size());
        assertEquals(1, repo.getLocalPartitionsStatistics(K2).size());

        repo.saveLocalPartitionsStatistics(K1, Arrays.asList(stat10, stat100));

        assertEquals(2, repo.getLocalPartitionsStatistics(K1).size());
    }

    /**
     * Test specified repository with local statistics:
     *
     * 1) Check that repository doesn't contains test table statistics.
     * 2) Save local statistics.
     * 3) Check that it doesn't available by wrong key and available by right one.
     * 4) Merge local statistics and check that new version available.
     *
     * @param repo Ignite statistics repository to test.
     */
    public void testRepositoryLocal(IgniteStatisticsRepositoryImpl repo) {
        assertNull(repo.getLocalStatistics(K1));
        assertNull(repo.getLocalStatistics(K2));

        ObjectStatisticsImpl stat1 = getStatistics(1);

        repo.saveLocalStatistics(K1, stat1);
        assertNull(repo.getLocalStatistics(K2));

        assertEquals(1L, repo.getLocalStatistics(K1).rowCount());

        ObjectStatisticsImpl stat2 = getStatistics(2);

        repo.mergeLocalStatistics(K1, stat2);

        assertNull(repo.getLocalStatistics(K2));
        assertEquals(2L, repo.getLocalStatistics(K1).rowCount());
    }

    /**
     * Test specified repository with global statistics:
     *
     * 1) Clear empty statistics (whole object and only one column).
     * 2) Save global statistics.
     * 3) Check that it doesn't available by wrong key and available by right key.
     * 4) Merge global statistics and check that new version available.
     *
     * @param repo Ignite statistics repository to test.
     */
    public void testRepositoryGlobal(IgniteStatisticsRepositoryImpl repo) {
        assertNull(repo.getGlobalStatistics(K1));
        repo.clearGlobalStatistics(K1);
        repo.clearGlobalStatistics(K1, "col10");

        ObjectStatisticsImpl tab1Statistics = getStatistics(1);

        repo.saveGlobalStatistics(K1, tab1Statistics);

        assertNull(repo.getGlobalStatistics(K2));

        assertEquals(1L, repo.getGlobalStatistics(K1).rowCount());

        ObjectStatisticsImpl tab1Statistics2 = getStatistics(2);

        repo.mergeGlobalStatistics(K1, tab1Statistics2);

        assertEquals(2L, repo.getGlobalStatistics(K1).rowCount());
    }

    /**
     * Test object statistics add:
     *
     * 1) Add statistics with partially the same columns.
     * 2) Add statistics with new columns.
     * 3) Add statistics with the same columns.
     */
    @Test
    public void addTest() {
        // 1) Add statistics with partially the same columns.
        HashMap<String, ColumnStatistics> colStat1 = new HashMap<>();
        colStat1.put("col1", cs1);
        colStat1.put("col2", cs2);

        HashMap<String, ColumnStatistics> colStat2 = new HashMap<>();
        colStat2.put("col2", cs3);
        colStat2.put("col3", cs4);

        ObjectStatisticsImpl os1 = new ObjectStatisticsImpl(100, colStat1);
        ObjectStatisticsImpl os2 = new ObjectStatisticsImpl(101, colStat2);

        ObjectStatisticsImpl sumStat1 = IgniteStatisticsRepositoryImpl.add(os1, os2);

        assertEquals(101, sumStat1.rowCount());
        assertEquals(3, sumStat1.columnsStatistics().size());
        assertEquals(cs3, sumStat1.columnStatistics("col2"));

        // 2) Add statistics with new columns.
        ObjectStatisticsImpl os3 = new ObjectStatisticsImpl(101, Collections.singletonMap("col3", cs3));

        ObjectStatisticsImpl sumStat2 = IgniteStatisticsRepositoryImpl.add(os1, os3);

        assertEquals(3, sumStat2.columnsStatistics().size());

        // 3) Add statistics with the same columns.
        HashMap<String, ColumnStatistics> colStat3 = new HashMap<>();
        colStat3.put("col1", cs3);
        colStat3.put("col2", cs4);

        ObjectStatisticsImpl os4 = new ObjectStatisticsImpl(99, colStat3);

        ObjectStatisticsImpl sumStat3 = IgniteStatisticsRepositoryImpl.add(os1, os4);

        assertEquals(99, sumStat3.rowCount());
        assertEquals(2, sumStat3.columnsStatistics().size());
        assertEquals(cs3, sumStat3.columnStatistics("col1"));
    }

    /**
     * 1) Remove not existing column.
     * 2) Remove some columns.
     * 3) Remove all columns.
     */
    @Test
    public void subtractTest() {
        HashMap<String, ColumnStatistics> colStat1 = new HashMap<>();
        colStat1.put("col1", cs1);
        colStat1.put("col2", cs2);

        ObjectStatisticsImpl os = new ObjectStatisticsImpl(100, colStat1);

        // 1) Remove not existing column.
        ObjectStatisticsImpl os1 = IgniteStatisticsRepositoryImpl.subtract(os, new String[]{"col0"});

        assertEquals(os, os1);

        // 2) Remove some columns.
        ObjectStatisticsImpl os2 = IgniteStatisticsRepositoryImpl.subtract(os, new String[]{"col1"});

        assertEquals(1, os2.columnsStatistics().size());
        assertEquals(cs2, os2.columnStatistics("col2"));

        // 3) Remove all columns.
        ObjectStatisticsImpl os3 = IgniteStatisticsRepositoryImpl.subtract(os, new String[]{"col2","col1"});

        assertTrue(os3.columnsStatistics().isEmpty());
    }
}
