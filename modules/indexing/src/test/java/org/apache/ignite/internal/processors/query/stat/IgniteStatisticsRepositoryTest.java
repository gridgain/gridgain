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
import org.junit.Test;
import org.mockito.Mockito;
import java.util.Arrays;

/**
 * Test for statistics repository.
 */
public class IgniteStatisticsRepositoryTest extends StatisticsAbstractTest {
    /** First default key. */
    private static final StatsKey K1 = new StatsKey("PUBLIC", "tab1");

    /** Second default key. */
    private static final StatsKey K2 = new StatsKey("PUBLIC", "tab2");

    /**
     * Test ignite statistics repository on client node without persistence.
     */
    @Test
    public void testClientNode() {
        IgniteStatisticsRepositoryImpl statsRepos = new IgniteStatisticsRepositoryImpl(false, null,
                null, null, cls -> log);

        testRepositoryGlobal(statsRepos);
    }

    /**
     * Test ignite statistics repository on server node without persistence.
     */
    @Test
    public void testServerWithoutPersistence() {
        IgniteStatisticsRepositoryImpl statsRepos = new IgniteStatisticsRepositoryImpl(true, null,
                null, null, cls -> log);

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

        IgniteStatisticsRepositoryImpl statsRepos = new IgniteStatisticsRepositoryImpl(true,
                new IgniteCacheDatabaseSharedManager(), subscriptionProcessor, null, cls -> log);

        ReadWriteMetaStorageMock metastorage = new ReadWriteMetaStorageMock();
        lsnr[0].onReadyForReadWrite(metastorage);

        testRepositoryGlobal(statsRepos);
        testRepositoryLocal(statsRepos);
        testRepositoryPartitions(statsRepos);
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
}
