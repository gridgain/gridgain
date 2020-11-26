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

import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

/**
 * Test for statistics repository.
 */
public class IgniteStatisticsRepositoryTest extends GridCommonAbstractTest {
    /** First default key. */
    private static final StatsKey k1 = new StatsKey("PUBLIC", "tab1");

    /** Second default key. */
    private static final StatsKey k2 = new StatsKey("PUBLIC", "tab2");

    /** Third default key. */
    private static final StatsKey k3 = new StatsKey("PUBLIC", "tab3");

    /**
     * Test ignite statistics repository on client node without persistence.
     */
    @Test
    public void testClientNode() {
        IgniteStatisticsRepositoryImpl statsRepos = new IgniteStatisticsRepositoryImpl(false, false,
                null, log);
        testRepositoryGlobal(statsRepos);
    }

    /**
     * Test ignite statistics repository on server node without persistence.
     */
    @Test
    public void testServerWithoutPersistence() {
        IgniteStatisticsRepositoryImpl statsRepos = new IgniteStatisticsRepositoryImpl(true, false,
                null, log);
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
     * @param repository Ignite statistics repository to test.
     */
    public void testRepositoryPartitions(IgniteStatisticsRepositoryImpl repository) {
        ObjectPartitionStatisticsImpl stat1 = getPartitionStatistics(1);
        ObjectPartitionStatisticsImpl stat10 = getPartitionStatistics(10);
        ObjectPartitionStatisticsImpl stat100 = getPartitionStatistics(100);

        ObjectPartitionStatisticsImpl stat1_2 = getPartitionStatistics(1);

        assertTrue(repository.getLocalPartitionsStatistics(k1).isEmpty());
        assertTrue(repository.getLocalPartitionsStatistics(k2).isEmpty());

        repository.saveLocalPartitionStatistics(k1, stat1);
        repository.saveLocalPartitionStatistics(k1, stat10);
        repository.saveLocalPartitionStatistics(k2, stat1_2);

        ObjectPartitionStatisticsImpl stat1Readed = repository.getLocalPartitionStatistics(k1, 1);
        assertNotNull(stat1Readed);
        assertEquals(1, stat1Readed.partId());

        ObjectPartitionStatisticsImpl stat10Readed = repository.getLocalPartitionStatistics(k1, 10);
        assertNotNull(stat10Readed);
        assertEquals(10, stat10Readed.partId());

        assertNull(repository.getLocalPartitionStatistics(k1, 2));

        assertEquals(2, repository.getLocalPartitionsStatistics(k1).size());
        assertEquals(1, repository.getLocalPartitionsStatistics(k2).size());

        repository.saveLocalPartitionsStatistics(k1, Arrays.asList(new ObjectPartitionStatisticsImpl[]{stat10, stat100}));

        assertEquals(3, repository.getLocalPartitionsStatistics(k1).size());
    }

    /**
     * Test specified repository with local statistics.
     *
     * @param repository Ignite statistics repository to test.
     */
    public void testRepositoryLocal(IgniteStatisticsRepositoryImpl repository) {
        assertNull(repository.getLocalStatistics(k1));
        assertNull(repository.getLocalStatistics(k2));

        ObjectStatisticsImpl stat1 = getStatistics(1);

        repository.saveLocalStatistics(k1, stat1);
        assertNull(repository.getLocalStatistics(k2));

        assertEquals(1L, repository.getLocalStatistics(k1).rowCount());

        ObjectStatisticsImpl stat2 = getStatistics(2);

        repository.mergeLocalStatistics(k1, stat2);

        assertNull(repository.getLocalStatistics(k2));
        assertEquals(2L, repository.getLocalStatistics(k1).rowCount());
    }

    /**
     * Test specified repository with global statistics.
     *
     * @param repository Ignite statistics repository to test.
     */
    public void testRepositoryGlobal(IgniteStatisticsRepositoryImpl repository) {
        assertNull(repository.getGlobalStatistics(k1));
        repository.clearGlobalStatistics(k1);
        repository.clearGlobalStatistics(k1, "col10");

        ObjectStatisticsImpl tab1Statistics = getStatistics(1);

        repository.saveGlobalStatistics(k1, tab1Statistics);

        assertNull(repository.getGlobalStatistics(k2));

        assertEquals(1L, repository.getGlobalStatistics(k1).rowCount());

        ObjectStatisticsImpl tab1Statistics2 = getStatistics(2);

        repository.mergeGlobalStatistics(k1, tab1Statistics2);

        assertEquals(2L, repository.getGlobalStatistics(k1).rowCount());
    }

    /**
     * Get object partition statistics.
     *
     * @param partId Partition id.
     * @return Object partition statistics with specified partition id.
     */
    private ObjectPartitionStatisticsImpl getPartitionStatistics(int partId) {
        ColumnStatistics columnStatistics = new ColumnStatistics(null, null,100,0, 100,
                0, new byte[0]);
        return new ObjectPartitionStatisticsImpl(partId, true, 0, 0, Collections
                .singletonMap("col1", columnStatistics));
    }

    /**
     * Get object statistics.
     *
     * @param rowsCnt Rows count.
     * @return Object statistics.
     */
    private ObjectStatisticsImpl getStatistics(long rowsCnt) {
        ColumnStatistics columnStatistics = new ColumnStatistics(null, null,100,0, 100,
                0, new byte[0]);
        return new ObjectStatisticsImpl(rowsCnt, Collections.singletonMap("col1", columnStatistics));
    }
}
