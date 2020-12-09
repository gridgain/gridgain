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
import org.apache.ignite.internal.processors.metastorage.persistence.ReadWriteMetaStorageMock;
import org.apache.ignite.internal.processors.subscription.GridInternalSubscriptionProcessor;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 * Test statistics storage itself for restart cases.
 */
public class StatisticsStorageRestartTest extends StatisticsAbstractTest {
    /** Default subscription processor mock. */
    private GridInternalSubscriptionProcessor subscriptionProcessor;

    /** Default metastorage mock. */
    private ReadWriteMetaStorageMock metastorage;

    /** Default statistics store. */
    private IgniteStatisticsPersistenceStoreImpl statStore;

    /** Default statistics repository mock. */
    private IgniteStatisticsRepositoryImpl statsRepos;

    /** All default statistics repository.cacheLocalStatistics arguments. */
    private List<Object[]> cacheArguments = new ArrayList<>();

    /** Test statistics key1. */
    private StatsKey k1 = new StatsKey("A", "B");

    /** Test object partition statistics 1_1. */
    private ObjectPartitionStatisticsImpl stat1_1 = getPartitionStatistics(1);

    /** Test statistics key2. */
    private StatsKey k2 = new StatsKey("A", "B2");

    /** Test object partition statistics 2_2. */
    private ObjectPartitionStatisticsImpl stat2_2 = getPartitionStatistics(2);

    /** Test object partition statistics 2_3. */
    private ObjectPartitionStatisticsImpl stat2_3 = getPartitionStatistics(3);

    /** {@inheritDoc} */
    @Override public void beforeTest() {
        subscriptionProcessor = Mockito.mock(GridInternalSubscriptionProcessor.class);
        statsRepos = Mockito.mock(IgniteStatisticsRepositoryImpl.class);
        Mockito.doAnswer(ans -> cacheArguments.add(ans.getArguments())).when(statsRepos)
                .cacheLocalStatistics(Mockito.any(StatsKey.class), Mockito.anyCollection());
        metastorage = new ReadWriteMetaStorageMock();
        statStore = new IgniteStatisticsPersistenceStoreImpl(subscriptionProcessor,
                new IgniteCacheDatabaseSharedManager(){}, statsRepos, cls -> log);
    }

    /**
     * Save statistics to metastore throw one statistics metastore and then attach new statistics store to the same
     * metastorage:
     *
     * 1) create metastorage mock and pass it to statistics storage.
     * 2) save partition level statistics.
     * 3) create new statistics storage on the top of the same metastorage mock
     * 4) check that statistics storage will call statistics repository to cache partition level statistics
     * 5) check that statistics storage will read the same partition statistics
     *
     * @throws IgniteCheckedException In case of errors.
     */
    @Test
    public void testRestart() throws IgniteCheckedException {
        statStore.onReadyForReadWrite(metastorage);

        statStore.saveLocalPartitionStatistics(k1, stat1_1);
        statStore.replaceLocalPartitionsStatistics(k2, Arrays.asList(stat2_2, stat2_3));

        assertEquals(1, statStore.getLocalPartitionsStatistics(k1).size());
        assertEquals(stat1_1, statStore.getLocalPartitionStatistics(k1, 1));
        assertEquals(2, statStore.getLocalPartitionsStatistics(k2).size());
        assertEquals(stat2_2, statStore.getLocalPartitionStatistics(k2, 2));
        assertEquals(stat2_3, statStore.getLocalPartitionStatistics(k2, 3));

        IgniteStatisticsPersistenceStoreImpl statStore2 = new IgniteStatisticsPersistenceStoreImpl(subscriptionProcessor,
                new IgniteCacheDatabaseSharedManager(){}, statsRepos, cls -> log);

        statStore2.onReadyForReadWrite(metastorage);

        assertEquals(1, statStore2.getLocalPartitionsStatistics(k1).size());
        assertEquals(stat1_1, statStore2.getLocalPartitionStatistics(k1, 1));
        assertEquals(2, statStore2.getLocalPartitionsStatistics(k2).size());
        assertEquals(stat2_2, statStore2.getLocalPartitionStatistics(k2, 2));
        assertEquals(stat2_3, statStore2.getLocalPartitionStatistics(k2, 3));

        assertEquals(2, cacheArguments.size());
        assertNotNull(cacheArguments.stream().filter(args -> k1.equals(args[0]) && stat1_1.equals(
                ((Collection<ObjectPartitionStatisticsImpl>)args[1]).iterator().next())).findAny().orElse(null));
        assertNotNull(cacheArguments.stream().filter(args -> k2.equals(args[0]) && stat2_2.equals(
                ((Collection<ObjectPartitionStatisticsImpl>)args[1]).iterator().next())).findAny().orElse(null));
    }

    /**
     * Test that after any deserialization error during startup reads all statistics for object will be deleted:
     *
     * 1) put into metastore some statistics for two objects.
     * 2) put into metastore some broken (A) value into statistics obj1 prefix
     * 3) put into metastore some object (B) outside the statistics prefix.
     * 4) start statistics store with such metastore
     * 5) check that log will contain error message and that object A will be deleted from metastore (with all objects
     * partition statistics), while
     * object C won't be deleted.
     *
     * @throws IgniteCheckedException In case of errors.
     */
    @Test
    public void testUnreadableStatistics() throws IgniteCheckedException {
        statStore.onReadyForReadWrite(metastorage);
        statStore.saveLocalPartitionStatistics(k1, stat1_1);
        statStore.replaceLocalPartitionsStatistics(k2, Arrays.asList(stat2_2, stat2_3));

        String statKey = String.format("stats.%s.%s.%d", k1.schema(), k1.obj(), 1000);
        metastorage.write(statKey, new byte[2]);

        String outerStatKey = "some.key.1";
        byte[] outerStatValue = new byte[] {1, 2};
        metastorage.write(outerStatKey, outerStatValue);

        IgniteStatisticsPersistenceStoreImpl statStore2 = new IgniteStatisticsPersistenceStoreImpl(subscriptionProcessor,
                new IgniteCacheDatabaseSharedManager(){}, statsRepos, cls -> log);
        statStore2.onReadyForReadWrite(metastorage);

        assertEquals(1, cacheArguments.size());
        Object[] args = cacheArguments.get(0);
        assertEquals(k2, args[0]);
        assertEquals(2, ((Collection<ObjectPartitionStatisticsImpl>)args[1]).size());

        assertNull(metastorage.read(statKey));
        assertTrue(statStore.getLocalPartitionsStatistics(k1).isEmpty());
        assertFalse(statStore.getLocalPartitionsStatistics(k2).isEmpty());
        assertTrue(Arrays.equals(outerStatValue, (byte[])metastorage.read(outerStatKey)));
    }
}
