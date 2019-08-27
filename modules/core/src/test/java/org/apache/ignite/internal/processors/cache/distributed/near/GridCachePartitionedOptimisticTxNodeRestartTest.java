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

package org.apache.ignite.internal.processors.cache.distributed.near;

import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.processors.cache.distributed.GridCacheAbstractNodeRestartSelfTest;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.junit.Test;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;

/**
 * Test node restart.
 */
public class GridCachePartitionedOptimisticTxNodeRestartTest extends GridCacheAbstractNodeRestartSelfTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(igniteInstanceName);

        c.getTransactionConfiguration().setDefaultTxConcurrency(OPTIMISTIC);

        return c;
    }

    /** {@inheritDoc} */
    @Override protected CacheConfiguration cacheConfiguration() {
        CacheConfiguration cc = defaultCacheConfiguration();

        cc.setName(CACHE_NAME);
        cc.setCacheMode(PARTITIONED);
        cc.setWriteSynchronizationMode(FULL_SYNC);
        cc.setRebalanceMode(rebalancMode);
        cc.setRebalanceBatchSize(rebalancBatchSize);
        cc.setAffinity(new RendezvousAffinityFunction(false, partitions));
        cc.setBackups(backups);

        cc.setNearConfiguration(nearEnabled() ? new NearCacheConfiguration() : null);

        return cc;
    }

    /**
     * @return {@code True} if near cache enabled.
     */
    protected boolean nearEnabled() {
        return true;
    }

    /** {@inheritDoc} */
    @Override protected TransactionConcurrency txConcurrency() {
        return OPTIMISTIC;
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testRestart() throws Exception {
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testRestartWithPutTwoNodesNoBackups() throws Throwable {
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testRestartWithPutTwoNodesOneBackup() throws Throwable {
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testRestartWithPutFourNodesNoBackups() throws Throwable {
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testRestartWithPutFourNodesOneBackups() throws Throwable {
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testRestartWithPutSixNodesTwoBackups() throws Throwable {
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testRestartWithPutEightNodesTwoBackups() throws Throwable {
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testRestartWithPutTenNodesTwoBackups() throws Throwable {
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testRestartWithTxEightNodesTwoBackups() throws Throwable {
        super.testRestartWithTxEightNodesTwoBackups();
    }

    @Test
    public void testRestartWithTxEightNodesTwoBackups_0() throws Throwable {
        super.testRestartWithTxEightNodesTwoBackups();
    }
    @Test
    public void testRestartWithTxEightNodesTwoBackups_1() throws Throwable {
        super.testRestartWithTxEightNodesTwoBackups();
    }
    @Test
    public void testRestartWithTxEightNodesTwoBackups_2() throws Throwable {
        super.testRestartWithTxEightNodesTwoBackups();
    }
    @Test
    public void testRestartWithTxEightNodesTwoBackups_3() throws Throwable {
        super.testRestartWithTxEightNodesTwoBackups();
    }
    @Test
    public void testRestartWithTxEightNodesTwoBackups_4() throws Throwable {
        super.testRestartWithTxEightNodesTwoBackups();
    }
    @Test
    public void testRestartWithTxEightNodesTwoBackups_5() throws Throwable {
        super.testRestartWithTxEightNodesTwoBackups();
    }
    @Test
    public void testRestartWithTxEightNodesTwoBackups_6() throws Throwable {
        super.testRestartWithTxEightNodesTwoBackups();
    }
    @Test
    public void testRestartWithTxEightNodesTwoBackups_7() throws Throwable {
        super.testRestartWithTxEightNodesTwoBackups();
    }
    @Test
    public void testRestartWithTxEightNodesTwoBackups_8() throws Throwable {
        super.testRestartWithTxEightNodesTwoBackups();
    }
    @Test
    public void testRestartWithTxEightNodesTwoBackups_9() throws Throwable {
        super.testRestartWithTxEightNodesTwoBackups();
    }
    @Test
    public void testRestartWithTxEightNodesTwoBackups_10() throws Throwable {
        super.testRestartWithTxEightNodesTwoBackups();
    }
    @Test
    public void testRestartWithTxEightNodesTwoBackups_11() throws Throwable {
        super.testRestartWithTxEightNodesTwoBackups();
    }
    @Test
    public void testRestartWithTxEightNodesTwoBackups_12() throws Throwable {
        super.testRestartWithTxEightNodesTwoBackups();
    }
    @Test
    public void testRestartWithTxEightNodesTwoBackups_13() throws Throwable {
        super.testRestartWithTxEightNodesTwoBackups();
    }
    @Test
    public void testRestartWithTxEightNodesTwoBackups_14() throws Throwable {
        super.testRestartWithTxEightNodesTwoBackups();
    }
    @Test
    public void testRestartWithTxEightNodesTwoBackups_15() throws Throwable {
        super.testRestartWithTxEightNodesTwoBackups();
    }
    @Test
    public void testRestartWithTxEightNodesTwoBackups_16() throws Throwable {
        super.testRestartWithTxEightNodesTwoBackups();
    }
    @Test
    public void testRestartWithTxEightNodesTwoBackups_17() throws Throwable {
        super.testRestartWithTxEightNodesTwoBackups();
    }
    @Test
    public void testRestartWithTxEightNodesTwoBackups_18() throws Throwable {
        super.testRestartWithTxEightNodesTwoBackups();
    }
    @Test
    public void testRestartWithTxEightNodesTwoBackups_19() throws Throwable {
        super.testRestartWithTxEightNodesTwoBackups();
    }
    @Test
    public void testRestartWithTxEightNodesTwoBackups_20() throws Throwable {
        super.testRestartWithTxEightNodesTwoBackups();
    }
    @Test
    public void testRestartWithTxEightNodesTwoBackups_21() throws Throwable {
        super.testRestartWithTxEightNodesTwoBackups();
    }
    @Test
    public void testRestartWithTxEightNodesTwoBackups_22() throws Throwable {
        super.testRestartWithTxEightNodesTwoBackups();
    }
    @Test
    public void testRestartWithTxEightNodesTwoBackups_23() throws Throwable {
        super.testRestartWithTxEightNodesTwoBackups();
    }
    @Test
    public void testRestartWithTxEightNodesTwoBackups_24() throws Throwable {
        super.testRestartWithTxEightNodesTwoBackups();
    }
    @Test
    public void testRestartWithTxEightNodesTwoBackups_25() throws Throwable {
        super.testRestartWithTxEightNodesTwoBackups();
    }
    @Test
    public void testRestartWithTxEightNodesTwoBackups_26() throws Throwable {
        super.testRestartWithTxEightNodesTwoBackups();
    }
    @Test
    public void testRestartWithTxEightNodesTwoBackups_27() throws Throwable {
        super.testRestartWithTxEightNodesTwoBackups();
    }
    @Test
    public void testRestartWithTxEightNodesTwoBackups_28() throws Throwable {
        super.testRestartWithTxEightNodesTwoBackups();
    }
    @Test
    public void testRestartWithTxEightNodesTwoBackups_29() throws Throwable {
        super.testRestartWithTxEightNodesTwoBackups();
    }
    @Test
    public void testRestartWithTxEightNodesTwoBackups_30() throws Throwable {
        super.testRestartWithTxEightNodesTwoBackups();
    }
    @Test
    public void testRestartWithTxEightNodesTwoBackups_31() throws Throwable {
        super.testRestartWithTxEightNodesTwoBackups();
    }
    @Test
    public void testRestartWithTxEightNodesTwoBackups_32() throws Throwable {
        super.testRestartWithTxEightNodesTwoBackups();
    }
    @Test
    public void testRestartWithTxEightNodesTwoBackups_33() throws Throwable {
        super.testRestartWithTxEightNodesTwoBackups();
    }
    @Test
    public void testRestartWithTxEightNodesTwoBackups_34() throws Throwable {
        super.testRestartWithTxEightNodesTwoBackups();
    }
    @Test
    public void testRestartWithTxEightNodesTwoBackups_35() throws Throwable {
        super.testRestartWithTxEightNodesTwoBackups();
    }
    @Test
    public void testRestartWithTxEightNodesTwoBackups_36() throws Throwable {
        super.testRestartWithTxEightNodesTwoBackups();
    }
    @Test
    public void testRestartWithTxEightNodesTwoBackups_37() throws Throwable {
        super.testRestartWithTxEightNodesTwoBackups();
    }
    @Test
    public void testRestartWithTxEightNodesTwoBackups_38() throws Throwable {
        super.testRestartWithTxEightNodesTwoBackups();
    }
    @Test
    public void testRestartWithTxEightNodesTwoBackups_39() throws Throwable {
        super.testRestartWithTxEightNodesTwoBackups();
    }
    @Test
    public void testRestartWithTxEightNodesTwoBackups_40() throws Throwable {
        super.testRestartWithTxEightNodesTwoBackups();
    }
    @Test
    public void testRestartWithTxEightNodesTwoBackups_41() throws Throwable {
        super.testRestartWithTxEightNodesTwoBackups();
    }
    @Test
    public void testRestartWithTxEightNodesTwoBackups_42() throws Throwable {
        super.testRestartWithTxEightNodesTwoBackups();
    }
    @Test
    public void testRestartWithTxEightNodesTwoBackups_43() throws Throwable {
        super.testRestartWithTxEightNodesTwoBackups();
    }
    @Test
    public void testRestartWithTxEightNodesTwoBackups_44() throws Throwable {
        super.testRestartWithTxEightNodesTwoBackups();
    }
    @Test
    public void testRestartWithTxEightNodesTwoBackups_45() throws Throwable {
        super.testRestartWithTxEightNodesTwoBackups();
    }
    @Test
    public void testRestartWithTxEightNodesTwoBackups_46() throws Throwable {
        super.testRestartWithTxEightNodesTwoBackups();
    }
    @Test
    public void testRestartWithTxEightNodesTwoBackups_47() throws Throwable {
        super.testRestartWithTxEightNodesTwoBackups();
    }
    @Test
    public void testRestartWithTxEightNodesTwoBackups_48() throws Throwable {
        super.testRestartWithTxEightNodesTwoBackups();
    }
    @Test
    public void testRestartWithTxEightNodesTwoBackups_49() throws Throwable {
        super.testRestartWithTxEightNodesTwoBackups();
    }
    @Test
    public void testRestartWithTxEightNodesTwoBackups_50() throws Throwable {
        super.testRestartWithTxEightNodesTwoBackups();
    }
    @Test
    public void testRestartWithTxEightNodesTwoBackups_51() throws Throwable {
        super.testRestartWithTxEightNodesTwoBackups();
    }
    @Test
    public void testRestartWithTxEightNodesTwoBackups_52() throws Throwable {
        super.testRestartWithTxEightNodesTwoBackups();
    }
    @Test
    public void testRestartWithTxEightNodesTwoBackups_53() throws Throwable {
        super.testRestartWithTxEightNodesTwoBackups();
    }
    @Test
    public void testRestartWithTxEightNodesTwoBackups_54() throws Throwable {
        super.testRestartWithTxEightNodesTwoBackups();
    }
    @Test
    public void testRestartWithTxEightNodesTwoBackups_55() throws Throwable {
        super.testRestartWithTxEightNodesTwoBackups();
    }
    @Test
    public void testRestartWithTxEightNodesTwoBackups_56() throws Throwable {
        super.testRestartWithTxEightNodesTwoBackups();
    }
    @Test
    public void testRestartWithTxEightNodesTwoBackups_57() throws Throwable {
        super.testRestartWithTxEightNodesTwoBackups();
    }
    @Test
    public void testRestartWithTxEightNodesTwoBackups_58() throws Throwable {
        super.testRestartWithTxEightNodesTwoBackups();
    }
    @Test
    public void testRestartWithTxEightNodesTwoBackups_59() throws Throwable {
        super.testRestartWithTxEightNodesTwoBackups();
    }
    @Test
    public void testRestartWithTxEightNodesTwoBackups_60() throws Throwable {
        super.testRestartWithTxEightNodesTwoBackups();
    }
    @Test
    public void testRestartWithTxEightNodesTwoBackups_61() throws Throwable {
        super.testRestartWithTxEightNodesTwoBackups();
    }
    @Test
    public void testRestartWithTxEightNodesTwoBackups_62() throws Throwable {
        super.testRestartWithTxEightNodesTwoBackups();
    }
    @Test
    public void testRestartWithTxEightNodesTwoBackups_63() throws Throwable {
        super.testRestartWithTxEightNodesTwoBackups();
    }
    @Test
    public void testRestartWithTxEightNodesTwoBackups_64() throws Throwable {
        super.testRestartWithTxEightNodesTwoBackups();
    }
    @Test
    public void testRestartWithTxEightNodesTwoBackups_65() throws Throwable {
        super.testRestartWithTxEightNodesTwoBackups();
    }
    @Test
    public void testRestartWithTxEightNodesTwoBackups_66() throws Throwable {
        super.testRestartWithTxEightNodesTwoBackups();
    }
    @Test
    public void testRestartWithTxEightNodesTwoBackups_67() throws Throwable {
        super.testRestartWithTxEightNodesTwoBackups();
    }
    @Test
    public void testRestartWithTxEightNodesTwoBackups_68() throws Throwable {
        super.testRestartWithTxEightNodesTwoBackups();
    }
    @Test
    public void testRestartWithTxEightNodesTwoBackups_69() throws Throwable {
        super.testRestartWithTxEightNodesTwoBackups();
    }
    @Test
    public void testRestartWithTxEightNodesTwoBackups_70() throws Throwable {
        super.testRestartWithTxEightNodesTwoBackups();
    }
    @Test
    public void testRestartWithTxEightNodesTwoBackups_71() throws Throwable {
        super.testRestartWithTxEightNodesTwoBackups();
    }
    @Test
    public void testRestartWithTxEightNodesTwoBackups_72() throws Throwable {
        super.testRestartWithTxEightNodesTwoBackups();
    }
    @Test
    public void testRestartWithTxEightNodesTwoBackups_73() throws Throwable {
        super.testRestartWithTxEightNodesTwoBackups();
    }
    @Test
    public void testRestartWithTxEightNodesTwoBackups_74() throws Throwable {
        super.testRestartWithTxEightNodesTwoBackups();
    }
    @Test
    public void testRestartWithTxEightNodesTwoBackups_75() throws Throwable {
        super.testRestartWithTxEightNodesTwoBackups();
    }
    @Test
    public void testRestartWithTxEightNodesTwoBackups_76() throws Throwable {
        super.testRestartWithTxEightNodesTwoBackups();
    }
    @Test
    public void testRestartWithTxEightNodesTwoBackups_77() throws Throwable {
        super.testRestartWithTxEightNodesTwoBackups();
    }
    @Test
    public void testRestartWithTxEightNodesTwoBackups_78() throws Throwable {
        super.testRestartWithTxEightNodesTwoBackups();
    }
    @Test
    public void testRestartWithTxEightNodesTwoBackups_79() throws Throwable {
        super.testRestartWithTxEightNodesTwoBackups();
    }
    @Test
    public void testRestartWithTxEightNodesTwoBackups_80() throws Throwable {
        super.testRestartWithTxEightNodesTwoBackups();
    }
    @Test
    public void testRestartWithTxEightNodesTwoBackups_81() throws Throwable {
        super.testRestartWithTxEightNodesTwoBackups();
    }
    @Test
    public void testRestartWithTxEightNodesTwoBackups_82() throws Throwable {
        super.testRestartWithTxEightNodesTwoBackups();
    }
    @Test
    public void testRestartWithTxEightNodesTwoBackups_83() throws Throwable {
        super.testRestartWithTxEightNodesTwoBackups();
    }
    @Test
    public void testRestartWithTxEightNodesTwoBackups_84() throws Throwable {
        super.testRestartWithTxEightNodesTwoBackups();
    }
    @Test
    public void testRestartWithTxEightNodesTwoBackups_85() throws Throwable {
        super.testRestartWithTxEightNodesTwoBackups();
    }
    @Test
    public void testRestartWithTxEightNodesTwoBackups_86() throws Throwable {
        super.testRestartWithTxEightNodesTwoBackups();
    }
    @Test
    public void testRestartWithTxEightNodesTwoBackups_87() throws Throwable {
        super.testRestartWithTxEightNodesTwoBackups();
    }
    @Test
    public void testRestartWithTxEightNodesTwoBackups_88() throws Throwable {
        super.testRestartWithTxEightNodesTwoBackups();
    }
    @Test
    public void testRestartWithTxEightNodesTwoBackups_89() throws Throwable {
        super.testRestartWithTxEightNodesTwoBackups();
    }
    @Test
    public void testRestartWithTxEightNodesTwoBackups_90() throws Throwable {
        super.testRestartWithTxEightNodesTwoBackups();
    }
    @Test
    public void testRestartWithTxEightNodesTwoBackups_91() throws Throwable {
        super.testRestartWithTxEightNodesTwoBackups();
    }
    @Test
    public void testRestartWithTxEightNodesTwoBackups_92() throws Throwable {
        super.testRestartWithTxEightNodesTwoBackups();
    }
    @Test
    public void testRestartWithTxEightNodesTwoBackups_93() throws Throwable {
        super.testRestartWithTxEightNodesTwoBackups();
    }
    @Test
    public void testRestartWithTxEightNodesTwoBackups_94() throws Throwable {
        super.testRestartWithTxEightNodesTwoBackups();
    }
    @Test
    public void testRestartWithTxEightNodesTwoBackups_95() throws Throwable {
        super.testRestartWithTxEightNodesTwoBackups();
    }
    @Test
    public void testRestartWithTxEightNodesTwoBackups_96() throws Throwable {
        super.testRestartWithTxEightNodesTwoBackups();
    }
    @Test
    public void testRestartWithTxEightNodesTwoBackups_97() throws Throwable {
        super.testRestartWithTxEightNodesTwoBackups();
    }
    @Test
    public void testRestartWithTxEightNodesTwoBackups_98() throws Throwable {
        super.testRestartWithTxEightNodesTwoBackups();
    }
    @Test
    public void testRestartWithTxEightNodesTwoBackups_99() throws Throwable {
        super.testRestartWithTxEightNodesTwoBackups();
    }


    /** {@inheritDoc} */
    @Test
    @Override public void testRestartWithTxFourNodesNoBackups() throws Throwable {
       // super.testRestartWithTxFourNodesNoBackups();
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testRestartWithTxFourNodesOneBackups() throws Throwable {
        //super.testRestartWithTxFourNodesOneBackups();
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testRestartWithTxSixNodesTwoBackups() throws Throwable {
       // super.testRestartWithTxSixNodesTwoBackups();
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testRestartWithTxTenNodesTwoBackups() throws Throwable {
      //  super.testRestartWithTxTenNodesTwoBackups();
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testRestartWithTxTwoNodesNoBackups() throws Throwable {
       // super.testRestartWithTxTwoNodesNoBackups();
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testRestartWithTxTwoNodesOneBackup() throws Throwable {
       // super.testRestartWithTxTwoNodesOneBackup();
    }
}
