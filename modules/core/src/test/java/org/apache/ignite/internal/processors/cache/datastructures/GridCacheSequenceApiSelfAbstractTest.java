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

package org.apache.ignite.internal.processors.cache.datastructures;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteAtomicSequence;
import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.AtomicConfiguration;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.processors.cache.GridCacheAdapter;
import org.apache.ignite.internal.processors.datastructures.DataStructuresProcessor;
import org.apache.ignite.internal.processors.datastructures.GridCacheInternalKeyImpl;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.transactions.Transaction;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 * Cache sequence basic tests.
 */
public abstract class GridCacheSequenceApiSelfAbstractTest extends IgniteAtomicsAbstractTest {
    /**  */
    protected static final int BATCH_SIZE = 3;

    /** Number of sequences. */
    protected static final int SEQ_NUM = 3;

    /** */
    private static final String TRANSACTIONAL_CACHE_NAME = "tx_cache";

    /** Number of loops in method execution. */
    protected static final int MAX_LOOPS_NUM = 1000;

    /** Number of threads for multi-threaded test. */
    protected static final int THREAD_NUM = 10;

    /** Random number generator. */
    protected static final Random RND = new Random();

    /** Names of mandatory sequences. */
    private static String[] seqNames = new String[SEQ_NUM];

    /** Mandatory sequences. */
    private static IgniteAtomicSequence[] seqArr = new IgniteAtomicSequence[SEQ_NUM];

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 1;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        CacheConfiguration ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

        ccfg.setName(TRANSACTIONAL_CACHE_NAME);
        ccfg.setCacheMode(PARTITIONED);

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected AtomicConfiguration atomicConfiguration() {
        AtomicConfiguration atomicCfg = super.atomicConfiguration();

        atomicCfg.setAtomicSequenceReserveSize(BATCH_SIZE);

        return atomicCfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        //Prepare names of mandatory sequences.
        for (int i = 0; i < SEQ_NUM; i++)
            seqNames[i] = UUID.randomUUID().toString();

        // Prepare mandatory sequences.
        seqArr[0] = grid().atomicSequence(seqNames[0], 0, true);
        seqArr[1] = grid().atomicSequence(seqNames[1], RND.nextLong(), true);
        seqArr[2] = grid().atomicSequence(seqNames[2], -1 * RND.nextLong(), true);

        // Check and change batch size.
        for (IgniteAtomicSequence seq : seqArr) {
            assert seq != null;

            // Compare with default batch size.
            assertEquals(BATCH_SIZE, seq.batchSize());
        }

        assertEquals(1, G.allGrids().size());
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        // Remove mandatory sequences from cache.
        for (String seqName : seqNames) {
            IgniteAtomicSequence seq = grid().atomicSequence(seqName, 0, false);

            assertNotNull(seq);

            seq.close();

            assertNull(grid().atomicSequence(seqName, 0, false));
        }
    }

    /** {@inheritDoc} */
    @Override protected IgniteEx grid() {
        return grid(0);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPrepareSequence() throws Exception {
        // Random sequence names.
        String locSeqName1 = UUID.randomUUID().toString();
        String locSeqName2 = UUID.randomUUID().toString();

        IgniteAtomicSequence locSeq1 = grid().atomicSequence(locSeqName1, 0, true);
        IgniteAtomicSequence locSeq2 = grid().atomicSequence(locSeqName2, 0, true);
        IgniteAtomicSequence locSeq3 = grid().atomicSequence(locSeqName1, 0, true);

        assertNotNull(locSeq1);
        assertNotNull(locSeq2);
        assertNotNull(locSeq3);

        assert locSeq1.equals(locSeq3);
        assert locSeq3.equals(locSeq1);
        assert !locSeq3.equals(locSeq2);

        removeSequence(locSeqName1);
        removeSequence(locSeqName2);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testAddWrongValue() throws Exception {
        for (IgniteAtomicSequence seq : seqArr) {
            try {
                seq.getAndAdd(-15);

                fail("Exception expected.");
            }
            catch (IllegalArgumentException e) {
                info("Caught expected exception: " + e);
            }

            try {
                seq.addAndGet(-15);

                fail("Exception expected.");
            }
            catch (IllegalArgumentException e) {
                info("Caught expected exception: " + e);
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testGetAndIncrement() throws Exception {
        for (int i = 0; i < MAX_LOOPS_NUM; i++) {
            for (IgniteAtomicSequence seq : seqArr)
                getAndIncrement(seq);

            if (i % 100 == 0)
                info("Finished iteration: " + i);
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testIncrementAndGet() throws Exception {
        for (int i = 0; i < MAX_LOOPS_NUM; i++) {
            for (IgniteAtomicSequence seq : seqArr)
                incrementAndGet(seq);

            if (i % 100 == 0)
                info("Finished iteration: " + i);
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testAddAndGet() throws Exception {
        for (int i = 1; i < MAX_LOOPS_NUM; i++) {
            for (IgniteAtomicSequence seq : seqArr)
                addAndGet(seq, i);

            if (i % 100 == 0)
                info("Finished iteration: " + i);
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testGetAndAdd() throws Exception {
        for (int i = 1; i < MAX_LOOPS_NUM; i++) {
            for (IgniteAtomicSequence seq : seqArr)
                getAndAdd(seq, i);

            if (i % 100 == 0)
                info("Finished iteration: " + i);
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testGetAndAddInTx() throws Exception {
        try (Transaction tx = grid().transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            for (int i = 1; i < MAX_LOOPS_NUM; i++) {
                for (IgniteAtomicSequence seq : seqArr)
                    getAndAdd(seq, i);

                if (i % 100 == 0)
                    info("Finished iteration: " + i);
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSequenceIntegrity0() throws Exception {
        // Random sequence names.
        String locSeqName1 = UUID.randomUUID().toString();
        String locSeqName2 = UUID.randomUUID().toString();

        // Sequence.
        IgniteAtomicSequence locSeq1 = grid().atomicSequence(locSeqName1, 0, true);

        locSeq1.batchSize(1);

        // Sequence.
        long initVal = -1500;

        IgniteAtomicSequence locSeq2 = grid().atomicSequence(locSeqName2, initVal, true);

        locSeq2.batchSize(7);

        // Compute sequence value manually and compare with sequence value.
        for (int i = 0; i < MAX_LOOPS_NUM; i++) {
            integrity(locSeq1, i * 4);
            integrity(locSeq2, (i * 4) + initVal);

            if (i % 100 == 0)
                info("Finished iteration: " + i);
        }

        removeSequence(locSeqName1);
        removeSequence(locSeqName2);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSequenceIntegrity1() throws Exception {
        sequenceIntegrity(1, 0);
        sequenceIntegrity(7, -1500);
        sequenceIntegrity(3, 345);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testMultiThreadedSequenceIntegrity() throws Exception {
        multiThreadedSequenceIntegrity(1, 0);
        multiThreadedSequenceIntegrity(7, -1500);
        multiThreadedSequenceIntegrity(3, 345);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testRemove() throws Exception {
        String locSeqName = UUID.randomUUID().toString();

        IgniteAtomicSequence seq = grid().atomicSequence(locSeqName, 0, true);

        seq.addAndGet(153);

        seq.close();

        try {
            seq.addAndGet(153);

            fail("Exception expected.");
        }
        catch (IgniteException e) {
            info("Caught expected exception: " + e);
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCacheSets() throws Exception {
        // Make new atomic sequence in cache.
        IgniteAtomicSequence seq = grid().atomicSequence(UUID.randomUUID().toString(), 0, true);

        seq.incrementAndGet();

        final String cacheName = DataStructuresProcessor.ATOMICS_CACHE_NAME + "@default-ds-group";

        GridCacheAdapter cache = ((IgniteKernal)grid()).internalCache(cacheName);

        assertNotNull(cache);

        GridTestUtils.assertThrows(log, new Callable<Void>() {
            @Override public Void call() throws Exception {
                grid().cache(cacheName);

                return null;
            }
        }, IllegalStateException.class, null);

        for (String seqName : seqNames)
            assert null != cache.get(new GridCacheInternalKeyImpl(seqName, "default-ds-group"));
    }

    /**
     * Tests that basic API works correctly when there are multiple structures in multiple groups.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testMultipleStructuresInDifferentGroups() throws Exception {
        Ignite ignite = grid(0);

        AtomicConfiguration cfg = new AtomicConfiguration().setGroupName("grp1");

        IgniteAtomicSequence seq1 = ignite.atomicSequence("seq1", 1, true);
        IgniteAtomicSequence seq2 = ignite.atomicSequence("seq2", 2, true);
        IgniteAtomicSequence seq3 = ignite.atomicSequence("seq3", cfg, 3, true);
        IgniteAtomicSequence seq4 = ignite.atomicSequence("seq4", cfg, 4, true);

        assertNull(ignite.atomicSequence("seq1", cfg, 1, false));
        assertNull(ignite.atomicSequence("seq2", cfg, 1, false));
        assertNull(ignite.atomicSequence("seq3", 1, false));
        assertNull(ignite.atomicSequence("seq4", 1, false));

        assertEquals(11, seq1.addAndGet(10));
        assertEquals(12, seq2.addAndGet(10));
        assertEquals(13, seq3.addAndGet(10));
        assertEquals(14, seq4.addAndGet(10));

        seq2.close();
        seq4.close();

        assertTrue(seq2.removed());
        assertTrue(seq4.removed());

        assertNull(ignite.atomicSequence("seq2", 2, false));
        assertNull(ignite.atomicSequence("seq4", cfg, 4, false));

        assertFalse(seq1.removed());
        assertFalse(seq3.removed());

        assertNotNull(ignite.atomicSequence("seq1", 1, false));
        assertNotNull(ignite.atomicSequence("seq3", cfg, 3, false));
    }

    /**
     * Tests that reserveSize value from explicit configuration takes preference.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testSequenceReserveSizeFromExplicitConfiguration() throws Exception {
        Ignite ignite = grid(0);

        IgniteAtomicSequence seq = ignite.atomicSequence("seq",
            new AtomicConfiguration().setAtomicSequenceReserveSize(BATCH_SIZE + 1), 0, true);

        assertEquals(BATCH_SIZE + 1, seq.batchSize());
    }

    /**
     * Sequence get and increment.
     *
     * @param seq Sequence for test.
     * @throws Exception If failed.
     * @return Result of operation.
     */
    private long getAndIncrement(IgniteAtomicSequence seq) throws Exception {
        long locSeqVal = seq.get();

        assertEquals(locSeqVal, seq.getAndIncrement());

        assertEquals(locSeqVal + 1, seq.get());

        return seq.get();
    }

    /**
     * Sequence add and increment
     *
     * @param seq Sequence for test.
     * @throws Exception If failed.
     * @return Result of operation.
     */
    private long incrementAndGet(IgniteAtomicSequence seq) throws Exception {
        long locSeqVal = seq.get();

        assertEquals(locSeqVal + 1, seq.incrementAndGet());

        assertEquals(locSeqVal + 1, seq.get());

        return seq.get();
    }

    /**
     * Sequence add and get.
     *
     * @param seq Sequence for test.
     * @param l Number of added elements.
     * @throws Exception If failed.
     * @return Result of operation.
     */
    private long addAndGet(IgniteAtomicSequence seq, long l) throws Exception {
        long locSeqVal = seq.get();

        assertEquals(locSeqVal + l, seq.addAndGet(l));

        assertEquals(locSeqVal + l, seq.get());

        return seq.get();
    }

    /**
     * Sequence add and get.
     *
     * @param seq Sequence for test.
     * @param l Number of added elements.
     * @throws Exception If failed.
     * @return Result of operation.
     */
    private long getAndAdd(IgniteAtomicSequence seq, long l) throws Exception {
        long locSeqVal = seq.get();

        assertEquals(locSeqVal, seq.getAndAdd(l));

        assertEquals(locSeqVal + l, seq.get());

        return seq.get();
    }

    /**
     *  Sequence integrity.
     *
     * @param batchSize Sequence batch size.
     * @param initVal  Sequence initial value.
     * @throws Exception If test fail.
     */
    private void sequenceIntegrity(int batchSize, long initVal) throws Exception {
        // Random sequence names.
        String locSeqName = UUID.randomUUID().toString();

        // Sequence.
        IgniteAtomicSequence locSeq = grid().atomicSequence(locSeqName, initVal, true);

        locSeq.batchSize(batchSize);

        // Result set.
        Collection<Long> resSet = new HashSet<>();

        // Get sequence value and try to put it result set.
        for (int i = 0; i < MAX_LOOPS_NUM; i++) {
            Long val = locSeq.getAndIncrement();

            assert resSet.add(val) : "Element already in set : " + val;
        }

        assert resSet.size() == MAX_LOOPS_NUM;

        for (long i = initVal; i < MAX_LOOPS_NUM + initVal; i++)
            assert resSet.contains(i) : "Element is absent in set : " + i;

        removeSequence(locSeqName);
    }

    /**
     *  Multi-threaded integrity.
     *
     * @param batchSize Sequence batch size.
     * @param initVal  Sequence initial value.
     * @throws Exception If test fail.
     */
    private void multiThreadedSequenceIntegrity(int batchSize, long initVal) throws Exception {
        // Random sequence names.
        String locSeqName = UUID.randomUUID().toString();

        // Sequence.
        final IgniteAtomicSequence locSeq = grid().atomicSequence(locSeqName, initVal,
            true);

        locSeq.batchSize(batchSize);

        // Result set.
        final Set<Long> resSet = Collections.synchronizedSet(new HashSet<Long>());

        // Get sequence value and try to put it result set.
        for (int i = 0; i < MAX_LOOPS_NUM; i++) {
            Long val = locSeq.getAndIncrement();

            assert !resSet.contains(val) : "Element already in set : " + val;

            resSet.add(val);

            if (i % 100 == 0)
                info("Finished iteration 1: " + i);
        }

        // Work with sequences in many threads.
        multithreaded(
            new Callable() {
                @Nullable @Override public Object call() throws Exception {
                    // Get sequence value and try to put it result set.
                    for (int i = 0; i < MAX_LOOPS_NUM; i++) {
                        Long val = locSeq.getAndIncrement();

                        assert !resSet.contains(val) : "Element already in set : " + val;

                        resSet.add(val);
                    }

                    return null;
                }
            }, THREAD_NUM);

        // Get sequence value and try to put it result set.
        for (int i = 0; i < MAX_LOOPS_NUM; i++) {
            Long val = locSeq.getAndIncrement();

            assert !resSet.contains(val) : "Element already in set : " + val;

            resSet.add(val);

            if (i % 100 == 0)
                info("Finished iteration 2: " + i);
        }

        assert resSet.size() == MAX_LOOPS_NUM * (THREAD_NUM + 2);

        for (long i = initVal; i < MAX_LOOPS_NUM * (THREAD_NUM + 2) + initVal; i++) {
            assert resSet.contains(i) : "Element is absent in set : " + i;

            if (i % 100 == 0)
                info("Finished iteration 3: " + i);
        }

        removeSequence(locSeqName);
    }

    /**
     * Test sequence integrity.
     *
     * @param seq Sequence for test.
     * @param calcVal Manually calculated value.
     * @throws Exception If failed.
     */
    private void integrity(IgniteAtomicSequence seq, long calcVal) throws Exception {
        assert calcVal == seq.get();

        getAndAdd(seq, 1);

        assert calcVal + 1 == seq.get();

        addAndGet(seq, 1);

        assert calcVal + 2 == seq.get();

        getAndIncrement(seq);

        assert calcVal + BATCH_SIZE == seq.get();

        incrementAndGet(seq);

        assert calcVal + 4 == seq.get();
    }

    /**
     * @param name Sequence name.
     * @throws Exception If failed.
     */
    private void removeSequence(String name) throws Exception {
        IgniteAtomicSequence seq = grid().atomicSequence(name, 0, false);

        assertNotNull(seq);

        seq.close();

        assertNull(grid().atomicSequence(name, 0, false));
    }
}
