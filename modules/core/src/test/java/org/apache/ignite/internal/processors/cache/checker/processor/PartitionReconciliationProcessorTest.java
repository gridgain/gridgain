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

package org.apache.ignite.internal.processors.cache.checker.processor;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.compute.ComputeTask;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCachePartitionExchangeManager;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.KeyCacheObjectImpl;
import org.apache.ignite.internal.processors.cache.checker.objects.CachePartitionRequest;
import org.apache.ignite.internal.processors.cache.checker.objects.VersionedValue;
import org.apache.ignite.internal.processors.cache.checker.processor.workload.Batch;
import org.apache.ignite.internal.processors.cache.checker.processor.workload.Recheck;
import org.apache.ignite.internal.processors.cache.checker.processor.workload.Repair;
import org.apache.ignite.internal.processors.cache.checker.tasks.CollectPartitionKeysByBatchTask;
import org.apache.ignite.internal.processors.cache.checker.tasks.CollectPartitionKeysByRecheckRequestTask;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsExchangeFuture;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.future.IgniteFinishedFutureImpl;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteInClosure;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.verification.VerificationMode;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

/**
 *
 */
public class PartitionReconciliationProcessorTest {
    /** Default cache. */
    private static final String DEFAULT_CACHE = "default-cache";

    /** Partition id. */
    private static final int PARTITION_ID = 123;

    /**
     *
     */
    private static final int MAX_RECHECK_ATTEPMTS = 3;

    /**
     *
     */
    private static final int MAX_REPAIR_ATTEPMTS = 3;

    /**
     *
     */
    @Test
    public void testBatchDoesNotHaveElementsNothingSchedule() throws IgniteCheckedException {
        MockedProcessor processor = MockedProcessor.create(false);

        T2<KeyCacheObject, Map<KeyCacheObject, Map<UUID, GridCacheVersion>>> emptyRes = new T2<>(null, new HashMap<>());

        processor.addTask(new Batch(DEFAULT_CACHE, PARTITION_ID, null))
            .whereResult(CollectPartitionKeysByBatchTask.class, emptyRes)
            .execute();

        processor.verify(never()).schedule(any());
        processor.verify(never()).schedule(any(), anyInt(), any());
        processor.verify(never()).scheduleHighPriority(any());
    }

    /**
     *
     */
    @Test
    public void testBatchHasElementsRecheckAndNextBatchShouldSchedule() throws IgniteCheckedException {
        MockedProcessor processor = MockedProcessor.create(false);

        KeyCacheObject nextKey = new KeyCacheObjectImpl(1, null, PARTITION_ID);
        Map<KeyCacheObject, Map<UUID, GridCacheVersion>> batchRes = new HashMap<>();
        batchRes.put(nextKey, new HashMap<>());
        T2<KeyCacheObject, Map<KeyCacheObject, Map<UUID, GridCacheVersion>>> emptyRes = new T2<>(nextKey, batchRes);

        processor.addTask(new Batch(DEFAULT_CACHE, PARTITION_ID, null))
            .whereResult(CollectPartitionKeysByBatchTask.class, emptyRes)
            .execute();

        processor.verify(times(1)).schedule(any(Batch.class));
        processor.verify(times(1)).schedule(any(Recheck.class), eq(10L), eq(SECONDS));
    }

    /**
     *
     */
    @Test
    public void testRecheckShouldFinishWithoutActionIfResultEmpty() throws IgniteCheckedException {
        MockedProcessor processor = MockedProcessor.create(false);

        Map<KeyCacheObject, Map<UUID, GridCacheVersion>> batchRes = new HashMap<>();
        batchRes.put(new KeyCacheObjectImpl(1, null, PARTITION_ID), new HashMap<>());

        Map<KeyCacheObject, Map<UUID, VersionedValue>> emptyRes = new HashMap<>();

        processor.addTask(new Recheck(batchRes, DEFAULT_CACHE, PARTITION_ID, 0, 0))
            .whereResult(CollectPartitionKeysByRecheckRequestTask.class, emptyRes)
            .execute();

        processor.verify(never()).schedule(any());
        processor.verify(never()).schedule(any(), anyInt(), any());
        processor.verify(never()).scheduleHighPriority(any());
    }


    /**
     *
     */
    @Test
    public void testRecheckShouldFinishWithoutActionIfConflictWasSolved() throws IgniteCheckedException {
        MockedProcessor processor = MockedProcessor.create(false);

        KeyCacheObjectImpl key = new KeyCacheObjectImpl(1, null, PARTITION_ID);
        UUID nodeId1 = UUID.randomUUID();
        UUID nodeId2 = UUID.randomUUID();
        GridCacheVersion ver = new GridCacheVersion(1, 0, 0, 0);

        Map<KeyCacheObject, Map<UUID, GridCacheVersion>> batchRes = new HashMap<>();
        Map<UUID, GridCacheVersion> oldKeys = new HashMap<>();
        oldKeys.put(nodeId1, ver);
        oldKeys.put(nodeId2, ver);
        batchRes.put(key, oldKeys);

        Map<KeyCacheObject, Map<UUID, VersionedValue>> sameRes = new HashMap<>();
        Map<UUID, VersionedValue> actualKey = new HashMap<>();
        actualKey.put(nodeId1, new VersionedValue(null, ver, 1, 1));
        actualKey.put(nodeId2, new VersionedValue(null, ver, 1, 1));
        sameRes.put(key, actualKey);

        processor.addTask(new Recheck(batchRes, DEFAULT_CACHE, PARTITION_ID, 0, 0))
            .whereResult(CollectPartitionKeysByRecheckRequestTask.class, sameRes)
            .execute();

        processor.verify(never()).schedule(any());
        processor.verify(never()).schedule(any(), anyInt(), any());
        processor.verify(never()).scheduleHighPriority(any());
    }

    /**
     *
     */
    @Test
    public void testRecheckShouldTryAgainIfConflictAndAttemptsExist() throws IgniteCheckedException {
        MockedProcessor processor = MockedProcessor.create(false);

        KeyCacheObjectImpl key = new KeyCacheObjectImpl(1, null, PARTITION_ID);
        UUID nodeId1 = UUID.randomUUID();
        UUID nodeId2 = UUID.randomUUID();
        GridCacheVersion ver = new GridCacheVersion(1, 0, 0, 0);
        GridCacheVersion ver2 = new GridCacheVersion(2, 0, 0, 0);

        Map<KeyCacheObject, Map<UUID, GridCacheVersion>> batchRes = new HashMap<>();
        Map<UUID, GridCacheVersion> oldKeys = new HashMap<>();
        oldKeys.put(nodeId1, ver);
        oldKeys.put(nodeId2, ver2);
        batchRes.put(key, oldKeys);

        Map<KeyCacheObject, Map<UUID, VersionedValue>> sameRes = new HashMap<>();
        Map<UUID, VersionedValue> actualKey = new HashMap<>();
        actualKey.put(nodeId1, new VersionedValue(null, ver, 1, 1));
        actualKey.put(nodeId2, new VersionedValue(null, ver2, 1, 1));
        sameRes.put(key, actualKey);

        processor.addTask(new Recheck(batchRes, DEFAULT_CACHE, PARTITION_ID, 0, 0))
            .whereResult(CollectPartitionKeysByRecheckRequestTask.class, sameRes)
            .execute();

        processor.verify(times(1)).schedule(any(Recheck.class), eq(10L), eq(SECONDS));
    }

    /**
     *
     */
    @Test
    public void testRecheckShouldTryRepairIfAttemptsDoesNotExist() throws IgniteCheckedException {
        MockedProcessor processor = MockedProcessor.create(true);

        KeyCacheObjectImpl key = new KeyCacheObjectImpl(1, null, PARTITION_ID);
        UUID nodeId1 = UUID.randomUUID();
        UUID nodeId2 = UUID.randomUUID();
        GridCacheVersion ver = new GridCacheVersion(1, 0, 0, 0);
        GridCacheVersion ver2 = new GridCacheVersion(2, 0, 0, 0);

        Map<KeyCacheObject, Map<UUID, GridCacheVersion>> batchRes = new HashMap<>();
        Map<UUID, GridCacheVersion> oldKeys = new HashMap<>();
        oldKeys.put(nodeId1, ver);
        oldKeys.put(nodeId2, ver2);
        batchRes.put(key, oldKeys);

        Map<KeyCacheObject, Map<UUID, VersionedValue>> sameRes = new HashMap<>();
        Map<UUID, VersionedValue> actualKey = new HashMap<>();
        actualKey.put(nodeId1, new VersionedValue(null, ver, 1, 1));
        actualKey.put(nodeId2, new VersionedValue(null, ver2, 1, 1));
        sameRes.put(key, actualKey);

        processor.addTask(new Recheck(batchRes, DEFAULT_CACHE, PARTITION_ID, MAX_RECHECK_ATTEPMTS, MAX_REPAIR_ATTEPMTS))
            .whereResult(CollectPartitionKeysByRecheckRequestTask.class, sameRes)
            .execute();

        processor.verify(times(1)).scheduleHighPriority(any(Repair.class));
    }

    /**
     *
     */
    private static class MockedProcessor extends PartitionReconciliationProcessor {
        /**
         *
         */
        private final AbstractPipelineProcessor mock = Mockito.mock(AbstractPipelineProcessor.class);

        /**
         *
         */
        private final ConcurrentMap<Class, Object> computeResults = new ConcurrentHashMap<>();

        /**
         *
         */
        public static MockedProcessor create(boolean fixMode) throws IgniteCheckedException {
            GridCachePartitionExchangeManager exchMgr = Mockito.mock(GridCachePartitionExchangeManager.class);
            GridDhtPartitionsExchangeFuture fut = Mockito.mock(GridDhtPartitionsExchangeFuture.class);

            when(fut.get()).thenReturn(new AffinityTopologyVersion());
            when(exchMgr.lastTopologyFuture()).thenReturn(fut);
            when(exchMgr.lastAffinityChangedTopologyVersion(any())).thenReturn(new AffinityTopologyVersion());

            IgniteEx igniteMock = Mockito.mock(IgniteEx.class);

            when(igniteMock.log()).thenReturn(Mockito.mock(IgniteLogger.class));

            return new MockedProcessor(igniteMock, exchMgr, Collections.emptyList(), fixMode, 0,
                10, MAX_RECHECK_ATTEPMTS, MAX_REPAIR_ATTEPMTS);
        }

        /**
         *
         */
        public MockedProcessor(IgniteEx ignite,
            GridCachePartitionExchangeManager<Object, Object> exchMgr,
            Collection<String> caches, boolean fixMode, int throttlingIntervalMillis, int batchSize,
            int recheckAttempts, int repairAttempts) throws IgniteCheckedException {
            super(ignite, exchMgr, caches, fixMode, throttlingIntervalMillis, batchSize, recheckAttempts, repairAttempts);
        }

        /** {@inheritDoc} */
        @Override
        protected <T extends CachePartitionRequest, R> void compute(Class<? extends ComputeTask<T, R>> taskCls, T arg,
            IgniteInClosure<? super IgniteFuture<R>> lsnr) throws InterruptedException {
            R res = (R)computeResults.get(taskCls);

            if (res == null)
                throw new IllegalStateException("Please add result for: " + taskCls.getSimpleName());

            lsnr.apply(new IgniteFinishedFutureImpl<>(res));
        }

        /** {@inheritDoc} */
        @Override protected void scheduleHighPriority(PipelineWorkload task) {
            mock.scheduleHighPriority(task);
        }

        /** {@inheritDoc} */
        @Override protected void schedule(PipelineWorkload task) {
            mock.schedule(task);
        }

        /** {@inheritDoc} */
        @Override protected void schedule(PipelineWorkload task, long duration, TimeUnit timeUnit) {
            mock.schedule(task, duration, timeUnit);
        }

        /**
         *
         */
        public MockedProcessor addTask(PipelineWorkload workload) {
            super.schedule(workload, 0, MILLISECONDS);

            return this;
        }

        /**
         *
         */
        public <T extends CachePartitionRequest, R> MockedProcessor whereResult(
            Class<? extends ComputeTask<T, R>> taskCls, R res) {
            computeResults.put(taskCls, res);

            return this;
        }

        /**
         *
         */
        public AbstractPipelineProcessor verify(VerificationMode times) {
            return Mockito.verify(mock, times);
        }
    }
}