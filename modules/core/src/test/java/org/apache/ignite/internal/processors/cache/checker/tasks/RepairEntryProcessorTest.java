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

package org.apache.ignite.internal.processors.cache.checker.tasks;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheObjectImpl;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.checker.objects.VersionedValue;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 *
 */
public class RepairEntryProcessorTest {
    /**
     *
     */
    private final static String OLD_VALUE = "old_value";

    /**
     *
     */
    private final static String NEW_VALUE = "new_value";

    /**
     * Value at the recheck phase. It uses to check parallel updates.
     */
    private final static String RECHECK_VALUE = OLD_VALUE;

    /** Local node id. */
    private final static UUID LOCAL_NODE_ID = UUID.randomUUID();

    /** Remove queue max size. */
    private final static int RMV_QUEUE_MAX_SIZE = 12;

    /**
     *
     */
    private GridCacheContext cctx;

    /**
     *
     */
    @Before
    public void setUp() throws Exception {
        cctx = mock(GridCacheContext.class);

        when(cctx.localNodeId()).thenReturn(LOCAL_NODE_ID);
    }

    /**
     *
     */
    @Test
    public void testForceRepairApplyRemove() {
        final boolean forceRepair = true;
        Map<UUID, VersionedValue> data = new HashMap<>();

        RepairEntryProcessor repairProcessor = new RepairEntryProcessorStub(
            null,
            data,
            RMV_QUEUE_MAX_SIZE,
            forceRepair,
            new AffinityTopologyVersion(1)
        );

        MutableEntry entry = mock(MutableEntry.class);

        assertTrue((Boolean)repairProcessor.process(entry));

        verify(entry, times(1)).remove();
    }

    /**
     *
     */
    @Test
    public void testForceRepairApplyValue() {
        final boolean forceRepair = true;

        RepairEntryProcessor repairProcessor = new RepairEntryProcessorStub(
            NEW_VALUE,
            new HashMap<>(),
            RMV_QUEUE_MAX_SIZE,
            forceRepair,
            new AffinityTopologyVersion(1)
        );

        MutableEntry entry = mock(MutableEntry.class);

        assertTrue((Boolean)repairProcessor.process(entry));

        verify(entry, times(1)).setValue(NEW_VALUE);
    }

    /**
     *
     */
    @Test
    public void testSetValueWithoutParallelUpdateWhereCurrentRecheckNotNull() {
        Map<UUID, VersionedValue> data = new HashMap<>();
        data.put(LOCAL_NODE_ID, new VersionedValue(
            new CacheObjectImpl(RECHECK_VALUE, RECHECK_VALUE.getBytes()),
            new GridCacheVersion(1, 1, 1),
            1,
            1
        ));

        RepairEntryProcessor repairProcessor = new RepairEntryProcessorStub(
            NEW_VALUE,
            data,
            RMV_QUEUE_MAX_SIZE,
            false,
            new AffinityTopologyVersion(1)
        ).setKeyVersion(new GridCacheVersion(1, 1, 1));

        MutableEntry entry = mock(MutableEntry.class);

        assertTrue((Boolean)repairProcessor.process(entry));

        verify(entry, times(1)).setValue(NEW_VALUE);
    }

    /**
     *
     */
    @Test
    public void testRemoveValueWithoutParallelUpdateWhereCurrentRecheckNotNull() {
        Map<UUID, VersionedValue> data = new HashMap<>();
        data.put(LOCAL_NODE_ID, new VersionedValue(
            new CacheObjectImpl(RECHECK_VALUE, RECHECK_VALUE.getBytes()),
            new GridCacheVersion(1, 1, 1),
            1,
            1
        ));

        RepairEntryProcessor repairProcessor = new RepairEntryProcessorStub(
            null,
            data,
            RMV_QUEUE_MAX_SIZE,
            false,
            new AffinityTopologyVersion(1)
        ).setKeyVersion(new GridCacheVersion(1, 1, 1));

        MutableEntry entry = mock(MutableEntry.class);

        assertTrue((Boolean)repairProcessor.process(entry));

        verify(entry, times(1)).remove();
    }

    /**
     *
     */
    @Test
    public void testWriteValueParallelUpdateWhereCurrentRecheckNotNull() {
        Map<UUID, VersionedValue> data = new HashMap<>();
        data.put(LOCAL_NODE_ID, new VersionedValue(
            new CacheObjectImpl(RECHECK_VALUE, RECHECK_VALUE.getBytes()),
            new GridCacheVersion(1, 1, 1),
            1,
            1
        ));

        RepairEntryProcessor repairProcessor = new RepairEntryProcessorStub(
            NEW_VALUE,
            data,
            RMV_QUEUE_MAX_SIZE,
            false,
            new AffinityTopologyVersion(1)
        ).setKeyVersion(new GridCacheVersion(2, 2, 2));

        MutableEntry entry = mock(MutableEntry.class);

        assertFalse((Boolean)repairProcessor.process(entry));
    }

    /**
     * It mean that a partition under load.
     */
    @Test
    public void testRecheckVersionNullCurrentValueExist() {
        RepairEntryProcessor repairProcessor = new RepairEntryProcessorStub(
            NEW_VALUE,
            new HashMap<>(),
            RMV_QUEUE_MAX_SIZE,
            false,
            new AffinityTopologyVersion(1)
        ).setKeyVersion(new GridCacheVersion(1, 1, 1));

        MutableEntry entry = mock(MutableEntry.class);

        assertFalse((Boolean)repairProcessor.process(entry));
    }

    /**
     *
     */
    @Test
    public void testRecheckVersionNullAndTtlEntryShouldNotAlreadyRemovedAndNewUpdateCounterLessDelQueueSizeOpRemove() {
        RepairEntryProcessor repairProcessor = new RepairEntryProcessorStub(
            null,
            new HashMap<>(),
            RMV_QUEUE_MAX_SIZE,
            false,
            new AffinityTopologyVersion(1)
        ).setKeyVersion(new GridCacheVersion(0, 0, 0));

        MutableEntry entry = mock(MutableEntry.class);

        assertTrue((Boolean)repairProcessor.process(entry));

        verify(entry, times(1)).remove();
    }

    /**
     *
     */
    private class RepairEntryProcessorStub extends RepairEntryProcessor {
        /**
         *
         */
        private GridCacheContext context = cctx;

        /**
         *
         */
        private boolean topologyChanged = false;

        /**
         *
         */
        private GridCacheVersion keyVersion;

        /**
         *
         */
        private long updateCounter;

        /**
         * @param val Value.
         * @param data Data.
         * @param rmvQueueMaxSize Remove queue max size.
         * @param forceRepair Force repair.
         * @param startTopVer Start topology version.
         */
        public RepairEntryProcessorStub(
            Object val,
            Map<UUID, VersionedValue> data,
            long rmvQueueMaxSize,
            boolean forceRepair,
            AffinityTopologyVersion startTopVer
        ) {
            super(val, data, rmvQueueMaxSize, forceRepair, startTopVer);
        }

        /**
         *
         */
        @Override protected GridCacheContext cacheContext(MutableEntry entry) {
            return context;
        }

        /**
         *
         */
        @Override protected boolean topologyChanged(GridCacheContext cctx, AffinityTopologyVersion expTop) {
            return topologyChanged;
        }

        /**
         *
         */
        @Override protected GridCacheVersion keyVersion(MutableEntry entry) {
            return keyVersion;
        }

        /**
         *
         */
        @Override protected long updateCounter(GridCacheContext cctx, Object affKey) {
            return updateCounter;
        }

        /**
         *
         */
        public RepairEntryProcessorStub setContext(GridCacheContext ctx) {
            this.context = ctx;

            return this;
        }

        /**
         *
         */
        public RepairEntryProcessorStub setTopologyChanged(boolean topChanged) {
            this.topologyChanged = topChanged;

            return this;
        }

        /**
         *
         */
        public RepairEntryProcessorStub setKeyVersion(GridCacheVersion keyVer) {
            this.keyVersion = keyVer;

            return this;
        }

        /**
         *
         */
        public RepairEntryProcessorStub setUpdateCounter(long updateCntr) {
            this.updateCounter = updateCntr;

            return this;
        }
    }
}