/*
 * Copyright 2026 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.internal.processors.cache.persistence.pagemem;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.pagemem.wal.WALIterator;
import org.apache.ignite.internal.pagemem.wal.WALPointer;
import org.apache.ignite.internal.pagemem.wal.record.CheckpointRecord;
import org.apache.ignite.internal.pagemem.wal.record.PageSnapshot;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.PageDeltaRecord;
import org.apache.ignite.internal.processors.cache.persistence.StorageException;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertArrayEquals;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link WalPageRecovery}.
 */
public class WalPageRecoveryTest extends GridCommonAbstractTest {
    /** */
    private static final int PAGE_SIZE = 4096;

    /** */
    @Test
    public void testRecoverSinglePageSnapshot() throws Exception {
        FullPageId fullId = new FullPageId(0x0001000000000001L, 42);

        byte[] expected = new byte[PAGE_SIZE];
        Arrays.fill(expected, (byte)0xAB);

        PageSnapshot snap = mock(PageSnapshot.class);
        when(snap.type()).thenReturn(WALRecord.RecordType.PAGE_RECORD);
        when(snap.fullPageId()).thenReturn(fullId);
        when(snap.pageData()).thenReturn(expected);
        when(snap.pageDataSize()).thenReturn(PAGE_SIZE);

        IgniteWriteAheadLogManager walMgr = stubWalMgrWithRecords(snap);

        WalPageRecovery rec = new WalPageRecovery(walMgr, null, log, PAGE_SIZE, grpId -> null);

        ByteBuffer dst = ByteBuffer.allocateDirect(PAGE_SIZE);
        rec.recoverPage(fullId, dst);

        byte[] got = new byte[PAGE_SIZE];
        dst.rewind();
        dst.get(got);
        assertArrayEquals(expected, got);
    }

    /** */
    @Test(expected = StorageException.class)
    public void testThrowsStorageExceptionWhenPageNotInWal() throws Exception {
        FullPageId fullId = new FullPageId(0x0001000000000001L, 42);

        IgniteWriteAheadLogManager walMgr = stubWalMgrWithRecords();

        WalPageRecovery rec = new WalPageRecovery(walMgr, null, log, PAGE_SIZE, grpId -> null);

        rec.recoverPage(fullId, ByteBuffer.allocateDirect(PAGE_SIZE));
    }

    /** */
    @Test
    public void testAppliesDeltasAfterPageSnapshot() throws Exception {
        FullPageId fullId = new FullPageId(0x0001000000000001L, 42);

        byte[] base = new byte[PAGE_SIZE];
        Arrays.fill(base, (byte)0x11);

        PageSnapshot snap = mock(PageSnapshot.class);
        when(snap.type()).thenReturn(WALRecord.RecordType.PAGE_RECORD);
        when(snap.fullPageId()).thenReturn(fullId);
        when(snap.pageData()).thenReturn(base);
        when(snap.pageDataSize()).thenReturn(PAGE_SIZE);

        PageDeltaRecord delta = mock(PageDeltaRecord.class, Mockito.CALLS_REAL_METHODS);
        when(delta.type()).thenReturn(WALRecord.RecordType.DATA_PAGE_INSERT_RECORD);
        when(delta.pageId()).thenReturn(fullId.pageId());
        when(delta.groupId()).thenReturn(fullId.groupId());
        Mockito.doAnswer(inv -> {
            long addr = inv.getArgument(1);
            GridUnsafe.putByte(addr, (byte)0x22); // prove delta ran
            return null;
        }).when(delta).applyDelta(any(), Mockito.anyLong());

        IgniteWriteAheadLogManager walMgr = stubWalMgrWithRecords(snap, delta);

        PageMemoryEx pageMem = mock(PageMemoryEx.class);
        WalPageRecovery rec = new WalPageRecovery(walMgr, null, log, PAGE_SIZE, grpId -> pageMem);

        ByteBuffer dst = ByteBuffer.allocateDirect(PAGE_SIZE);
        rec.recoverPage(fullId, dst);

        dst.rewind();
        assertEquals((byte)0x22, dst.get(0)); // first byte was overwritten by delta
    }

    /** */
    @Test
    public void testCheckpointRecordPromotesCurrentToLastValid() throws Exception {
        FullPageId fullId = new FullPageId(0x0001000000000001L, 42);

        byte[] valid = new byte[PAGE_SIZE];
        Arrays.fill(valid, (byte)0x55);

        PageSnapshot snap = mock(PageSnapshot.class);
        when(snap.type()).thenReturn(WALRecord.RecordType.PAGE_RECORD);
        when(snap.fullPageId()).thenReturn(fullId);
        when(snap.pageData()).thenReturn(valid);
        when(snap.pageDataSize()).thenReturn(PAGE_SIZE);

        CheckpointRecord cpRec = mock(CheckpointRecord.class);
        when(cpRec.type()).thenReturn(WALRecord.RecordType.CHECKPOINT_RECORD);
        when(cpRec.end()).thenReturn(false);

        // After checkpoint, simulate a MEMORY_RECOVERY that would wipe curPage —
        // lastValidPage must still be kept so the final restore succeeds.
        WALRecord memRec = mock(WALRecord.class);
        when(memRec.type()).thenReturn(WALRecord.RecordType.MEMORY_RECOVERY);

        IgniteWriteAheadLogManager walMgr = stubWalMgrWithRecords(snap, cpRec, memRec);
        WalPageRecovery rec = new WalPageRecovery(walMgr, null, log, PAGE_SIZE, grpId -> null);

        ByteBuffer dst = ByteBuffer.allocateDirect(PAGE_SIZE);
        rec.recoverPage(fullId, dst);

        byte[] got = new byte[PAGE_SIZE];
        dst.rewind();
        dst.get(got);
        assertArrayEquals(valid, got);
    }

    /** */
    @Test(expected = StorageException.class)
    public void testMemoryRecoveryDiscardsInProgressPage() throws Exception {
        FullPageId fullId = new FullPageId(0x0001000000000001L, 42);

        byte[] discardable = new byte[PAGE_SIZE];
        Arrays.fill(discardable, (byte)0x77);

        PageSnapshot snap = mock(PageSnapshot.class);
        when(snap.type()).thenReturn(WALRecord.RecordType.PAGE_RECORD);
        when(snap.fullPageId()).thenReturn(fullId);
        when(snap.pageData()).thenReturn(discardable);
        when(snap.pageDataSize()).thenReturn(PAGE_SIZE);

        WALRecord memRec = mock(WALRecord.class);
        when(memRec.type()).thenReturn(WALRecord.RecordType.MEMORY_RECOVERY);

        // No checkpoint between snap and memRec, no further records — expect StorageException.
        IgniteWriteAheadLogManager walMgr = stubWalMgrWithRecords(snap, memRec);
        WalPageRecovery rec = new WalPageRecovery(walMgr, null, log, PAGE_SIZE, grpId -> null);

        rec.recoverPage(fullId, ByteBuffer.allocateDirect(PAGE_SIZE));
    }

    /** */
    @Test
    public void testSummarizeCountsRelevantRecords() throws Exception {
        FullPageId fullId = new FullPageId(0x0001000000000001L, 42);

        PageSnapshot snap = mock(PageSnapshot.class);
        when(snap.type()).thenReturn(WALRecord.RecordType.PAGE_RECORD);
        when(snap.fullPageId()).thenReturn(fullId);

        PageDeltaRecord delta = mock(PageDeltaRecord.class);
        when(delta.type()).thenReturn(WALRecord.RecordType.DATA_PAGE_INSERT_RECORD);
        when(delta.pageId()).thenReturn(fullId.pageId());
        when(delta.groupId()).thenReturn(fullId.groupId());

        CheckpointRecord cpRec = mock(CheckpointRecord.class);
        when(cpRec.type()).thenReturn(WALRecord.RecordType.CHECKPOINT_RECORD);
        when(cpRec.end()).thenReturn(false);

        IgniteWriteAheadLogManager walMgr = stubWalMgrWithRecords(snap, delta, cpRec);

        WalPageRecovery rec = new WalPageRecovery(walMgr, null, log, PAGE_SIZE, grpId -> null);
        WalPageRecovery.WalRecordSummary sum = rec.summarize(fullId);

        assertEquals(1, sum.pageRecordCount);
        assertEquals(1, sum.pageDeltaRecordCount);
        assertEquals(1, sum.checkpointBoundariesSeen);
        assertFalse(sum.memoryRecoverySeen);
    }

    /** */
    private IgniteWriteAheadLogManager stubWalMgrWithRecords(WALRecord... records) throws Exception {
        IgniteWriteAheadLogManager wal = mock(IgniteWriteAheadLogManager.class);
        WALIterator it = mock(WALIterator.class);

        List<IgniteBiTuple<WALPointer, WALRecord>> tuples = new java.util.ArrayList<>();
        for (WALRecord r : records)
            tuples.add(new IgniteBiTuple<>(mock(WALPointer.class), r));

        Iterator<IgniteBiTuple<WALPointer, WALRecord>> inner = tuples.iterator();
        when(it.hasNext()).thenAnswer(inv -> inner.hasNext());
        when(it.next()).thenAnswer(inv -> inner.next());
        when(it.iterator()).thenReturn(inner);

        when(wal.replay(any(), any())).thenReturn(it);

        return wal;
    }
}
