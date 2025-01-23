/*
 * Copyright 2024 GridGain Systems, Inc. and Contributors.
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
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.processors.cache.persistence.PageStoreWriter;
import org.apache.ignite.internal.util.GridUnsafe;
import org.jetbrains.annotations.Nullable;

/**
 * Stateful class for page replacement of one page with write() delay. This allows to write page content without holding
 * segment write lock, which allows for a long time not to block the segment read lock due to IO writes when reading pages or writing dirty
 * pages at a checkpoint.
 *
 * <p>Usage:</p>
 * <ul>
 *     <li>On page replacement, invoke {@link #copyPageToTemporaryBuffer}.</li>
 *     <li>After releasing the segment write lock, invoke {@link #flushCopiedPageIfExists}.</li>
 * </ul>
 *
 * <p>Not thread safe.</p>
 */
public class DelayedDirtyPageStoreWrite {
    /** Real flush dirty page implementation. */
    private final PageStoreWriter flushDirtyPage;

    /** Page size. */
    private final int pageSize;

    /** Thread local with byte buffers. */
    private final ThreadLocal<ByteBuffer> byteBufThreadLoc;

    /** Replacing pages tracker, used to register & unregister pages being written. */
    private final DelayedPageReplacementTracker tracker;

    /** Full page id to be written on {@link #flushCopiedPageIfExists()} or null if nothing to write. */
    @Nullable private FullPageId fullPageId;

    /** Partition update tag to be used in{@link #flushCopiedPageIfExists()} or null if -1 to write. */
    private int tag = -1;

    /**
     * Dirty pages of the segment that need to be written at the current checkpoint or page replacement, {@code null}
     * if nothing to write.
     */
    private @Nullable CheckpointPages checkpointPages;

    /**
     * @param flushDirtyPage real writer to save page to store.
     * @param byteBufThreadLoc thread local buffers to use for pages copying.
     * @param pageSize page size.
     * @param tracker tracker to lock/unlock page reads.
     */
    public DelayedDirtyPageStoreWrite(
        PageStoreWriter flushDirtyPage,
        ThreadLocal<ByteBuffer> byteBufThreadLoc,
        int pageSize,
        DelayedPageReplacementTracker tracker
    ) {
        this.flushDirtyPage = flushDirtyPage;
        this.pageSize = pageSize;
        this.byteBufThreadLoc = byteBufThreadLoc;
        this.tracker = tracker;
    }

    /**
     * Copies a page to a temporary buffer on page replacement.
     *
     * @param fullPageId ID of the copied page.
     * @param originPageBuf Buffer with the full contents of the page being copied (from which we will copy).
     * @param tag Partition generation.
     * @param checkpointPages Dirty pages of the segment that need to be written at the current checkpoint or page
     *      replacement.
     * @see #flushCopiedPageIfExists()
     */
    public void copyPageToTemporaryBuffer(
        FullPageId fullPageId,
        ByteBuffer originPageBuf,
        int tag,
        CheckpointPages checkpointPages
    ) {
        tracker.lock(fullPageId);

        ByteBuffer tlb = byteBufThreadLoc.get();

        tlb.rewind();

        long writeAddr = GridUnsafe.bufferAddress(tlb);
        long origBufAddr = GridUnsafe.bufferAddress(originPageBuf);

        GridUnsafe.copyMemory(origBufAddr, writeAddr, pageSize);

        this.fullPageId = fullPageId;
        this.tag = tag;
        this.checkpointPages = checkpointPages;
    }

    /**
     * Flushes a previously copied page to disk if it was copied.
     *
     * @throws IgniteCheckedException If write failed.
     * @see #copyPageToTemporaryBuffer(FullPageId, ByteBuffer, int, CheckpointPages)
     */
    public void flushCopiedPageIfExists() throws IgniteCheckedException {
        if (fullPageId == null)
            return;

        assert checkpointPages != null : fullPageId;

        Throwable errorOnWrite = null;

        try {
            flushDirtyPage.writePage(fullPageId, byteBufThreadLoc.get(), tag);
        }
        catch (Throwable t) {
            errorOnWrite = t;

            throw t;
        }
        finally {
            checkpointPages.unblockFsyncOnPageReplacement(fullPageId, errorOnWrite);

            tracker.unlock(fullPageId);

            fullPageId = null;
            tag = -1;
        }
    }
}
