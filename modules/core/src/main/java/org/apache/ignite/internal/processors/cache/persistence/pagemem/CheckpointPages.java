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

package org.apache.ignite.internal.processors.cache.persistence.pagemem;

import java.nio.ByteBuffer;
import java.util.Collection;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.processors.cache.persistence.CheckpointState;
import org.apache.ignite.internal.processors.cache.persistence.PageStoreWriter;
import org.apache.ignite.internal.processors.cache.persistence.checkpoint.CheckpointProgress;
import org.apache.ignite.internal.processors.cache.persistence.checkpoint.CheckpointProgressImpl;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.cache.persistence.CheckpointState.MARKER_STORED_TO_DISK;

/**
 * Class contains dirty pages of the segment that will need to be written at a checkpoint or page replacement. It also
 * contains helper methods before writing pages.
 *
 * <p>For correct parallel operation of the checkpoint writer and page replacement, external synchronization must be
 * used.</p>
 *
 * @see PageMemoryImpl#checkpointWritePage(FullPageId, ByteBuffer, PageStoreWriter, CheckpointMetricsTracker)
 * @see PageMemoryImpl.Segment#tryToRemovePage(FullPageId, long)
 */
class CheckpointPages {
    /** */
    private final Collection<FullPageId> pageIds;

    /** */
    private final CheckpointProgressImpl checkpointProgress;

    /**
     * @param pageIds Dirty page IDs in the segment that should be written at a checkpoint or page replacement.
     * @param checkpointProgress Progress of the current checkpoint at which the object was created, {@code null} on
     *      binary recovery, it is expected that there will be no parallel writes on page replacement.
     */
    CheckpointPages(Collection<FullPageId> pageIds, @Nullable CheckpointProgress checkpointProgress) {
        this.pageIds = pageIds;
        this.checkpointProgress = (CheckpointProgressImpl)checkpointProgress;
    }

    /**
     * Removes a page ID that would be written at page replacement. Must be invoked before writing a page to disk.
     *
     * <p>Page will be removed only if the {@link CheckpointState#MARKER_STORED_TO_DISK} phase at the checkpoint has
     * completed or will synchronously wait for it to complete.</p>
     *
     * <p>To keep the data consistent, we need to use {@link #blockFsyncOnPageReplacement} and
     * {@link #unblockFsyncOnPageReplacement} after calling the current method to prevent the fsync checkpoint phase
     * from starting.</p>
     *
     * @param pageId Page ID to remove.
     * @return {@code True} if the page was removed by the current method invoke, {@code false} if the page was already
     *      removed by another removes or did not exist.
     * @throws IgniteCheckedException If any error occurred while waiting for the
     *      {@link CheckpointState#MARKER_STORED_TO_DISK} phase to complete at a checkpoint.
     * @see #removeOnCheckpoint(FullPageId)
     * @see #blockFsyncOnPageReplacement(FullPageId)
     * @see #unblockFsyncOnPageReplacement(FullPageId, Throwable)
     */
    public boolean removeOnPageReplacement(FullPageId pageId) throws IgniteCheckedException {
        assert checkpointProgress != null : "No page replacement expected on binary recovery";

        // Uninterruptibly is important because otherwise in case of interrupt of client thread node would be stopped.
        checkpointProgress.futureFor(MARKER_STORED_TO_DISK).getUninterruptibly();

        return pageIds.remove(pageId);
    }

    /**
     * Removes a page ID that would be written at checkpoint. Must be invoked before writing a page to disk.
     *
     * <p>We don't need to block the fsync phase of the checkpoint, since it won't start until all dirty pages have
     * been written at the checkpoint, except those for which page replacement has occurred.</p>
     *
     * @param pageId Page ID to remove.
     * @return {@code True} if the page was removed by the current method invoke, {@code false} if the page was already
     *      removed by another removes or did not exist.
     * @see #removeOnPageReplacement(FullPageId)
     */
    public boolean removeOnCheckpoint(FullPageId pageId) {
        return pageIds.remove(pageId);
    }

    /**
     * Removes a page ID on refresh outdated page.
     *
     * <p>We don't need to block the fsync phase of the checkpoint, since it won't start until all dirty pages have
     * been written at the checkpoint, except those for which page replacement has occurred.</p>
     *
     * @param pageId Page ID to remove.
     */
    public void removeOnRefreshOutdatedPage(FullPageId pageId) {
        pageIds.remove(pageId);
    }

    /**
     * Returns {@code true} if the page has not yet been written by a checkpoint or page replacement.
     *
     * @param pageId Page ID for checking.
     */
    public boolean contains(FullPageId pageId) {
        return pageIds.contains(pageId);
    }

    /** Returns the current size of all pages that will be written at a checkpoint or page replacement. */
    public int size() {
        return pageIds.size();
    }

    /**
     * Block the start of the fsync phase at a checkpoint before replacing the page.
     *
     * <p>It is expected that the method will be invoked once and after that the {@link #unblockFsyncOnPageReplacement}
     * will be invoked on the same page.</p>
     *
     * @param pageId Page ID for which page replacement will begin.
     * @see #unblockFsyncOnPageReplacement(FullPageId, Throwable)
     * @see #removeOnPageReplacement(FullPageId)
     */
    public void blockFsyncOnPageReplacement(FullPageId pageId) {
        assert checkpointProgress != null : "No page replacement expected on binary recovery";

        checkpointProgress.blockFsyncOnPageReplacement(pageId);
    }

    /**
     * Unblocks the start of the fsync phase at a checkpoint after the page replacement is completed.
     *
     * <p>It is expected that the method will be invoked once and after the {@link #blockFsyncOnPageReplacement} for
     * same page ID.</p>
     *
     * <p>The fsync phase will only be started after page replacement has been completed for all pages for which
     * {@link #blockFsyncOnPageReplacement} was invoked or no page replacement occurred at all.</p>
     *
     * <p>The method must be invoked even if any error occurred, so as not to hang a checkpoint.</p>
     *
     * @param pageId Page ID for which the page replacement has ended.
     * @param error Error on page replacement, {@code null} if missing.
     * @see #blockFsyncOnPageReplacement(FullPageId)
     * @see #removeOnPageReplacement(FullPageId)
     */
    public void unblockFsyncOnPageReplacement(FullPageId pageId, @Nullable Throwable error) {
        assert checkpointProgress != null : "No page replacement expected on binary recovery";

        checkpointProgress.unblockFsyncOnPageReplacement(pageId, error);
    }
}
