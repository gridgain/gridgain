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

package org.apache.ignite.development.utils.indexreader;

import java.io.File;
import java.io.IOException;
import java.nio.file.StandardOpenOption;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.internal.pagemem.PageIdAllocator;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStore;
import org.apache.ignite.internal.processors.cache.persistence.file.RandomAccessFileIO;
import org.apache.ignite.internal.processors.metric.impl.LongAdderMetric;

/**
 * Extension {@link FilePageStore} for reading snaphot files without transformation.
 */
public class SnapshotFilePageStore extends FilePageStore {
    /**
     * Mapping pageIdx(array index) -> position(array value) in file.
     * Value {@code -1} means that page is corrupted.
     */
    private final long[] pageFilePositions;

    /**
     * Constructor.
     *
     * @param type Data type, can be {@link PageIdAllocator#FLAG_IDX} or {@link PageIdAllocator#FLAG_DATA}.
     * @param dsCfg Data storage configuration.
     * @param pageStoreFile Page store file.
     * @param pageFilePositions Mapping pageIdx(array index) -> position(array value) in file.
     * @param allocatedTracker Metrics updater.
     * @throws IOException If an I/O error occurs.
     */
    public SnapshotFilePageStore(
        byte type,
        DataStorageConfiguration dsCfg,
        File pageStoreFile,
        long[] pageFilePositions,
        LongAdderMetric allocatedTracker
    ) throws IOException {
        super(
            type,
            pageStoreFile::toPath,
            (file, modes) -> {
                throw new UnsupportedOperationException();
            },
            dsCfg,
            allocatedTracker
        );

        this.pageFilePositions = pageFilePositions;

        fileIO = new RandomAccessFileIO(pageStoreFile, StandardOpenOption.READ);
        allocated.set(pageStoreFile.length());

        inited = true;
    }

    /** {@inheritDoc} */
    @Override public synchronized void ensure() throws IgniteCheckedException {
        //no-op
    }

    /** {@inheritDoc} */
    @Override public long pageOffset(long pageId) {
        return pageFilePositions[PageIdUtils.pageIndex(pageId)];
    }
}
