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
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.internal.pagemem.PageIdAllocator;
import org.apache.ignite.internal.processors.cache.persistence.StorageException;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStore;
import org.apache.ignite.internal.processors.cache.persistence.file.RandomAccessFileIO;
import org.apache.ignite.internal.processors.metric.impl.LongAdderMetric;

import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;
import static java.util.stream.Collectors.joining;
import static org.apache.ignite.internal.pagemem.PageIdUtils.pageIndex;

/**
 * Extension {@link FilePageStore} for reading snaphot files without transformation.
 */
public class SnapshotFilePageStore extends FilePageStore {
    /**
     * Mapping pageIdx(list index) -> file and position(offset) in it.
     * Value {@code null} means that page is corrupted.
     */
    private final List<FilePosition> pageFilePositions;

    /**
     * Mapping file(from {@link #pageFilePositions}) -> I/O inteface.
     * Value {@code null} means that I/O interface either not initialized or stopped.
     */
    private final Map<File, FileIO> fileIOs;

    /**
     * Constructor.
     *
     * @param type Data type, can be {@link PageIdAllocator#FLAG_IDX} or {@link PageIdAllocator#FLAG_DATA}.
     * @param dsCfg Data storage configuration.
     * @param pageFilePositions Mapping pageIdx(list index) -> file and position(offset) in it.
     * @param allocatedTracker Metrics updater.
     */
    public SnapshotFilePageStore(
        byte type,
        DataStorageConfiguration dsCfg,
        List<FilePosition> pageFilePositions,
        LongAdderMetric allocatedTracker
    ) {
        super(
            type,
            () -> {
                throw new UnsupportedOperationException();
            },
            (file, modes) -> {
                throw new UnsupportedOperationException();
            },
            dsCfg,
            allocatedTracker
        );

        this.pageFilePositions = pageFilePositions;

        fileIOs = new HashMap<>();

        for (FilePosition filePosition : pageFilePositions) {
            if (nonNull(filePosition))
                fileIOs.put(filePosition.file(), null);
        }
    }

    /** {@inheritDoc} */
    @Override public synchronized void ensure() throws IgniteCheckedException {
        for (Map.Entry<File, FileIO> entry : fileIOs.entrySet()) {
            File file = entry.getKey();

            try {
                entry.setValue(new RandomAccessFileIO(file, StandardOpenOption.READ));
            }
            catch (IOException e) {
                throw new IgniteCheckedException("Error while create I/O file=" + file.getAbsolutePath(), e);
            }
        }

        allocated.set(size());

        inited = true;
    }

    /** {@inheritDoc} */
    @Override public long pageOffset(long pageId) {
        int pageIdx = pageIndex(pageId);

        FilePosition filePosition = pageIdx >= pageFilePositions.size() ? null : pageFilePositions.get(pageIdx);

        if (isNull(filePosition))
            throw new IllegalArgumentException("Unknown position for page [id=" + pageId + ", idx=" + pageIdx + "]");

        return filePosition.position();
    }

    /** {@inheritDoc} */
    @Override protected FileIO fileIO(long pageId) {
        return fileIOs.get(pageFilePositions.get(pageIndex(pageId)).file());
    }

    /** {@inheritDoc} */
    @Override public String getFileAbsolutePath() {
        return fileIOs.keySet().stream().map(File::getAbsolutePath).collect(joining(", "));
    }

    /** {@inheritDoc} */
    @Override public long size() {
        return pageSize * pageFilePositions.size();
    }

    /** {@inheritDoc} */
    @Override public int headerSize() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public void stop(boolean del) throws StorageException {
        try {
            for (Map.Entry<File, FileIO> entry : fileIOs.entrySet()) {
                File file = entry.getKey();
                FileIO fileIO = entry.getValue();

                try {
                    fileIO.force();

                    fileIO.close();

                    if (del)
                        Files.delete(file.toPath());

                    entry.setValue(null);
                }
                catch (IOException e) {
                    throw new StorageException(
                        "Error while stop [file=" + file.getAbsolutePath() + ", delete=" + del + "]",
                        e
                    );
                }
            }
        }
        finally {
            inited = false;

            allocated.set(0);
        }
    }
}
