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

package org.apache.ignite.internal.processors.cache.persistence.wal;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.configuration.DataStorageConfiguration;

/**
 * Class for working with WAL archive size,
 * prevents exceeding {@link DataStorageConfiguration#getMaxWalArchiveSize() maxWalArchiveSize}.
 */
public class WalArchiveSize {
    /** Maximum WAL archive size in bytes. */
    private final long maxSize;

    /** Сurrent size of WAL archive in bytes. */
    private final AtomicLong currSize = new AtomicLong();

    /** Mapping: absolute segment index -> segment size in bytes. */
    // TODO: 30.10.2020 kirill: может нам не надо будет следить за мапой если мы безлимитны!
    private final Map<Long, Long> sizes = new ConcurrentHashMap<>();

    /**
     * Constructor.
     *
     * @param dsCfg Data storage configuration.
     */
    public WalArchiveSize(DataStorageConfiguration dsCfg) {
        maxSize = dsCfg.getMaxWalArchiveSize();
    }

    /**
     * Adding a segment size.
     *
     * @param idx Absolut segment index.
     * @param size Segment size in bytes.
     */
    public void add(long idx, long size) {
        sizes.merge(idx, size, Long::sum);
        currSize.addAndGet(size);
    }

    /**
     * Removing segment.
     *
     * @param idx Absolut segment index.
     */
    public void remove(long idx) {
        Long size = sizes.remove(idx);

        if (size != null)
            currSize.addAndGet(-size);
    }

    /**
     * Check that current archive size does not exceed maximum.
     *
     * @return {@code True} if current archive size is greater than maximum.
     */
    public boolean exceedMaxArchiveSize() {
        return maxSize != DataStorageConfiguration.UNLIMITED_WAL_ARCHIVE && currSize.get() > maxSize;
    }

    /**
     * Return current size of WAL archive in bytes.
     *
     * @return Сurrent size of WAL archive in bytes.
     */
    public long currentSize() {
        return currSize.get();
    }

    /**
     * Return maximum WAL archive size in bytes.
     *
     * @return Maximum WAL archive size in bytes.
     */
    public long maxSize() {
        return maxSize;
    }
}
