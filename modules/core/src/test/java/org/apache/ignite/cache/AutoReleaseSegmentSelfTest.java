/*
 * Copyright 2021 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.cache;

import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.junit.Test;

import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.internal.processors.cache.persistence.wal.FileWriteAheadLogManager.totalSize;

/**
 * Class for testing automatic release of segments.
 */
public class AutoReleaseSegmentSelfTest extends AbstractReleaseSegmentTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.getDataStorageConfiguration()
            .setWalSegmentSize((int)(2 * U.MB))
            .setMaxWalArchiveSize(10 * U.MB);

        return cfg;
    }

    /**
     * Checking that if at the time of start the node, the {@link DataStorageConfiguration#getMaxWalArchiveSize()}
     * is exceeded, then there will be no automatic release of segments due to which there will be an error in
     * {@code GridCacheDatabaseSharedManager#applyLogicalUpdates}.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testStartNodeWithExceedMaxWalArchiveSize() throws Exception {
        IgniteEx n = startGrid(0);

        n.cluster().state(ACTIVE);

        forceCheckpoint();
        enableCheckpoints(n, false);

        int i = 0;

        while (totalSize(walMgr(n).walArchiveFiles()) < 20 * U.MB)
            n.cache(DEFAULT_CACHE_NAME).put(i++, new byte[(int)(64 * U.KB)]);

        stopGrid(0);
        startGrid(0);
    }
}
