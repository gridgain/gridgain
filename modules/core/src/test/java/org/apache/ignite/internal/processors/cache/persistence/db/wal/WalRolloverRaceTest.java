/*
 * Copyright 2020 GridGain Systems, Inc. and Contributors.
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

/*
 * Copyright 2020 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.internal.processors.cache.persistence.db.wal;

import java.util.Collections;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.failure.StopNodeFailureHandler;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.pagemem.wal.record.DataRecord;
import org.apache.ignite.internal.processors.cache.persistence.IgniteCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileWriteAheadLogManager;
import org.apache.ignite.internal.processors.cache.persistence.wal.filehandle.FileWriteHandleImpl;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_WAL_MMAP;

public class WalRolloverRaceTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String name) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(name);

        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                .setPersistenceEnabled(true)
                .setMaxSize(40 * 1024 * 1024))
            .setWalMode(WALMode.LOG_ONLY)
            .setWalSegmentSize(512 * 1024)
        );

        cfg.setFailureHandler(new StopNodeFailureHandler());

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    @Test
    @WithSystemProperty(key = IGNITE_WAL_MMAP, value = "false")
    public void test() throws Exception {
        IgniteEx ig = startGrid(0);

        ig.cluster().active(true);

        awaitPartitionMapExchange();

        FileWriteAheadLogManager walMgr = (FileWriteAheadLogManager) ig.context().cache().context().wal();

        IgniteCacheDatabaseSharedManager dbMgr = ig.context().cache().context().database();

        FileWriteHandleImpl hnd = GridTestUtils.getFieldValue(walMgr, "currHnd");

        int written = ((Long) GridTestUtils.getFieldValue(hnd, "written")).intValue();
        int maxWalSegmentSize = ((Long) GridTestUtils.getFieldValue(hnd, "maxWalSegmentSize")).intValue();

        int target = maxWalSegmentSize - 2;
        int delta = target - written;

        while (delta != 0) {
            int toWrite = Math.min(delta, 100_000);
            DataRecord rec = new DataRecord(Collections.emptyList());
            rec.size(toWrite);
            hnd.addRecord(rec);
            delta -= toWrite;
        }

        hnd.flushOrWait(null);

        Thread t1 = new Thread(() -> {
            DataRecord rec = new DataRecord(Collections.emptyList());
            rec.size(10);
            try {
                walMgr.log(rec);
            }
            catch (IgniteCheckedException e) {
                e.printStackTrace();
            }
        });

        Thread t2 = new Thread(() -> {
            DataRecord rec = new DataRecord(Collections.emptyList());
            rec.size(1);
            try {
                hnd.addRecord(rec);
            }
            catch (IgniteCheckedException e) {
                e.printStackTrace();
            }
        });

        t2.start();
        t1.start();

        t2.join();
        t1.join();
    }
}
