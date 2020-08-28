package org.apache.ignite.internal.processors.query.h2.opt.statistics;


import org.apache.ignite.internal.processors.cache.query.QueryTable;
import org.apache.ignite.internal.processors.metastorage.persistence.ReadWriteMetaStorageMock;
import org.junit.Test;

import java.util.Collection;
import java.util.Collections;

/**
 * Test sql statistics store operations.
 */
public class SqlStatisticsStoreSelfTest extends SqlStatisticsTest {


    @Test
    public void testStoreObjectPartitionStatistics() throws Exception {
        SqlStatisticsStoreImpl store = getStore();
        ReadWriteMetaStorageMock metastore = new ReadWriteMetaStorageMock();

        store.onReadyForRead(metastore);
        store.onReadyForReadWrite(metastore);

        store.clearAllStatistics();

        ObjectPartitionStatistics statistics1 = new ObjectPartitionStatistics(1, true, 2, 100,
                Collections.emptyMap());
        store.saveLocalPartitionStatistics(tbl(1), 1, statistics1);

        QueryTable tbl1 = tbl(1);
        store.clearLocalPartitionStatistics(tbl1, 2);

        ObjectPartitionStatistics statistics1r = store.getLocalPartitionStatistics(tbl1, 1);

        assertEquals(statistics1, statistics1r);

        Collection<ObjectPartitionStatistics> tbl1statistics = store.getLocalPartitionsStatistics(tbl1);

        assertEquals(1, tbl1statistics.size());
        assertEquals(statistics1, tbl1statistics.iterator().next());

        store.clearLocalPartitionsStatistics(tbl1);

        tbl1statistics = store.getLocalPartitionsStatistics(tbl1);

        assertTrue(tbl1statistics.isEmpty());
    }

    @Test
    public void testStoreObjectLocalStatistics() throws Exception {
        SqlStatisticsStoreImpl store = getStore();
        ReadWriteMetaStorageMock metastore = new ReadWriteMetaStorageMock();

        store.onReadyForRead(metastore);
        store.onReadyForReadWrite(metastore);

        store.clearAllStatistics();

        ObjectStatistics statistics1 = new ObjectStatistics(1,  Collections.emptyMap());
        store.saveLocalStatistics(tbl(1), statistics1);

        QueryTable tbl1 = tbl(1);
        ObjectStatistics statistics1r = store.getLocalStatistics(tbl1, false);

        assertEquals(statistics1, statistics1r);

        store.clearLocalStatistics(tbl1);

        statistics1r = store.getLocalStatistics(tbl1, false);

        assertNull(statistics1r);
    }

    @Test
    public void testStoreObjectGlobalStatistics() throws Exception {
        SqlStatisticsStoreImpl store = getStore();
        ReadWriteMetaStorageMock metastore = new ReadWriteMetaStorageMock();

        store.onReadyForRead(metastore);
        store.onReadyForReadWrite(metastore);

        store.clearAllStatistics();

        ObjectStatistics statistics1 = new ObjectStatistics(1,  Collections.emptyMap());
        store.saveGlobalStatistics(tbl(1), statistics1);

        QueryTable tbl1 = tbl(1);
        ObjectStatistics statistics1r = store.getGlobalStatistics(tbl1, false);

        assertEquals(statistics1, statistics1r);

        store.clearGlobalStatistics(tbl1);

        statistics1r = store.getGlobalStatistics(tbl1, false);

        assertNull(statistics1r);
    }

}
