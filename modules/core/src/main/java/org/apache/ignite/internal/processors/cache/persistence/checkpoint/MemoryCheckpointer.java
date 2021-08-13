package org.apache.ignite.internal.processors.cache.persistence.checkpoint;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState;
import org.apache.ignite.internal.processors.cache.persistence.DataRegion;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.PageMemoryEx;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PagePartitionCountersIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PagePartitionMetaIO;
import org.apache.ignite.internal.util.GridMultiCollectionWrapper;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.worker.GridWorker;
import org.apache.ignite.internal.worker.WorkersRegistry;
import org.apache.ignite.thread.IgniteThread;
import org.jetbrains.annotations.Nullable;

public class MemoryCheckpointer extends GridWorker {

    /**
     * Data regions from which checkpoint will collect pages.
     */
    private final Supplier<Collection<DataRegion>> dataRegions;

    MemoryCheckpointer(
        @Nullable String gridName,
        String name,
        WorkersRegistry workersRegistry,
        Supplier<Collection<DataRegion>> dataRegions,
        Function<Class<?>, IgniteLogger> logger
    ) {
        super(gridName, name, logger.apply(Checkpointer.class), workersRegistry);

        this.dataRegions = dataRegions;
    }

    /**
     * Restart worker in IgniteThread.
     */
    public void start() {
        if (runner() != null)
            return;

        assert runner() == null : "Memory checkpointer is running.";

        new IgniteThread(this).start();
    }

    @Override protected void body() throws InterruptedException, IgniteInterruptedCheckedException {

        while (true) {
            System.out.println("RUNNING");
            //getActiveMamory();
            Thread.sleep(5000);
        }
    }

    /*
    private void getActiveMamory(){

        Collection<DataRegion> checkpointedRegions = dataRegions.get();
        for (DataRegion reg : checkpointedRegions) {
            if (!reg.config().isPersistenceEnabled())
                continue;

            PageMemoryEx pmex = (PageMemoryEx)reg.pageMemory();

            pmex.


            GridMultiCollectionWrapper<FullPageId> nextCpPages = ((PageMemoryEx)reg.pageMemory())
                .beginCheckpoint(null);

            pagesNum += nextCpPages.size();

            res.add(new T2<>((PageMemoryEx)reg.pageMemory(),


    }


        private LocalSnapshotPartitionData readPartitionData(
            SnapshotInputStream input,
        int grpId,
        int partId,
        String consId,
        int pageSize
    ) throws IOException, IgniteCheckedException {
            final long PAGE_BUF_PTR = GridUnsafe.allocateMemory(pageSize);

            final ByteBuffer PAGE_BUF = GridUnsafe.wrapPointer(PAGE_BUF_PTR, pageSize);

            LocalSnapshotPartitionData res = null;

            long nextCntrPageId = 0;

            Map<Long, byte[]> savedCntrPages = new HashMap<>();

            while (input.readNextPage(PAGE_BUF)) {
                PAGE_BUF.flip();

                int type = PageIO.getType(PAGE_BUF_PTR);

                switch (type) {
                    case PageIO.T_PART_META:
                        PagePartitionMetaIO io = PageIO.getPageIO(PAGE_BUF_PTR);

                        byte partStateOrdinal = io.getPartitionState(PAGE_BUF_PTR);

                        res = new LocalSnapshotPartitionData(consId, grpId, partId, io.getUpdateCounter(PAGE_BUF_PTR),
                            io.getSize(PAGE_BUF_PTR), GridDhtPartitionState.fromOrdinal(partStateOrdinal), null,
                            false);

                        nextCntrPageId = io.getCacheSizesPageId(PAGE_BUF_PTR);

                        break;

                    case PageIO.T_PART_CNTRS:
                        byte[] heapCntrPage = new byte[pageSize];

                        GridUnsafe.copyOffheapHeap(PAGE_BUF_PTR, heapCntrPage, GridUnsafe.BYTE_ARR_OFF, pageSize);

                        savedCntrPages.put(PageIO.getPageId(PAGE_BUF_PTR), heapCntrPage);

                        break;

                    default:
                        // Skip page.
                }

                while (res != null && savedCntrPages.containsKey(nextCntrPageId)) {
                    byte[] heapCntrPage = savedCntrPages.remove(nextCntrPageId);

                    GridUnsafe.copyHeapOffheap(heapCntrPage, GridUnsafe.BYTE_ARR_OFF, PAGE_BUF_PTR, pageSize);

                    PagePartitionCountersIO io = PageIO.getPageIO(PAGE_BUF_PTR);

                    HashMap<Integer, Long> cacheIdToCacheSize = new HashMap<>();
                    io.readCacheSizes(PAGE_BUF_PTR, cacheIdToCacheSize);

                    if (res.cacheIdToCacheSize() == null)
                        res.cacheIdToCacheSize(cacheIdToCacheSize);
                    else
                        res.cacheIdToCacheSize().putAll(cacheIdToCacheSize);

                    nextCntrPageId = io.getNextCountersPageId(PAGE_BUF_PTR);
                }

                if (res != null && nextCntrPageId == 0)
                    break;
            }

            return res;
        }

     */
}
