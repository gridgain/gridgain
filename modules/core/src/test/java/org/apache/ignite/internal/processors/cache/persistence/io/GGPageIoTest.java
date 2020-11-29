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

package org.apache.ignite.internal.processors.cache.persistence.io;

import java.nio.ByteBuffer;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PagePartitionMetaIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PagePartitionMetaIOGG;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PagePartitionMetaIOV1GG;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PagePartitionMetaIOV2;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PagePartitionMetaIOV3;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.internal.util.typedef.internal.SB;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.junit.Test;

import static org.apache.ignite.internal.processors.cache.persistence.tree.io.PagePartitionMetaIO.GG_VERSION_OFFSET;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Test updating various page IO to GridGain page IO.
 */
public class GGPageIoTest {
    /** Page size. */
    public static final int PAGE_SIZE = 1024;

    /** */
    @Test
    public void testUpgradeToGGV1FromAI() {
        testUpgrade(1, GG_VERSION_OFFSET);
        testUpgrade(2, GG_VERSION_OFFSET);
        testUpgrade(3, GG_VERSION_OFFSET);
    }

    /** */
    @Test
    public void testUpgradeToGGV2FromAI() {
        testUpgrade(1, GG_VERSION_OFFSET + 1);
        testUpgrade(2, GG_VERSION_OFFSET + 1);
        testUpgrade(3, GG_VERSION_OFFSET + 1);
        testUpgrade(4, GG_VERSION_OFFSET + 1);
    }

    /** */
    @Test
    public void testUpgradeToGGV1ToGGV2() {
        testUpgrade(GG_VERSION_OFFSET, GG_VERSION_OFFSET + 1);
    }

    /**
     * Tests upgrade to gg version.
     *
     * @param from From version.
     * @param to To version.
     */
    public void testUpgrade(int from, int to) {
        PagePartitionMetaIO fromIO = PagePartitionMetaIO.VERSIONS.forVersion(from);
        assertEquals(from, fromIO.getVersion());

        PagePartitionMetaIO toIO = PagePartitionMetaIO.VERSIONS.forVersion(to);
        assertEquals(to, toIO.getVersion());

        ByteBuffer bb = GridUnsafe.allocateBuffer(PAGE_SIZE);
        long pageId = Long.MAX_VALUE;

        try {
            long addr = GridUnsafe.bufferAddress(bb);

            fromIO.initNewPage(addr, pageId, PAGE_SIZE);

            setFields(addr);

            System.out.println("The page before upgrade:");
            System.out.println(PageIO.printPage(addr, PAGE_SIZE));

            ((PagePartitionMetaIOGG)toIO).upgradePage(addr);

            System.out.println("The page after upgrade:");
            System.out.println(PageIO.printPage(addr, PAGE_SIZE));

            validate("Failed upgrading from " + from + " to " + to, addr, from);
        }
        finally {
            GridUnsafe.freeBuffer(bb);
        }
    }


    /**
     * Assigns all available fields for the page.
     *
     * @param addr Page addr.
     */
    private void setFields(long addr) {
        PagePartitionMetaIO io = PagePartitionMetaIO.VERSIONS.forPage(addr);

        io.setSize(addr, 1);
        io.setUpdateCounter(addr, 2);
        io.setGlobalRemoveId(addr, 3);
        io.setPartitionState(addr, (byte) 4);
        io.setSizesPageId(addr, 5);

        if (io.getVersion() >= 2) {
            io.setPendingTreeRoot(addr, 6);
            io.setPartitionMetaStoreReuseListRoot(addr, 7);
            io.setGapsLink(addr, 8);
        }

        if (io.getVersion() >= 3) {
            io.setEncryptedPageIndex(addr, 9);
            io.setEncryptedPageCount(addr, 10);
        }

        if (io.getVersion() >= 4 && io.getVersion() < GG_VERSION_OFFSET)
            io.setTombstonesCount(addr, 11);

        if (io.getVersion() >= GG_VERSION_OFFSET)
            io.setUpdateTreeRoot(addr, 1001);
    }

    /**
     * @param msg Message.
     * @param addr Address.
     * @param from From.
     */
    private void validate(String msg, long addr, int from) {
//        PagePartitionMetaIO io = PagePartitionMetaIO.VERSIONS.forPage(addr);
//
//        assertTrue(io instanceof PagePartitionMetaIOGG);
//
//        assertEquals(msg, 1, io.getSize(addr));
//        assertEquals(msg, 2, io.getUpdateCounter(addr));
//        assertEquals(msg, 3, io.getGlobalRemoveId(addr));
//        assertEquals(msg, 4, io.getPartitionState(addr));
//        assertEquals(msg, 5, io.getCacheSizesPageId(addr));
//
//        if (from >= 2) {
//            assertEquals(msg, 6, io.getPendingTreeRoot(addr));
//            assertEquals(msg, 7, io.getPartitionMetaStoreReuseListRoot(addr));
//            assertEquals(msg, 8, io.getGapsLink(addr));
//        }
//        else {
//            assertEquals(msg, 0, io.getPendingTreeRoot(addr));
//            assertEquals(msg, 0, io.getPartitionMetaStoreReuseListRoot(addr));
//            assertEquals(msg, 0, io.getGapsLink(addr));
//        }
//
//        if (from >= 3) {
//            assertEquals(msg, 9, io.getEncryptedPageIndex(addr));
//            assertEquals(msg, 10, io.getEncryptedPageCount(addr));
//        }
//        else {
//            assertEquals(msg, 0, io.getEncryptedPageIndex(addr));
//            assertEquals(msg, 0, io.getEncryptedPageCount(addr));
//        }
//
//        if (from >= 4)
//            assertEquals(msg, 12, io.getTombstonesCount(addr));
//        else
//            assertEquals(msg, 0, io.getTombstonesCount(addr));
//
//        assertEquals(msg, 11, io.getUpdateTreeRoot(addr));
//        assertEquals(msg, 12, io.getTombstonesCount(addr));
    }

    /**
     * Checks all values for current page.
     *
     * @param msg Message.
     * @param addr Page address.
     */
    private void checkFields(String msg, long addr) {
        PagePartitionMetaIO io = PagePartitionMetaIO.VERSIONS.forPage(addr);

        checkFields(msg, addr, io);

        assertEquals(msg, 1, io.getSize(addr));
        assertEquals(msg, 2, io.getUpdateCounter(addr));
        assertEquals(msg, 3, io.getGlobalRemoveId(addr));
        assertEquals(msg, 4, io.getPartitionState(addr));
        assertEquals(msg, 5, io.getCacheSizesPageId(addr));
        assertEquals(msg, 6, io.getPendingTreeRoot(addr));
        assertEquals(msg, 7, io.getPartitionMetaStoreReuseListRoot(addr));
        assertEquals(msg, 8, io.getGapsLink(addr));
        assertEquals(msg, 9, io.getEncryptedPageIndex(addr));
        assertEquals(msg, 10, io.getEncryptedPageCount(addr));
        assertEquals(msg, 11, io.getUpdateTreeRoot(addr));
        assertEquals(msg, 12, io.getTombstonesCount(addr));
    }

    /**
     * Checks all values with the specific page IO.
     *
     * @param msg Message.
     * @param addr Page address.
     * @param io Page IO to check.
     */
    private void checkFields(String msg, long addr, PagePartitionMetaIO io) {
        switch (io.getVersion()) {
            case GG_VERSION_OFFSET:
            case 3: {
                PagePartitionMetaIOV3 io3 = (PagePartitionMetaIOV3)io;
                assertEquals(msg, 9, io3.getEncryptedPageIndex(addr));
                assertEquals(msg, 10, io3.getEncryptedPageCount(addr));
            }
            case 65535:
            case 2: {
                PagePartitionMetaIOV2 io2 = (PagePartitionMetaIOV2)io;
                assertEquals(msg, 6, io2.getPendingTreeRoot(addr));
                assertEquals(msg, 7, io2.getPartitionMetaStoreReuseListRoot(addr));
                assertEquals(msg, 8, io2.getGapsLink(addr));
            }
            case 1: {
                assertEquals(msg, 1, io.getSize(addr));
                assertEquals(msg, 2, io.getUpdateCounter(addr));
                assertEquals(msg, 3, io.getGlobalRemoveId(addr));
                assertEquals(msg, 4, io.getPartitionState(addr));
                assertEquals(msg, 5, io.getCacheSizesPageId(addr));
            }
        }

        switch (io.getVersion()) {
            case 65534:
            case 65535: {
                PagePartitionMetaIO ggIo = PagePartitionMetaIO.VERSIONS.forPage(addr);

                assertTrue(ggIo instanceof PagePartitionMetaIOGG);

                assertEquals(msg, 11, ggIo.getUpdateTreeRoot(addr));
            }
        }

        PagePartitionMetaIO toIo = PagePartitionMetaIO.VERSIONS.forPage(addr);

        //Check zero fields which was appear after update.
        if (io != toIo) {
            assertTrue(toIo instanceof PagePartitionMetaIOGG);

            if (io.getVersion() < 2) {
                PagePartitionMetaIOV2 io2 = (PagePartitionMetaIOV2)toIo;
                assertEquals(msg, 0, io2.getPendingTreeRoot(addr));
                assertEquals(msg, 0, io2.getPartitionMetaStoreReuseListRoot(addr));
                assertEquals(msg, 0, io2.getGapsLink(addr));
            }

            if (io.getVersion() != 3 && toIo.getVersion() < 65535) {
                PagePartitionMetaIOV3 io3 = (PagePartitionMetaIOV3)toIo;

                assertEquals(msg, 0, io3.getEncryptedPageIndex(addr));
                assertEquals(msg, 0, io3.getEncryptedPageCount(addr));
            }

            if (io.getVersion() < 65534)
                assertEquals(msg, 0, toIo.getUpdateTreeRoot(addr));
        }
    }
}
