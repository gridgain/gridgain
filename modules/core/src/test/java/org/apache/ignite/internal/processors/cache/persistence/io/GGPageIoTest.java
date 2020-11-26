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
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PagePartitionMetaIOV2;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PagePartitionMetaIOV3;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Test updating various page IO to GridGain page IO.
 * If anyone will be adding a new page IO I think better to rewrite
 * all this test to explicitly check an IO correct convert to another or not.
 * I try to use DRY principle here but see this code looks difficulty to read, do not use it anymore.
 */
public class GGPageIoTest {

    /**
     * Test of IOs of partition meta page.
     */
    @Test
    public void metaPageIoTest() {
        int pageSize = 4 * 1024;
        long pageId = 3233232L;

        ByteBuffer bb = GridUnsafe.allocateBuffer(pageSize);

        long addr = GridUnsafe.bufferAddress(bb);

        System.err.println("Test!");

        PageIO[] vers = U.field(PagePartitionMetaIO.VERSIONS, "vers");

        assertEquals("Need to add a test of new IO version", 3, vers.length);

        PageIO[] ggVers = U.field(PagePartitionMetaIO.VERSIONS, "ggVers");

        assertEquals("Need to add a test of new GG IO version", 2, ggVers.length);

        PageIO[] allVers = new PageIO[5];

        System.arraycopy(vers, 0, allVers, 0, 3);
        System.arraycopy(ggVers, 0, allVers, 3, 2);

        for (PageIO pageFrom : allVers) {
            for (PageIO pageTo : ggVers) {
                assertTrue(pageFrom instanceof PagePartitionMetaIO);

                pageFrom.initNewPage(addr, pageId, pageSize);

                assignFields(addr);

                String before = pageFrom.printPage(addr, pageSize);

                checkFields("Checking was no passed:\n" + before, addr);

                if (canUpdate(pageFrom, pageTo)) {
                    assertTrue(pageTo instanceof PagePartitionMetaIOGG);

                    ((PagePartitionMetaIOGG)pageTo).upgradePage(addr);

                    String after = pageTo.printPage(addr, pageSize);

                    checkFields("Page was modified from:\n" + before + "to:\n" + after, addr, (PagePartitionMetaIO)pageFrom);
                }
            }
        }
    }

    /**
     * Assigns all available fields for the page.
     *
     * @param addr Page addr.
     */
    private void assignFields(long addr) {
        PagePartitionMetaIO io = PagePartitionMetaIO.VERSIONS.forPage(addr);

        switch (io.getVersion()) {
            case 65534:
            case 3: {
                PagePartitionMetaIOV3 io3 = (PagePartitionMetaIOV3)io;
                io3.setEncryptedPageIndex(addr, 9);
                io3.setEncryptedPageCount(addr, 10);
            }
            case 65535:
            case 2: {
                PagePartitionMetaIOV2 io2 = (PagePartitionMetaIOV2)io;
                io2.setPendingTreeRoot(addr, 6);
                io2.setPartitionMetaStoreReuseListRoot(addr, 7);
                io2.setGapsLink(addr, 8);
            }
            case 1: {
                io.setSize(addr, 1);
                io.setUpdateCounter(addr, 2);
                io.setGlobalRemoveId(addr, 3);
                io.setPartitionState(addr, (byte)4);
                io.setCountersPageId(addr, 5);
            }
        }

        switch (io.getVersion()) {
            case 65534:
            case 65535: {
                PagePartitionMetaIO ggIo = PagePartitionMetaIO.VERSIONS.forPage(addr);

                assertTrue(ggIo instanceof PagePartitionMetaIOGG);

                ggIo.setUpdateTreeRoot(addr, 11L);
            }
        }
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
            case 65534:
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
                assertEquals(msg, 5, io.getCountersPageId(addr));
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

    /**
     * Checks if IO can update or not.
     *
     * @param fromIo Update from IO version.
     * @param toIo Update to IO version.
     * @return True when the IO can update, false if its cannot.
     */
    private boolean canUpdate(PageIO fromIo, PageIO toIo) {
        assertTrue(toIo instanceof PagePartitionMetaIOGG);

        assertTrue(toIo.getVersion() > 4);

        if (fromIo.getVersion() < 4) {
            if (toIo.getVersion() == 65535)
                return fromIo.getVersion() < 3;

            if (toIo.getVersion() == 65534)
                return fromIo.getVersion() < 4;
        }

        if (fromIo.getVersion() > 4) {
            assertTrue(fromIo instanceof PagePartitionMetaIOGG);

            return fromIo.getVersion() > toIo.getVersion();
        }

        fail("Unexpected IO versions [fromIO=" + fromIo.getVersion() + " (" + fromIo.getClass().getSimpleName() +
            "), toIO=" + toIo.getVersion() + " (" + fromIo.getClass().getSimpleName() + ")]");

        return false;
    }
}
