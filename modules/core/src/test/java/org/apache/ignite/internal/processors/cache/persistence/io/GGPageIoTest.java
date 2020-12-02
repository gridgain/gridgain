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
import java.util.Arrays;
import java.util.List;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.IOVersions;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PagePartitionMetaIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PagePartitionMetaIOGG;
import org.apache.ignite.internal.util.GridStringBuilder;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.junit.Test;

import static org.apache.ignite.internal.processors.cache.persistence.tree.io.IOVersions.GG_VERSION_OFFSET;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests upgrading each compatible AI/GG page IO to GG page IO.
 */
public class GGPageIoTest {
    /** Page size. */
    private static final int PAGE_SIZE = 1024;

    /** */
    @Test
    public void testGGVersionsCovered() {
        List<Integer> knownVers = Arrays.asList(1, 2, 3, GG_VERSION_OFFSET);

        PagePartitionMetaIO[] vers = U.field(PagePartitionMetaIO.VERSIONS, "vers");

        for (int i = 0; i < vers.length; i++) {
            PagePartitionMetaIO ver = vers[i];

            assertTrue(String.valueOf(ver.getVersion()), knownVers.contains(ver.getVersion()));
        }
    }

    /** */
    @Test
    public void testCheckVersions1() {
        new IOVersions<>(
            new TestPageIO(1),
            new TestPageIO(2),
            new TestPageIO(GG_VERSION_OFFSET),
            new TestPageIO(GG_VERSION_OFFSET + 1));
    }

    /** */
    @Test
    public void testCheckVersions2() {
        new IOVersions<>(new TestPageIO(1), new TestPageIO(2), new TestPageIO(3));
    }

    /** */
    @Test
    public void testCheckVersions3() {
        new IOVersions<>(new TestPageIO(GG_VERSION_OFFSET), new TestPageIO(GG_VERSION_OFFSET + 1));
    }

    /** */
    @Test
    public void testUpgradeToGGV1FromAIV1() {
        testUpgrade(1, GG_VERSION_OFFSET, 1L, 2L, 3L, (byte)4, 5L, 0L, 0L, 0L, 0, 0, 0L);
    }

    /** */
    @Test
    public void testUpgradeToGGV1FromAIV2() {
        testUpgrade(2, GG_VERSION_OFFSET, 1L, 2L, 3L, (byte)4, 5L, 6L, 7L, 8L, 0, 0, 0L);
    }

    /** */
    @Test
    public void testUpgradeToGGV1FromAIV3() {
        testUpgrade(3, GG_VERSION_OFFSET, 1L, 2L, 3L, (byte)4, 5L, 6L, 7L, 8L, 9, 10, 0L);
    }

    /**
     * Tests upgrade to GG version.
     *
     * @param from From version.
     * @param to To version.
     */
    private void testUpgrade(int from, int to, Object... expVals) {
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

            validate("Failed upgrading from " + from + " to " + to, addr, expVals);
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
        io.setCountersPageId(addr, 5);

        if (io.getVersion() >= 2) {
            io.setPendingTreeRoot(addr, 6);
            io.setPartitionMetaStoreReuseListRoot(addr, 7);
            io.setGapsLink(addr, 8);
        }

        if (io.getVersion() >= 3) {
            io.setEncryptedPageIndex(addr, 9);
            io.setEncryptedPageCount(addr, 10);
        }
    }

    /**
     * @param msg Message.
     * @param addr Address.
     */
    private void validate(String msg, long addr, Object... expVals) {
        PagePartitionMetaIO io = PagePartitionMetaIO.VERSIONS.forPage(addr);

        assertTrue(io instanceof PagePartitionMetaIOGG);

        assertEquals(msg, expVals[0], io.getSize(addr));
        assertEquals(msg, expVals[1], io.getUpdateCounter(addr));
        assertEquals(msg, expVals[2], io.getGlobalRemoveId(addr));
        assertEquals(msg, expVals[3], io.getPartitionState(addr));
        assertEquals(msg, expVals[4], io.getCountersPageId(addr));
        assertEquals(msg, expVals[5], io.getPendingTreeRoot(addr));
        assertEquals(msg, expVals[6], io.getPartitionMetaStoreReuseListRoot(addr));
        assertEquals(msg, expVals[7], io.getGapsLink(addr));
        assertEquals(msg, expVals[8], io.getEncryptedPageIndex(addr));
        assertEquals(msg, expVals[9], io.getEncryptedPageCount(addr));
        assertEquals(msg, expVals[10], io.getUpdateTreeRoot(addr));
    }

    /** */
    private static class TestPageIO extends PageIO {
        /**
         * @param ver Version.
         */
        public TestPageIO(int ver) {
            super(1, ver);
        }

        /** {@inheritDoc} */
        @Override protected void printPage(long addr, int pageSize, GridStringBuilder sb) {
            // No-op.
        }
    }
}
